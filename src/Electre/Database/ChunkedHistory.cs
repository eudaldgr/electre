using System.Buffers.Binary;
using Electre.Utils;
using RocksDbSharp;

namespace Electre.Database;

/// <summary>
///     Manages chunked storage of scripthash transaction history using TxNums.
///     Stores history in fixed-size chunks to avoid large value sizes in RocksDB.
/// </summary>
public sealed class ChunkedHistory
{
    private const int TargetChunkSize = 4096; // 4KB
    private const int TxNumsPerChunk = TargetChunkSize / 6; // ~682 TxNums per chunk
    private readonly ColumnFamilyHandle _cfData;
    private readonly ColumnFamilyHandle _cfHead;

    private readonly RocksDb _db;

    public ChunkedHistory(RocksDb db, ColumnFamilyHandle cfData, ColumnFamilyHandle cfHead)
    {
        _db = db;
        _cfData = cfData;
        _cfHead = cfHead;
    }

    /// <summary>
    ///     Appends a TxNum to scripthash history. Auto-creates new chunk when current fills.
    ///     NO deduplication - caller is responsible for uniqueness.
    /// </summary>
    public void AppendTxNum(WriteBatch batch, byte[] scripthash, ulong txNum)
    {
        AppendTxNumsBatch(batch, [(scripthash, txNum)]);
    }

    /// <summary>
    ///     Appends multiple TxNums to scripthash histories in batch.
    /// </summary>
    public void AppendTxNumsBatch(WriteBatch batch, IReadOnlyList<(byte[] scripthash, ulong txNum)> entries)
    {
        if (entries.Count == 0)
            return;

        var grouped = new Dictionary<byte[], List<ulong>>(ByteArrayComparer.Instance);
        foreach (var (scripthash, txNum) in entries)
        {
            if (scripthash.Length != 32)
                throw new ArgumentException("Scripthash must be 32 bytes", nameof(entries));

            if (!grouped.TryGetValue(scripthash, out var txNums))
            {
                txNums = [];
                grouped[scripthash] = txNums;
            }

            txNums.Add(txNum);
        }

        foreach (var (scripthash, txNums) in grouped)
            AppendTxNumsForScripthash(batch, scripthash, txNums);
    }

    private void AppendTxNumsForScripthash(WriteBatch batch, byte[] scripthash, List<ulong> txNums)
    {
        if (txNums.Count == 0)
            return;

        var headValue = _db.Get(scripthash, _cfHead);
        uint lastChunkId = 0;
        uint countInLastChunk = 0;

        if (headValue is { Length: 8 })
        {
            lastChunkId = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(0, 4));
            countInLastChunk = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(4, 4));
            if (countInLastChunk > TxNumsPerChunk)
                countInLastChunk = TxNumsPerChunk;
        }

        if (countInLastChunk >= TxNumsPerChunk)
        {
            lastChunkId++;
            countInLastChunk = 0;
        }

        var chunkKey = MakeChunkKey(scripthash, lastChunkId);
        var existingChunk = _db.Get(chunkKey, _cfData);
        var existingBytes = Math.Min((int)countInLastChunk * 6, existingChunk?.Length ?? 0);
        var chunkBytes = new List<byte>(Math.Max(existingBytes + txNums.Count * 6, TargetChunkSize));
        if (existingChunk is not null && existingBytes > 0)
            chunkBytes.AddRange(existingChunk.AsSpan(0, existingBytes).ToArray());

        foreach (var txNum in txNums)
        {
            if (countInLastChunk >= TxNumsPerChunk)
            {
                batch.Put(chunkKey, chunkBytes.ToArray(), _cfData);
                lastChunkId++;
                countInLastChunk = 0;
                chunkKey = MakeChunkKey(scripthash, lastChunkId);
                chunkBytes.Clear();
            }

            var txNumBytes = TxNumToBytes(txNum);
            chunkBytes.AddRange(txNumBytes);
            countInLastChunk++;
        }

        batch.Put(chunkKey, chunkBytes.ToArray(), _cfData);

        var newHeadValue = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(0, 4), lastChunkId);
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(4, 4), countInLastChunk);
        batch.Put(scripthash, newHeadValue, _cfHead);
    }

    /// <summary>
    ///     Gets the total count of TxNums for a scripthash without reading chunk data.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <returns>Total number of history entries.</returns>
    public long GetHistoryCount(byte[] scripthash)
    {
        var headValue = _db.Get(scripthash, _cfHead);
        if (headValue is null || headValue.Length != 8)
            return 0;

        var lastChunkId = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(0, 4));
        var countInLastChunk = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(4, 4));

        return (long)lastChunkId * TxNumsPerChunk + countInLastChunk;
    }

    /// <summary>
    ///     Gets all TxNums for a scripthash in append order.
    /// </summary>
    public List<ulong> GetHistory(byte[] scripthash)
    {
        if (scripthash.Length != 32)
            throw new ArgumentException("Scripthash must be 32 bytes", nameof(scripthash));

        var result = new List<ulong>();

        // Read head metadata
        var headValue = _db.Get(scripthash, _cfHead);
        if (headValue is null || headValue.Length != 8)
            return result; // No history

        var lastChunkId = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(0, 4));
        var countInLastChunk = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(4, 4));

        // Read all chunks
        for (uint chunkId = 0; chunkId <= lastChunkId; chunkId++)
        {
            var chunkKey = MakeChunkKey(scripthash, chunkId);
            var chunkData = _db.Get(chunkKey, _cfData);

            if (chunkData is null)
                continue;

            // Determine how many TxNums in this chunk
            var txNumCount = chunkId == lastChunkId ? countInLastChunk : (uint)(chunkData.Length / 6);

            // Decode TxNums
            for (var i = 0; i < txNumCount; i++)
            {
                var offset = i * 6;
                if (offset + 6 <= chunkData.Length)
                {
                    var txNum = TxNumFromBytes(chunkData.AsSpan(offset, 6));
                    result.Add(txNum);
                }
            }
        }

        return result;
    }

    /// <summary>
    ///     Deletes all TxNums >= fromTxNum from history. Used during reorg rollback.
    /// </summary>
    public void DeleteTxNumsAfter(WriteBatch batch, byte[] scripthash, ulong fromTxNum)
    {
        if (scripthash.Length != 32)
            throw new ArgumentException("Scripthash must be 32 bytes", nameof(scripthash));

        // Fast path: trim/delete from tail chunks without rebuilding full history.
        // Falls back to a full rebuild on malformed chunk state.
        var headValue = _db.Get(scripthash, _cfHead);
        if (headValue is null || headValue.Length != 8)
            return;

        var lastChunkId = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(0, 4));
        var countInLastChunk = BinaryPrimitives.ReadUInt32BigEndian(headValue.AsSpan(4, 4));

        var changed = false;
        uint? newLastChunkId = null;
        uint newCountInLastChunk = 0;

        for (var chunkIdLong = (long)lastChunkId; chunkIdLong >= 0; chunkIdLong--)
        {
            var chunkId = (uint)chunkIdLong;
            var chunkKey = MakeChunkKey(scripthash, chunkId);
            var chunkData = _db.Get(chunkKey, _cfData);
            if (chunkData is null)
            {
                DeleteTxNumsAfterFullRebuild(batch, scripthash, fromTxNum, lastChunkId);
                return;
            }

            uint txCount;
            if (chunkId == lastChunkId)
            {
                var maxCountByBytes = (uint)(chunkData.Length / TxNumCodec.Size);
                txCount = Math.Min(countInLastChunk, maxCountByBytes);
            }
            else
            {
                txCount = (uint)(chunkData.Length / TxNumCodec.Size);
            }

            if (txCount == 0)
            {
                changed = true;
                batch.Delete(chunkKey, _cfData);
                continue;
            }

            var first = TxNumFromBytes(chunkData.AsSpan(0, TxNumCodec.Size));
            var lastOffset = checked((int)((txCount - 1) * TxNumCodec.Size));
            var last = TxNumFromBytes(chunkData.AsSpan(lastOffset, TxNumCodec.Size));

            // Entire chunk is >= cutoff, delete chunk and keep scanning backwards.
            if (first >= fromTxNum)
            {
                changed = true;
                batch.Delete(chunkKey, _cfData);
                continue;
            }

            // Entire chunk is below cutoff, this chunk becomes new tail.
            if (last < fromTxNum)
            {
                if (!changed)
                    return; // No changes at all

                newLastChunkId = chunkId;
                newCountInLastChunk = txCount;
                break;
            }

            // Cutoff falls inside this chunk: keep prefix < fromTxNum.
            uint keepCount = 0;
            for (; keepCount < txCount; keepCount++)
            {
                var offset = checked((int)(keepCount * TxNumCodec.Size));
                var txNum = TxNumFromBytes(chunkData.AsSpan(offset, TxNumCodec.Size));
                if (txNum >= fromTxNum)
                    break;
            }

            changed = true;

            if (keepCount == 0)
            {
                batch.Delete(chunkKey, _cfData);
                continue;
            }

            var keepBytes = checked((int)(keepCount * TxNumCodec.Size));
            if (keepBytes < chunkData.Length)
                batch.Put(chunkKey, chunkData.AsSpan(0, keepBytes).ToArray(), _cfData);

            newLastChunkId = chunkId;
            newCountInLastChunk = keepCount;
            break;
        }

        if (!changed)
            return;

        if (!newLastChunkId.HasValue)
        {
            batch.Delete(scripthash, _cfHead);
            return;
        }

        var newHeadValue = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(0, 4), newLastChunkId.Value);
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(4, 4), newCountInLastChunk);
        batch.Put(scripthash, newHeadValue, _cfHead);
    }

    private void DeleteTxNumsAfterFullRebuild(WriteBatch batch, byte[] scripthash, ulong fromTxNum, uint lastChunkId)
    {
        var allTxNums = GetHistory(scripthash);
        var remainingTxNums = allTxNums.Where(txNum => txNum < fromTxNum).ToList();

        if (remainingTxNums.Count == allTxNums.Count)
            return;

        for (uint chunkId = 0; chunkId <= lastChunkId; chunkId++)
        {
            var chunkKey = MakeChunkKey(scripthash, chunkId);
            batch.Delete(chunkKey, _cfData);
        }

        if (remainingTxNums.Count == 0)
        {
            batch.Delete(scripthash, _cfHead);
            return;
        }

        uint currentChunkId = 0;
        var currentChunkData = new List<byte>();
        var txNumsInCurrentChunk = 0;

        foreach (var txNum in remainingTxNums)
        {
            if (txNumsInCurrentChunk >= TxNumsPerChunk)
            {
                var chunkKey = MakeChunkKey(scripthash, currentChunkId);
                batch.Put(chunkKey, currentChunkData.ToArray(), _cfData);

                currentChunkId++;
                currentChunkData.Clear();
                txNumsInCurrentChunk = 0;
            }

            var txNumBytes = TxNumToBytes(txNum);
            currentChunkData.AddRange(txNumBytes);
            txNumsInCurrentChunk++;
        }

        if (currentChunkData.Count > 0)
        {
            var chunkKey = MakeChunkKey(scripthash, currentChunkId);
            batch.Put(chunkKey, currentChunkData.ToArray(), _cfData);
        }

        var newHeadValue = new byte[8];
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(0, 4), currentChunkId);
        BinaryPrimitives.WriteUInt32BigEndian(newHeadValue.AsSpan(4, 4), (uint)txNumsInCurrentChunk);
        batch.Put(scripthash, newHeadValue, _cfHead);
    }

    /// <summary>
    ///     Converts a TxNum to a 6-byte little-endian buffer.
    /// </summary>
    /// <param name="txNum">Transaction number to convert.</param>
    /// <returns>6-byte buffer containing the encoded TxNum.</returns>
    private static byte[] TxNumToBytes(ulong txNum)
    {
        var buffer = new byte[TxNumCodec.Size];
        TxNumCodec.Encode(buffer.AsSpan(), txNum);
        return buffer;
    }

    /// <summary>
    ///     Converts a 6-byte little-endian buffer to a TxNum.
    /// </summary>
    /// <param name="bytes">Buffer containing the encoded TxNum.</param>
    /// <returns>Decoded transaction number.</returns>
    private static ulong TxNumFromBytes(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length < TxNumCodec.Size)
            throw new ArgumentException($"TxNum requires {TxNumCodec.Size} bytes");

        return TxNumCodec.Decode(bytes);
    }

    /// <summary>
    ///     Creates a chunk key from a scripthash and chunk ID.
    /// </summary>
    /// <param name="scripthash">Script hash (32 bytes).</param>
    /// <param name="chunkId">Chunk identifier.</param>
    /// <returns>36-byte key (32 bytes scripthash + 4 bytes chunk_id).</returns>
    private static byte[] MakeChunkKey(byte[] scripthash, uint chunkId)
    {
        var key = new byte[36]; // 32 bytes scripthash + 4 bytes chunk_id
        Array.Copy(scripthash, key, 32);
        BinaryPrimitives.WriteUInt32BigEndian(key.AsSpan(32, 4), chunkId);
        return key;
    }
}