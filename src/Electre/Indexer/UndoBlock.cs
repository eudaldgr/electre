namespace Electre.Indexer;

/// <summary>
///     Stores undo information for a block to enable reorg recovery.
///     Contains spent UTXOs, created UTXOs, and history entries that need to be reversed.
/// </summary>
public sealed class UndoBlock
{
    /// <summary>
    ///     Gets the list of UTXOs that were spent in this block.
    /// </summary>
    public List<SpentUtxo> SpentUtxos { get; } = [];

    /// <summary>
    ///     Gets the list of UTXOs that were created in this block.
    /// </summary>
    public List<CreatedUtxo> CreatedUtxos { get; } = [];

    /// <summary>
    ///     Gets the list of history entries for affected scripthashes.
    /// </summary>
    public List<HistoryEntry> HistoryEntries { get; } = [];

    /// <summary>
    ///     Serializes the undo block to binary format.
    /// </summary>
    /// <returns>Serialized undo block bytes.</returns>
    public byte[] Serialize()
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms);

        bw.Write(SpentUtxos.Count);
        foreach (var u in SpentUtxos)
        {
            bw.Write(u.Scripthash);
            WriteTxNum(bw, u.TxNum);
            bw.Write(u.OutputIndex);
            bw.Write(u.Height);
            bw.Write(u.Value);
        }

        bw.Write(CreatedUtxos.Count);
        foreach (var u in CreatedUtxos)
        {
            bw.Write(u.Scripthash);
            WriteTxNum(bw, u.TxNum);
            bw.Write(u.OutputIndex);
        }

        bw.Write(HistoryEntries.Count);
        foreach (var h in HistoryEntries)
        {
            bw.Write(h.Scripthash);
            WriteTxNum(bw, h.TxNum);
        }

        return ms.ToArray();
    }

    /// <summary>
    ///     Deserializes an undo block from binary format.
    /// </summary>
    /// <param name="data">Serialized undo block bytes.</param>
    /// <returns>Deserialized UndoBlock object.</returns>
    public static UndoBlock Deserialize(byte[] data)
    {
        var undo = new UndoBlock();
        using var ms = new MemoryStream(data);
        using var br = new BinaryReader(ms);

        var spentCount = br.ReadInt32();
        for (var i = 0; i < spentCount; i++)
            undo.SpentUtxos.Add(new SpentUtxo(
                br.ReadBytes(32),
                ReadTxNum(br),
                br.ReadInt32(),
                br.ReadInt32(),
                br.ReadInt64()
            ));

        var createdCount = br.ReadInt32();
        for (var i = 0; i < createdCount; i++)
            undo.CreatedUtxos.Add(new CreatedUtxo(
                br.ReadBytes(32),
                ReadTxNum(br),
                br.ReadInt32()
            ));

        var historyCount = br.ReadInt32();
        for (var i = 0; i < historyCount; i++)
            undo.HistoryEntries.Add(new HistoryEntry(
                br.ReadBytes(32),
                ReadTxNum(br)
            ));

        return undo;
    }

    /// <summary>
    ///     Writes a transaction number (6 bytes, little-endian) to a binary writer.
    /// </summary>
    /// <param name="bw">Binary writer.</param>
    /// <param name="txNum">Transaction number to write.</param>
    private static void WriteTxNum(BinaryWriter bw, ulong txNum)
    {
        bw.Write((byte)((txNum >> 0) & 0xFF));
        bw.Write((byte)((txNum >> 8) & 0xFF));
        bw.Write((byte)((txNum >> 16) & 0xFF));
        bw.Write((byte)((txNum >> 24) & 0xFF));
        bw.Write((byte)((txNum >> 32) & 0xFF));
        bw.Write((byte)((txNum >> 40) & 0xFF));
    }

    /// <summary>
    ///     Reads a transaction number (6 bytes, little-endian) from a binary reader.
    /// </summary>
    /// <param name="br">Binary reader.</param>
    /// <returns>Transaction number.</returns>
    private static ulong ReadTxNum(BinaryReader br)
    {
        var bytes = br.ReadBytes(6);
        return ((ulong)bytes[0] << 0)
               | ((ulong)bytes[1] << 8)
               | ((ulong)bytes[2] << 16)
               | ((ulong)bytes[3] << 24)
               | ((ulong)bytes[4] << 32)
               | ((ulong)bytes[5] << 40);
    }
}

/// <summary>
///     Represents a UTXO that was spent in a block.
/// </summary>
/// <param name="Scripthash">Scripthash bytes (32 bytes).</param>
/// <param name="TxNum">Transaction number of the spending transaction.</param>
/// <param name="OutputIndex">Output index in the previous transaction.</param>
/// <param name="Height">Block height where the UTXO was created.</param>
/// <param name="Value">UTXO value in satoshis.</param>
public record SpentUtxo(byte[] Scripthash, ulong TxNum, int OutputIndex, int Height, long Value);

/// <summary>
///     Represents a UTXO that was created in a block.
/// </summary>
/// <param name="Scripthash">Scripthash bytes (32 bytes).</param>
/// <param name="TxNum">Transaction number that created this UTXO.</param>
/// <param name="OutputIndex">Output index in the transaction.</param>
public record CreatedUtxo(byte[] Scripthash, ulong TxNum, int OutputIndex);

/// <summary>
///     Represents a history entry for a scripthash affected by a block.
/// </summary>
/// <param name="Scripthash">Scripthash bytes (32 bytes).</param>
/// <param name="TxNum">Transaction number in the history.</param>
public record HistoryEntry(byte[] Scripthash, ulong TxNum);