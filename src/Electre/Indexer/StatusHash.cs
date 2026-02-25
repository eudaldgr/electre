using System.Security.Cryptography;
using System.Text;

namespace Electre.Indexer;

/// <summary>
///     Computes Electrum status hashes for scripthashes based on transaction history.
/// </summary>
public static class StatusHash
{
    private static readonly ObjectPool<StringBuilder> _sbPool = new(() => new StringBuilder(4096), 16);

    /// <summary>
    ///     Computes the status hash for a scripthash history.
    ///     The status hash is SHA256 of the concatenated transaction hashes and heights.
    ///     PRECONDITION: history must be pre-sorted by (height, position) - RocksDB guarantees this.
    /// </summary>
    /// <param name="history">List of (height, txHash) tuples for the scripthash.</param>
    /// <returns>Status hash hex string, or null if history is empty.</returns>
    public static string? Compute(List<(int height, byte[] txHash)> history)
    {
        if (history.Count == 0)
            return null;

        var sb = _sbPool.Rent();
        try
        {
            sb.EnsureCapacity(history.Count * 75);

            Span<byte> txHashDisplay = stackalloc byte[32];
            Span<char> hexBuffer = stackalloc char[64];

            foreach (var (height, txHash) in history)
            {
                txHash.AsSpan().CopyTo(txHashDisplay);
                txHashDisplay.Reverse();

                HexEncodeLower(txHashDisplay, hexBuffer);
                sb.Append(hexBuffer);
                sb.Append(':');
                sb.Append(height);
                sb.Append(':');
            }

            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
            return Convert.ToHexStringLower(hash);
        }
        finally
        {
            sb.Clear();
            _sbPool.Return(sb);
        }
    }

    /// <summary>
    ///     Computes the status hash for a scripthash with both confirmed and mempool history.
    ///     Confirmed entries must be pre-sorted. Mempool entries are appended after confirmed ones,
    ///     sorted by (height == 0 first, then txid hex ascending).
    /// </summary>
    /// <param name="confirmed">List of (height, txHash) tuples for confirmed transactions.</param>
    /// <param name="mempool">List of (height, txHash) tuples for mempool transactions.</param>
    /// <returns>Status hash hex string, or null if both lists are empty.</returns>
    public static string? Compute(List<(int height, byte[] txHash)> confirmed,
        List<(int height, byte[] txHash)> mempool)
    {
        if (confirmed.Count == 0 && mempool.Count == 0)
            return null;

        var sb = _sbPool.Rent();
        try
        {
            sb.EnsureCapacity((confirmed.Count + mempool.Count) * 75);

            Span<byte> txHashDisplay = stackalloc byte[32];
            Span<char> hexBuffer = stackalloc char[64];

            foreach (var (height, txHash) in confirmed)
            {
                txHash.AsSpan().CopyTo(txHashDisplay);
                txHashDisplay.Reverse();

                HexEncodeLower(txHashDisplay, hexBuffer);
                sb.Append(hexBuffer);
                sb.Append(':');
                sb.Append(height);
                sb.Append(':');
            }

            foreach (var (height, txHash) in mempool)
            {
                txHash.AsSpan().CopyTo(txHashDisplay);
                txHashDisplay.Reverse();

                HexEncodeLower(txHashDisplay, hexBuffer);
                sb.Append(hexBuffer);
                sb.Append(':');
                sb.Append(height);
                sb.Append(':');
            }

            if (sb.Length == 0)
                return null;

            var hash = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
            return Convert.ToHexStringLower(hash);
        }
        finally
        {
            sb.Clear();
            _sbPool.Return(sb);
        }
    }

    /// <summary>
    ///     Encodes bytes to lowercase hexadecimal characters.
    /// </summary>
    /// <param name="bytes">Bytes to encode.</param>
    /// <param name="chars">Output character buffer (must be at least 2 * bytes.Length).</param>
    private static void HexEncodeLower(ReadOnlySpan<byte> bytes, Span<char> chars)
    {
        const string hexChars = "0123456789abcdef";
        for (var i = 0; i < bytes.Length; i++)
        {
            chars[i * 2] = hexChars[bytes[i] >> 4];
            chars[i * 2 + 1] = hexChars[bytes[i] & 0xF];
        }
    }
}

/// <summary>
///     Generic object pool for reusing objects to reduce garbage collection pressure.
/// </summary>
/// <typeparam name="T">Type of objects to pool.</typeparam>
internal sealed class ObjectPool<T> where T : class
{
    private readonly Func<T> _factory;
    private readonly T?[] _items;
    private readonly object _lock = new();
    private int _index;

    /// <summary>
    ///     Initializes a new instance of the ObjectPool class.
    /// </summary>
    /// <param name="factory">Factory function to create new objects.</param>
    /// <param name="size">Maximum pool size.</param>
    public ObjectPool(Func<T> factory, int size)
    {
        _factory = factory;
        _items = new T?[size];
    }

    /// <summary>
    ///     Rents an object from the pool, or creates a new one if pool is empty.
    /// </summary>
    /// <returns>Object instance.</returns>
    public T Rent()
    {
        lock (_lock)
        {
            if (_index > 0)
            {
                _index--;
                var item = _items[_index]!;
                _items[_index] = null;
                return item;
            }
        }

        return _factory();
    }

    /// <summary>
    ///     Returns an object to the pool for reuse.
    /// </summary>
    /// <param name="item">Object to return.</param>
    public void Return(T item)
    {
        lock (_lock)
        {
            if (_index < _items.Length)
            {
                _items[_index] = item;
                _index++;
            }
        }
    }
}