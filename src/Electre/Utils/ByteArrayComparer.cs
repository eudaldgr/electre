using System.Buffers.Binary;

namespace Electre.Utils;

/// <summary>
///     Comparer for byte arrays to use as dictionary keys.
///     Provides efficient equality comparison and hashing for byte arrays.
/// </summary>
public sealed class ByteArrayComparer : IEqualityComparer<byte[]>
{
    /// <summary>
    ///     Singleton instance for reuse across the application.
    /// </summary>
    public static readonly ByteArrayComparer Instance = new();

    /// <summary>
    ///     Compares two byte arrays for equality using span-based comparison.
    /// </summary>
    /// <param name="x">First byte array to compare.</param>
    /// <param name="y">Second byte array to compare.</param>
    /// <returns>True if arrays are equal, false otherwise.</returns>
    public bool Equals(byte[]? x, byte[]? y)
    {
        if (ReferenceEquals(x, y)) return true;
        if (x is null || y is null) return false;
        return x.AsSpan().SequenceEqual(y);
    }

    /// <summary>
    ///     Computes hash code for a byte array using the first 4 bytes.
    /// </summary>
    /// <param name="obj">Byte array to hash.</param>
    /// <returns>Hash code based on first 4 bytes in little-endian format.</returns>
    public int GetHashCode(byte[] obj)
    {
        if (obj.Length >= 4)
            return BinaryPrimitives.ReadInt32LittleEndian(obj);
        return obj.Length > 0 ? obj[0] : 0;
    }
}