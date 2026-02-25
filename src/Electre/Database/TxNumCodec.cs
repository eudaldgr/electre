namespace Electre.Database;

/// <summary>
///     Encodes/decodes 48-bit transaction numbers (TxNum) as 6 bytes little-endian.
/// </summary>
public static class TxNumCodec
{
    /// <summary>
    ///     Byte length of an encoded TxNum (6 bytes = 48 bits).
    /// </summary>
    public const int Size = 6;

    /// <summary>
    ///     Encodes a 48-bit transaction number into a 6-byte little-endian buffer.
    /// </summary>
    /// <param name="buffer">Destination span that must be at least 6 bytes long.</param>
    /// <param name="txNum">Transaction number to encode (max 2^48 - 1).</param>
    public static void Encode(Span<byte> buffer, ulong txNum)
    {
        buffer[0] = (byte)(txNum & 0xFF);
        buffer[1] = (byte)((txNum >> 8) & 0xFF);
        buffer[2] = (byte)((txNum >> 16) & 0xFF);
        buffer[3] = (byte)((txNum >> 24) & 0xFF);
        buffer[4] = (byte)((txNum >> 32) & 0xFF);
        buffer[5] = (byte)((txNum >> 40) & 0xFF);
    }

    /// <summary>
    ///     Decodes a 48-bit transaction number from a 6-byte little-endian buffer.
    /// </summary>
    /// <param name="buffer">Source span containing at least 6 bytes.</param>
    /// <returns>The decoded transaction number.</returns>
    public static ulong Decode(ReadOnlySpan<byte> buffer)
    {
        return buffer[0]
               | ((ulong)buffer[1] << 8)
               | ((ulong)buffer[2] << 16)
               | ((ulong)buffer[3] << 24)
               | ((ulong)buffer[4] << 32)
               | ((ulong)buffer[5] << 40);
    }
}