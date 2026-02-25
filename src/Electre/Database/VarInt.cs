namespace Electre.Database;

/// <summary>
///     Variable-length integer encoding and decoding utilities.
///     Uses a 7-bit continuation encoding where the high bit indicates if more bytes follow.
/// </summary>
public static class VarInt
{
    /// <summary>
    ///     Encodes an unsigned 64-bit integer as variable-length bytes.
    /// </summary>
    /// <param name="value">Value to encode.</param>
    /// <param name="buffer">Destination buffer for encoded bytes.</param>
    /// <returns>Number of bytes written to the buffer.</returns>
    public static int Encode(ulong value, Span<byte> buffer)
    {
        var required = GetEncodedLength(value);
        if (buffer.Length < required)
            throw new ArgumentException($"Buffer too small: need {required} bytes, got {buffer.Length}");

        var i = 0;
        while (value >= 0x80)
        {
            buffer[i++] = (byte)(value | 0x80);
            value >>= 7;
        }

        buffer[i++] = (byte)value;
        return i;
    }

    /// <summary>
    ///     Encodes a signed 64-bit integer as variable-length bytes.
    /// </summary>
    /// <param name="value">Value to encode.</param>
    /// <param name="buffer">Destination buffer for encoded bytes.</param>
    /// <returns>Number of bytes written to the buffer.</returns>
    public static int Encode(long value, Span<byte> buffer)
    {
        return Encode((ulong)value, buffer);
    }

    /// <summary>
    ///     Encodes a signed 32-bit integer as variable-length bytes.
    /// </summary>
    /// <param name="value">Value to encode.</param>
    /// <param name="buffer">Destination buffer for encoded bytes.</param>
    /// <returns>Number of bytes written to the buffer.</returns>
    public static int Encode(int value, Span<byte> buffer)
    {
        return Encode((ulong)value, buffer);
    }

    /// <summary>
    ///     Decodes a variable-length encoded unsigned 64-bit integer.
    /// </summary>
    /// <param name="buffer">Buffer containing encoded bytes.</param>
    /// <returns>Tuple of (decoded value, number of bytes read).</returns>
    public static (ulong value, int bytesRead) Decode(ReadOnlySpan<byte> buffer)
    {
        ulong result = 0;
        var shift = 0;
        var i = 0;
        while (i < buffer.Length)
        {
            var b = buffer[i++];
            result |= (ulong)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) break;
            shift += 7;
        }

        return (result, i);
    }

    /// <summary>
    ///     Decodes a variable-length encoded signed 32-bit integer.
    /// </summary>
    /// <param name="buffer">Buffer containing encoded bytes.</param>
    /// <returns>Tuple of (decoded value, number of bytes read).</returns>
    public static (int value, int bytesRead) DecodeInt32(ReadOnlySpan<byte> buffer)
    {
        var (v, n) = Decode(buffer);
        return ((int)v, n);
    }

    /// <summary>
    ///     Decodes a variable-length encoded signed 64-bit integer.
    /// </summary>
    /// <param name="buffer">Buffer containing encoded bytes.</param>
    /// <returns>Tuple of (decoded value, number of bytes read).</returns>
    public static (long value, int bytesRead) DecodeInt64(ReadOnlySpan<byte> buffer)
    {
        var (v, n) = Decode(buffer);
        return ((long)v, n);
    }

    /// <summary>
    ///     Calculates the number of bytes required to encode a value.
    /// </summary>
    /// <param name="value">Value to calculate encoding length for.</param>
    /// <returns>Number of bytes required for encoding.</returns>
    public static int GetEncodedLength(ulong value)
    {
        var len = 1;
        while (value >= 0x80)
        {
            len++;
            value >>= 7;
        }

        return len;
    }
}