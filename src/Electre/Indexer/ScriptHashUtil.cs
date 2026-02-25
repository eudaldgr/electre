using System.Security.Cryptography;
using NBitcoin;

namespace Electre.Indexer;

/// <summary>
///     Utility methods for computing and converting scripthashes.
/// </summary>
public static class ScriptHashUtil
{
    /// <summary>
    ///     Computes the Electrum scripthash from a script.
    ///     ScriptHash = SHA256(scriptPubKey) reversed (little-endian to big-endian).
    /// </summary>
    /// <param name="script">Bitcoin script.</param>
    /// <returns>Scripthash bytes (32 bytes, big-endian).</returns>
    public static byte[] ComputeScriptHash(Script script)
    {
        var scriptBytes = script.ToBytes();
        var hash = SHA256.HashData(scriptBytes);
        Array.Reverse(hash);
        return hash;
    }

    /// <summary>
    ///     Computes the Electrum scripthash from raw script bytes.
    /// </summary>
    /// <param name="scriptBytes">Raw script bytes.</param>
    /// <returns>Scripthash bytes (32 bytes, big-endian).</returns>
    public static byte[] ComputeScriptHash(byte[] scriptBytes)
    {
        var hash = SHA256.HashData(scriptBytes);
        Array.Reverse(hash);
        return hash;
    }

    /// <summary>
    ///     Converts scripthash bytes to hex string (Electrum format).
    /// </summary>
    /// <param name="scripthash">Scripthash bytes.</param>
    /// <returns>Scripthash as lowercase hex string.</returns>
    public static string ToHex(byte[] scripthash)
    {
        return Convert.ToHexStringLower(scripthash);
    }

    /// <summary>
    ///     Converts hex string to scripthash bytes.
    /// </summary>
    /// <param name="hex">Scripthash hex string.</param>
    /// <returns>Scripthash bytes.</returns>
    public static byte[] FromHex(string hex)
    {
        return Convert.FromHexString(hex);
    }

    /// <summary>
    ///     Converts a Bitcoin address to its Electrum scripthash.
    ///     Supports P2PKH, P2SH, bech32 (P2WPKH, P2WSH), and bech32m (P2TR) addresses.
    /// </summary>
    /// <param name="address">Bitcoin address string.</param>
    /// <param name="network">The Bitcoin network to validate the address against.</param>
    /// <returns>Scripthash bytes (32 bytes).</returns>
    public static byte[] AddressToScriptHash(string address, Network network)
    {
        var addr = BitcoinAddress.Create(address, network);
        return ComputeScriptHash(addr.ScriptPubKey);
    }
}