using Electre.Database;
using NBitcoin;
using NBitcoin.Crypto;

namespace Electre.Indexer;

/// <summary>
///     Validates block headers for correctness and chain continuity.
/// </summary>
public sealed class BlockValidator
{
    private readonly Network _network;
    private readonly Store _store;

    /// <summary>
    ///     Initializes a new instance of the BlockValidator class.
    /// </summary>
    /// <param name="store">RocksDB store for blockchain data.</param>
    /// <param name="network">Bitcoin network.</param>
    public BlockValidator(Store store, Network network)
    {
        _store = store;
        _network = network;
    }

    /// <summary>
    ///     Verifies that the genesis block in the database matches the expected network genesis.
    /// </summary>
    /// <param name="currentHeight">Current synced height.</param>
    /// <exception cref="InvalidOperationException">Thrown if genesis block hash does not match.</exception>
    public void VerifyGenesisBlock(int currentHeight)
    {
        if (currentHeight < 0)
            return;

        var genesisHeader = _store.GetBlockHeader(0);
        if (genesisHeader is null)
            return;

        var genesisHash = GetBlockHashFromHeader(genesisHeader);
        var expectedHash = _network.GetGenesis().GetHash().ToString().ToLowerInvariant();

        if (!genesisHash.Equals(expectedHash, StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                $"Genesis block mismatch! Database has {genesisHash}, expected {expectedHash} for {_network.Name}. " +
                "Database was synced for a different network. Delete database and resync.");
    }

    /// <summary>
    ///     Verifies a block header for correct size and chain linkage.
    /// </summary>
    /// <param name="height">Block height.</param>
    /// <param name="header">Block header bytes (80 bytes).</param>
    /// <param name="previousHeader">Previous block header bytes for chain verification.</param>
    /// <exception cref="InvalidOperationException">Thrown if header verification fails.</exception>
    public void VerifyBlockHeader(int height, byte[] header, byte[]? previousHeader)
    {
        if (header.Length != 80)
            throw new InvalidOperationException(
                $"Block {height} header verification failed: wrong size {header.Length}, expected 80 bytes");

        if (height == 0)
            return;

        if (previousHeader is null)
            throw new InvalidOperationException(
                $"Block {height} header verification failed: no previous header available for chain linkage check");

        var hashPrevBlock = header.AsSpan(4, 32);
        var prevHash = Hashes.DoubleSHA256(previousHeader);
        var prevHashBytes = prevHash.ToBytes();

        if (!hashPrevBlock.SequenceEqual(prevHashBytes))
        {
            var expectedHex = Convert.ToHexStringLower(prevHashBytes);
            var gotHex = Convert.ToHexStringLower(hashPrevBlock);
            throw new InvalidOperationException(
                $"Block {height} header verification failed: hashPrevBlock mismatch. " +
                $"Expected {expectedHex}, got {gotHex}");
        }
    }

    /// <summary>
    ///     Computes the block hash from a block header.
    /// </summary>
    /// <param name="header">Block header bytes (80 bytes).</param>
    /// <returns>Block hash as hex string (big-endian).</returns>
    public static string GetBlockHashFromHeader(byte[] header)
    {
        var hash = Hashes.DoubleSHA256(header);
        var hashBytes = hash.ToBytes();
        Array.Reverse(hashBytes);
        return Convert.ToHexStringLower(hashBytes);
    }
}