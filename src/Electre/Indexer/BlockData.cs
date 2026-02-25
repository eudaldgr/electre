using NBitcoin;

namespace Electre.Indexer;

/// <summary>
///     Represents a parsed block with its header and transactions.
///     Stores the original NBitcoin Block object and pre-computed scripthashes to minimize allocations.
/// </summary>
public sealed class BlockData
{
    /// <summary>
    ///     Gets the block height.
    /// </summary>
    public required int Height { get; init; }

    /// <summary>
    ///     Gets the block hash bytes (32 bytes, little-endian).
    /// </summary>
    public required byte[] Hash { get; init; }

    /// <summary>
    ///     Gets the block header bytes (80 bytes).
    /// </summary>
    public required byte[] Header { get; init; }

    /// <summary>
    ///     Gets the original parsed NBitcoin Block object.
    /// </summary>
    public required Block Block { get; init; }

    /// <summary>
    ///     Gets the pre-computed scripthashes for all outputs in the block.
    ///     This is a jagged array where OutputScriptHashes[i] corresponds to the scripthash of the i-th output encountered
    ///     when iterating through transactions and their outputs sequentially.
    /// </summary>
    public required byte[][] OutputScriptHashes { get; init; }
}