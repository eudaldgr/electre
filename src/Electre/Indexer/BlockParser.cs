using NBitcoin;

namespace Electre.Indexer;

/// <summary>
///     Parses raw block bytes into structured BlockData objects.
/// </summary>
public static class BlockParser
{
    /// <summary>
    ///     Parses a raw block into structured block data.
    /// </summary>
    /// <param name="rawBlock">Raw block bytes.</param>
    /// <param name="height">Block height.</param>
    /// <param name="network">Bitcoin network.</param>
    /// <returns>Parsed block data.</returns>
    public static BlockData Parse(byte[] rawBlock, int height, Network network)
    {
        var block = Block.Load(rawBlock, network);
        var blockHash = block.GetHash().ToBytes();

        // Pre-compute scripthashes for all outputs.
        // First pass: count total outputs to pre-allocate array.
        var totalOutputs = 0;
        foreach (var tx in block.Transactions)
            totalOutputs += tx.Outputs.Count;

        var outputScriptHashes = new byte[totalOutputs][];
        var idx = 0;

        // Second pass: fill array with computed scripthashes.
        // Iteration order MUST match Syncer.IndexBlock (transaction → output sequential).
        foreach (var tx in block.Transactions)
        foreach (var output in tx.Outputs)
            outputScriptHashes[idx++] = ScriptHashUtil.ComputeScriptHash(output.ScriptPubKey);

        return new BlockData
        {
            Height = height,
            Hash = blockHash,
            Header = block.Header.ToBytes(),
            Block = block,
            OutputScriptHashes = outputScriptHashes
        };
    }
}