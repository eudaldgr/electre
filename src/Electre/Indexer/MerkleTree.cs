using System.Security.Cryptography;

namespace Electre.Indexer;

/// <summary>
///     Computes Bitcoin merkle trees and merkle proofs.
/// </summary>
public static class MerkleTree
{
    /// <summary>
    ///     Computes a merkle proof for a transaction in a block.
    /// </summary>
    /// <param name="txHashes">List of transaction hashes in the block.</param>
    /// <param name="targetTxHash">Target transaction hash to prove.</param>
    /// <returns>Tuple of (position, branch) where position is the tx index and branch is the merkle path.</returns>
    public static (int position, List<byte[]> branch) GetMerkleProof(List<byte[]> txHashes, byte[] targetTxHash)
    {
        if (txHashes.Count == 0)
            return (-1, []);

        var position = txHashes.FindIndex(h => h.SequenceEqual(targetTxHash));
        if (position < 0)
            return (-1, []);

        var branch = new List<byte[]>();
        var hashes = txHashes.Select(h => (byte[])h.Clone()).ToList();

        while (hashes.Count > 1)
        {
            if (hashes.Count % 2 == 1)
                hashes.Add((byte[])hashes[^1].Clone());

            var siblingIndex = position % 2 == 0 ? position + 1 : position - 1;
            if (siblingIndex < hashes.Count)
                branch.Add(hashes[siblingIndex]);

            var nextLevel = new List<byte[]>();
            for (var i = 0; i < hashes.Count; i += 2)
                nextLevel.Add(DoubleSha256Concat(hashes[i], hashes[i + 1]));

            position /= 2;
            hashes = nextLevel;
        }

        return (txHashes.FindIndex(h => h.SequenceEqual(targetTxHash)), branch);
    }

    /// <summary>
    ///     Computes the merkle root from a list of transaction hashes.
    /// </summary>
    /// <param name="txHashes">List of transaction hashes.</param>
    /// <returns>Merkle root hash (32 bytes).</returns>
    public static byte[] ComputeMerkleRoot(List<byte[]> txHashes)
    {
        if (txHashes.Count == 0)
            return new byte[32];

        var hashes = txHashes.Select(h => (byte[])h.Clone()).ToList();

        while (hashes.Count > 1)
        {
            if (hashes.Count % 2 == 1)
                hashes.Add((byte[])hashes[^1].Clone());

            var nextLevel = new List<byte[]>();
            for (var i = 0; i < hashes.Count; i += 2)
                nextLevel.Add(DoubleSha256Concat(hashes[i], hashes[i + 1]));

            hashes = nextLevel;
        }

        return hashes[0];
    }

    /// <summary>
    ///     Computes double SHA256 of two concatenated hashes.
    /// </summary>
    /// <param name="left">Left hash (32 bytes).</param>
    /// <param name="right">Right hash (32 bytes).</param>
    /// <returns>Double SHA256 hash (32 bytes).</returns>
    private static byte[] DoubleSha256Concat(byte[] left, byte[] right)
    {
        var combined = new byte[64];
        left.CopyTo(combined, 0);
        right.CopyTo(combined, 32);
        return SHA256.HashData(SHA256.HashData(combined));
    }
}