using Electre.Indexer;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Electre.Health;

/// <summary>
///     Health check implementation for blockchain syncer status.
/// </summary>
/// <remarks>
///     Reports healthy status when the syncer is fully synchronized with the blockchain tip,
///     and degraded status when synchronization is in progress.
/// </remarks>
public sealed class SyncerHealthCheck : IHealthCheck
{
    private readonly Syncer _syncer;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SyncerHealthCheck" /> class.
    /// </summary>
    /// <param name="syncer">The blockchain syncer instance to monitor.</param>
    public SyncerHealthCheck(Syncer syncer)
    {
        _syncer = syncer;
    }

    /// <inheritdoc />
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        if (_syncer.IsSynced)
            return Task.FromResult(HealthCheckResult.Healthy("Syncer is fully synced"));

        return Task.FromResult(HealthCheckResult.Degraded(
            $"Syncer is syncing. Current: {_syncer.CurrentHeight}, Tip: {_syncer.ChainTipHeight}"));
    }
}