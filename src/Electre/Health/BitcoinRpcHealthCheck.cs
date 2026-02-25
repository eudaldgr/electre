using Electre.Bitcoin;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Electre.Health;

/// <summary>
///     Health check implementation for Bitcoin RPC connectivity.
/// </summary>
/// <remarks>
///     Verifies that the Bitcoin Core RPC endpoint is reachable and responsive by querying the current block height.
/// </remarks>
public sealed class BitcoinRpcHealthCheck : IHealthCheck
{
    private readonly RpcClient _rpc;

    /// <summary>
    ///     Initializes a new instance of the <see cref="BitcoinRpcHealthCheck" /> class.
    /// </summary>
    /// <param name="rpc">The Bitcoin RPC client used to check connectivity.</param>
    public BitcoinRpcHealthCheck(RpcClient rpc)
    {
        _rpc = rpc;
    }

    /// <inheritdoc />
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var height = await _rpc.GetBlockCountAsync();
            return HealthCheckResult.Healthy($"Bitcoin RPC reachable. Height: {height}");
        }
        catch (Exception ex)
        {
            return HealthCheckResult.Unhealthy("Bitcoin RPC unreachable", ex);
        }
    }
}