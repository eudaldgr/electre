using System.Collections.Concurrent;
using System.Net;
using System.Text.Json.Nodes;
using Electre.Bitcoin;
using Electre.Database;
using Electre.Indexer;
using Electre.Server;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NBitcoin;
using NBitcoin.Crypto;

namespace Electre.Protocol;

/// <summary>
///     Implements all Electrum protocol 1.6 method handlers for blockchain queries and subscriptions.
/// </summary>
public sealed class Methods
{
    /// <summary>
    ///     Server version string.
    /// </summary>
    private const string ServerVersion = "Electre 0.1.0";

    /// <summary>
    ///     Minimum supported Electrum protocol version.
    /// </summary>
    private const string ProtocolMin = "1.6";

    /// <summary>
    ///     Maximum supported Electrum protocol version.
    /// </summary>
    private const string ProtocolMax = "1.6";

    /// <summary>
    ///     LRU cache for Merkle proofs to avoid recomputation.
    /// </summary>
    private static readonly LruCache<string, (int pos, List<byte[]> branch)> _merkleCache = new(10_000);

    /// <summary>
    ///     Lock protecting shared cp_height Merkle cache.
    /// </summary>
    private static readonly object HeaderCheckpointCacheLock = new();

    /// <summary>
    ///     Shared cp_height Merkle cache state across sessions.
    /// </summary>
    private static HeaderCheckpointCacheState _headerCheckpointCache = HeaderCheckpointCacheState.Empty;

    /// <summary>
    ///     Global semaphore limiting total concurrent RPC proxy calls to Bitcoin Core.
    /// </summary>
    private static SemaphoreSlim? _rpcGlobalGate;

    /// <summary>
    ///     Per-IP semaphores limiting concurrent RPC proxy calls per client IP.
    /// </summary>
    private static readonly ConcurrentDictionary<IPAddress, SemaphoreSlim> _rpcPerIpGates = new();

    /// <summary>
    ///     Server configuration.
    /// </summary>
    private readonly Config _config;

    /// <summary>
    ///     Logger instance for RPC method execution.
    /// </summary>
    private readonly ILogger _logger;

    private readonly int _maxConcurrentRpcProxy;

    /// <summary>
    ///     Bitcoin network (mainnet, testnet, etc.).
    /// </summary>
    private readonly Network _network;

    /// <summary>
    ///     Bitcoin Core RPC client for blockchain operations.
    /// </summary>
    private readonly RpcClient _rpc;

    private readonly int _rpcProxyPerIpBurst;

    /// <summary>
    ///     RocksDB store for blockchain data access.
    /// </summary>
    private readonly Store _store;

    /// <summary>
    ///     Subscription manager for client notifications.
    /// </summary>
    private readonly SubscriptionManager _subscriptions;

    /// <summary>
    ///     Blockchain synchronizer for index state and balance queries.
    /// </summary>
    private readonly Syncer _syncer;

    /// <summary>
    ///     Initializes a new instance of the Methods class.
    /// </summary>
    /// <param name="store">The blockchain data store.</param>
    /// <param name="rpc">The Bitcoin Core RPC client.</param>
    /// <param name="syncer">The blockchain synchronizer.</param>
    /// <param name="subscriptions">The subscription manager.</param>
    /// <param name="config">The server configuration.</param>
    /// <param name="network">The Bitcoin network.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public Methods(Store store, RpcClient rpc, Syncer syncer, SubscriptionManager subscriptions,
        IOptions<Config> config, Network network, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("RPC");
        _store = store;
        _rpc = rpc;
        _syncer = syncer;
        _subscriptions = subscriptions;
        _config = config.Value;
        _network = network;
        _maxConcurrentRpcProxy = _config.MaxConcurrentRpcProxy;
        _rpcProxyPerIpBurst = _config.RpcProxyPerIpBurst;

        // Initialize global gate once (thread-safe via CompareExchange)
        if (_rpcGlobalGate is null)
        {
            var gate = new SemaphoreSlim(_maxConcurrentRpcProxy, _maxConcurrentRpcProxy);
            Interlocked.CompareExchange(ref _rpcGlobalGate, gate, null);
            if (_rpcGlobalGate != gate)
                gate.Dispose();
        }
    }

    /// <summary>
    ///     Handles an incoming Electrum protocol method call.
    /// </summary>
    /// <param name="method">The method name (e.g., "server.version", "blockchain.scripthash.get_balance").</param>
    /// <param name="args">The method parameters.</param>
    /// <param name="session">The client session context.</param>
    /// <returns>The method result, or null for methods that return no value.</returns>
    /// <exception cref="JsonRpcException">Thrown if the method is not found.</exception>
    public async Task<object?> HandleAsync(string method, object?[] args, IClientSession session)
    {
        _logger.LogDebug("[{SessionId}] {Method}", session.Id, method);

        return method switch
        {
            "server.version" => ServerVersionHandler(args),
            "server.ping" => null,
            "server.banner" => _config.ServerBanner,
            "server.donation_address" => _config.DonationAddress,
            "server.features" => GetFeatures(),
            "server.add_peer" => false,
            "server.peers.subscribe" => Array.Empty<object>(),

            "blockchain.block.header" => await GetBlockHeader(args),
            "blockchain.block.headers" => await GetBlockHeaders(args),
            "blockchain.headers.subscribe" => await HeadersSubscribe(session),
            "blockchain.scripthash.get_balance" => GetScripthashBalance(args),
            "blockchain.scripthash.get_history" => GetScripthashHistory(args),
            "blockchain.scripthash.get_mempool" => GetScripthashMempool(args),
            "blockchain.scripthash.listunspent" => GetScripthashUtxos(args),
            "blockchain.scripthash.subscribe" => await ScripthashSubscribe(args, session),
            "blockchain.scripthash.unsubscribe" => ScripthashUnsubscribe(args, session),
            "blockchain.transaction.get" => await GetTransaction(args, session),
            "blockchain.transaction.get_merkle" => await GetTransactionMerkle(args, session),
            "blockchain.transaction.broadcast" => await BroadcastTransaction(args, session),
            "blockchain.transaction.id_from_pos" => await IdFromPos(args, session),
            "blockchain.transaction.broadcast_package" => await BroadcastPackage(args, session),
            "blockchain.estimatefee" => await EstimateFee(args, session),
            "blockchain.relayfee" => 0.00001, // Deprecated in 1.6, replaced by mempool.get_info. Kept for backward compatibility.

            "blockchain.address.get_balance" => GetAddressBalance(args),
            "blockchain.address.get_history" => GetAddressHistory(args),
            "blockchain.address.get_mempool" => GetAddressMempool(args),
            "blockchain.address.get_scripthash" => GetAddressScripthash(args),
            "blockchain.address.listunspent" => GetAddressUtxos(args),
            "blockchain.address.subscribe" => await AddressSubscribe(args, session),
            "blockchain.address.unsubscribe" => AddressUnsubscribe(args, session),

            "mempool.get_fee_histogram" => GetFeeHistogram(),
            "mempool.get_info" => await GetMempoolInfo(session),

            _ => throw new JsonRpcException(JsonRpcErrorCodes.MethodNotFound, $"Unknown method: {method}")
        };
    }

    /// <summary>
    ///     Handles the server.version method call.
    ///     Protocol 1.6: Tolerates extraneous arguments to allow for future protocol extensions.
    /// </summary>
    /// <param name="args">The method parameters (tolerates any number of extraneous args per 1.6 spec).</param>
    /// <returns>An array containing the server version and maximum protocol version.</returns>
    private object ServerVersionHandler(object?[] args)
    {
        return new[] { ServerVersion, ProtocolMax };
    }

    /// <summary>
    ///     Handles the server.features method call.
    /// </summary>
    /// <returns>A dictionary containing server capabilities including genesis hash, protocol versions, and network ports.</returns>
    private object GetFeatures()
    {
        var genesisHash = _network.GetGenesis().GetHash().ToString().ToLowerInvariant();

        var hosts = new Dictionary<string, object>
        {
            ["tcp_port"] = _config.ServerPort
        };

        if (_config.SslEnabled) hosts["ssl_port"] = _config.SslPort;

        return new Dictionary<string, object?>
        {
            ["genesis_hash"] = genesisHash,
            ["hosts"] = new Dictionary<string, object> { [""] = hosts },
            ["protocol_max"] = ProtocolMax,
            ["protocol_min"] = ProtocolMin,
            ["pruning"] = null,
            ["server_version"] = ServerVersion,
            ["hash_function"] = "sha256"
        };
    }

    /// <summary>
    ///     Wraps an RPC proxy call with global and per-IP concurrency throttling.
    /// </summary>
    private async Task<T> WithRpcThrottle<T>(IClientSession session, Func<Task<T>> rpcCall)
    {
        if (session.RemoteIp is not null)
        {
            var ipGate = _rpcPerIpGates.GetOrAdd(session.RemoteIp,
                _ => new SemaphoreSlim(_rpcProxyPerIpBurst, _rpcProxyPerIpBurst));

            if (!await ipGate.WaitAsync(0))
                throw new JsonRpcException(JsonRpcErrorCodes.ServerBusy,
                    "Too many concurrent RPC requests from this IP");
            try
            {
                if (!await _rpcGlobalGate!.WaitAsync(TimeSpan.FromSeconds(5)))
                    throw new JsonRpcException(JsonRpcErrorCodes.ServerBusy, "Server RPC capacity exceeded");
                try
                {
                    return await rpcCall();
                }
                finally
                {
                    _rpcGlobalGate.Release();
                }
            }
            finally
            {
                ipGate.Release();
            }
        }

        if (!await _rpcGlobalGate!.WaitAsync(TimeSpan.FromSeconds(5)))
            throw new JsonRpcException(JsonRpcErrorCodes.ServerBusy, "Server RPC capacity exceeded");
        try
        {
            return await rpcCall();
        }
        finally
        {
            _rpcGlobalGate.Release();
        }
    }

    /// <summary>
    ///     Handles the blockchain.block.header method call.
    /// </summary>
    /// <param name="args">The method parameters: [height, cp_height (optional)].</param>
    /// <returns>The block header as a hex string, or a dictionary with header, root, and branch if cp_height is provided.</returns>
    /// <exception cref="JsonRpcException">Thrown if height parameter is missing or block not found.</exception>
    private Task<object> GetBlockHeader(object?[] args)
    {
        if (args.Length < 1)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "Missing height parameter");

        var height = GetIntArg(args, 0, "height");
        var cpHeight = args.Length > 1 ? GetIntArg(args, 1, "cp_height") : 0;
        var tip = _syncer.CurrentHeight;

        if (height < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "height must be >= 0");
        if (cpHeight < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "cp_height must be >= 0");
        if (cpHeight > 0 && !(height <= cpHeight && cpHeight <= tip))
            throw new JsonRpcException(
                JsonRpcErrorCodes.InvalidParams,
                $"header height {height} must be <= cp_height {cpHeight} which must be <= chain height {tip}");

        var header = _store.GetBlockHeader(height);
        if (header is null)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Block not found");

        var headerHex = Convert.ToHexStringLower(header);

        if (cpHeight > 0)
        {
            var (root, branch) = GetHeaderCheckpointProof(height, cpHeight);
            return Task.FromResult<object>(new Dictionary<string, object>
            {
                ["header"] = headerHex,
                ["root"] = root,
                ["branch"] = branch
            });
        }

        return Task.FromResult<object>(headerHex);
    }

    /// <summary>
    ///     Handles the blockchain.block.headers method call.
    ///     Protocol 1.6 format: returns headers as an array of individual hex strings.
    /// </summary>
    /// <param name="args">The method parameters: [start_height, count, cp_height (optional)].</param>
    /// <returns>A dictionary containing the count of headers, array of header hex strings, and maximum headers per request.</returns>
    /// <exception cref="JsonRpcException">Thrown if required parameters are missing.</exception>
    private Task<object> GetBlockHeaders(object?[] args)
    {
        if (args.Length < 2)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "Missing parameters");

        var startHeight = GetIntArg(args, 0, "start_height");
        var requestedCount = GetIntArg(args, 1, "count");
        var cpHeight = args.Length > 2 ? GetIntArg(args, 2, "cp_height") : 0;
        var tip = _syncer.CurrentHeight;

        if (startHeight < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "start_height must be >= 0");
        if (requestedCount < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "count must be >= 0");
        if (cpHeight < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "cp_height must be >= 0");

        var maxAvailable = tip >= startHeight ? tip - startHeight + 1 : 0;
        var count = Math.Min(Math.Min(requestedCount, 2016), maxAvailable);

        var headers = _store.GetBlockHeaders(startHeight, count);
        var headerHexArray = headers.Select(h => Convert.ToHexStringLower(h)).ToArray();

        var response = new Dictionary<string, object>
        {
            ["count"] = headers.Count,
            ["headers"] = headerHexArray,
            ["max"] = 2016
        };

        if (cpHeight > 0 && headers.Count > 0)
        {
            var lastHeight = startHeight + headers.Count - 1;
            if (!(lastHeight <= cpHeight && cpHeight <= tip))
                throw new JsonRpcException(
                    JsonRpcErrorCodes.InvalidParams,
                    $"header height + (count - 1) {lastHeight} must be <= cp_height {cpHeight} which must be <= chain height {tip}");

            var (root, branch) = GetHeaderCheckpointProof(lastHeight, cpHeight);
            response["root"] = root;
            response["branch"] = branch;
        }

        return Task.FromResult<object>(response);
    }

    private (string root, string[] branch) GetHeaderCheckpointProof(int targetHeight, int cpHeight)
    {
        if (targetHeight > cpHeight)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "height must be <= cp_height");

        var cache = GetOrBuildHeaderCheckpointCache(cpHeight);
        var branchBytes = BuildBranchFromLevels(cache.Levels, targetHeight);
        return (ToDisplayHashHex(cache.Root), branchBytes.Select(ToDisplayHashHex).ToArray());
    }

    private HeaderCheckpointCacheState GetOrBuildHeaderCheckpointCache(int cpHeight)
    {
        lock (HeaderCheckpointCacheLock)
        {
            var state = _headerCheckpointCache;

            if (state.CpHeight >= 0)
            {
                if (!IsCacheTipStillValid(state))
                    state = HeaderCheckpointCacheState.Empty;
                else if (cpHeight < state.CpHeight) state = HeaderCheckpointCacheState.Empty;
            }

            if (state.CpHeight < 0)
            {
                var headers = _store.GetBlockHeaders(0, cpHeight + 1);
                if (headers.Count != cpHeight + 1)
                    throw new JsonRpcException(JsonRpcErrorCodes.ServerError,
                        "Insufficient header chain for cp_height proof");

                var leaves = ComputeHeaderHashes(headers);
                var levels = BuildMerkleLevels(leaves);
                var tipHash = leaves[^1];
                state = new HeaderCheckpointCacheState(cpHeight, leaves, levels, tipHash);
            }
            else if (cpHeight > state.CpHeight)
            {
                var appendStart = state.CpHeight + 1;
                var appendCount = cpHeight - state.CpHeight;
                var headers = _store.GetBlockHeaders(appendStart, appendCount);
                if (headers.Count != appendCount)
                    throw new JsonRpcException(JsonRpcErrorCodes.ServerError,
                        "Insufficient header chain for cp_height proof");

                var appendedLeaves = ComputeHeaderHashes(headers);
                var leaves = new List<byte[]>(state.Leaves.Count + appendedLeaves.Count);
                leaves.AddRange(state.Leaves);
                leaves.AddRange(appendedLeaves);

                var levels = BuildMerkleLevels(leaves);
                state = new HeaderCheckpointCacheState(cpHeight, leaves, levels, leaves[^1]);
            }

            _headerCheckpointCache = state;
            return state;
        }
    }

    private bool IsCacheTipStillValid(HeaderCheckpointCacheState state)
    {
        if (state.CpHeight < 0 || state.Leaves.Count == 0)
            return false;

        var tipHeader = _store.GetBlockHeader(state.CpHeight);
        if (tipHeader is null || tipHeader.Length != 80)
            return false;

        var tipHash = Hashes.DoubleSHA256(tipHeader).ToBytes();
        return tipHash.AsSpan().SequenceEqual(state.TipHash);
    }

    private static List<byte[]> ComputeHeaderHashes(List<byte[]> headers)
    {
        var hashes = new List<byte[]>(headers.Count);
        foreach (var header in headers)
        {
            if (header.Length != 80)
                throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Invalid header size in store");

            hashes.Add(Hashes.DoubleSHA256(header).ToBytes());
        }

        return hashes;
    }

    private static List<List<byte[]>> BuildMerkleLevels(List<byte[]> leaves)
    {
        if (leaves.Count == 0)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Failed to construct cp_height proof");

        var levels = new List<List<byte[]>>(32) { new(leaves) };
        while (levels[^1].Count > 1)
        {
            var current = levels[^1];
            var next = new List<byte[]>((current.Count + 1) / 2);
            for (var i = 0; i < current.Count; i += 2)
            {
                var left = current[i];
                var right = i + 1 < current.Count ? current[i + 1] : current[i];
                next.Add(DoubleSha256Concat(left, right));
            }

            levels.Add(next);
        }

        return levels;
    }

    private static List<byte[]> BuildBranchFromLevels(List<List<byte[]>> levels, int targetIndex)
    {
        if (levels.Count == 0 || levels[0].Count == 0)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Failed to construct cp_height proof");
        if (targetIndex < 0 || targetIndex >= levels[0].Count)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Failed to construct cp_height proof");

        var branch = new List<byte[]>(levels.Count - 1);
        var index = targetIndex;
        for (var levelIndex = 0; levelIndex < levels.Count - 1; levelIndex++)
        {
            var level = levels[levelIndex];
            var siblingIndex = index % 2 == 0 ? index + 1 : index - 1;
            if (siblingIndex >= level.Count)
                siblingIndex = index;

            branch.Add(level[siblingIndex]);
            index /= 2;
        }

        return branch;
    }

    private static byte[] DoubleSha256Concat(byte[] left, byte[] right)
    {
        Span<byte> combined = stackalloc byte[64];
        left.CopyTo(combined[..32]);
        right.CopyTo(combined[32..]);
        return Hashes.DoubleSHA256(combined.ToArray()).ToBytes();
    }

    private static string ToDisplayHashHex(byte[] hash)
    {
        var display = hash.ToArray();
        Array.Reverse(display);
        return Convert.ToHexStringLower(display);
    }

    private static string ToDisplayTxHash(byte[] txHash)
    {
        Span<byte> display = stackalloc byte[txHash.Length];
        txHash.CopyTo(display);
        display.Reverse();
        return Convert.ToHexStringLower(display);
    }

    /// <summary>
    ///     Handles the blockchain.headers.subscribe method call.
    /// </summary>
    /// <param name="session">The client session to subscribe.</param>
    /// <returns>A dictionary containing the current block height and header hex.</returns>
    private async Task<object> HeadersSubscribe(IClientSession session)
    {
        _subscriptions.SubscribeHeaders(session);

        var height = _syncer.CurrentHeight;
        var header = _syncer.CurrentHeader;

        if (header is null)
            return new Dictionary<string, object> { ["height"] = 0, ["hex"] = "" };

        return new Dictionary<string, object>
        {
            ["height"] = height,
            ["hex"] = Convert.ToHexStringLower(header)
        };
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.get_balance method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <returns>A dictionary containing confirmed and unconfirmed balance in satoshis.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private object GetScripthashBalance(object?[] args)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");
        var scripthash = ScriptHashUtil.FromHex(scripthashHex);

        try
        {
            var (confirmed, unconfirmed) = _syncer.GetBalance(scripthash);
            return new Dictionary<string, object>
            {
                ["confirmed"] = confirmed,
                ["unconfirmed"] = unconfirmed
            };
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.get_history method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <returns>An array of transaction history entries with height and tx_hash, including fee for unconfirmed transactions.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private object GetScripthashHistory(object?[] args)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");
        var scripthash = ScriptHashUtil.FromHex(scripthashHex);

        try
        {
            var historyWithFee = _syncer.GetHistoryWithFee(scripthash);

            return historyWithFee.Select(h =>
            {
                var result = new Dictionary<string, object>
                {
                    ["height"] = h.height,
                    ["tx_hash"] = ToDisplayTxHash(h.txHash)
                };

                if (h is { height: <= 0, fee: > 0 })
                    result["fee"] = h.fee;

                return result;
            }).ToArray();
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.get_mempool method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <returns>An array of unconfirmed transactions in the mempool for the scripthash.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private object GetScripthashMempool(object?[] args)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");

        var mempool = _syncer.Mempool;
        if (mempool is null)
            return Array.Empty<object>();

        var scripthash = ScriptHashUtil.FromHex(scripthashHex);
        var mempoolTxs = mempool.GetMempoolHistory(scripthash);

        return mempoolTxs.Select(m =>
        {
            return new Dictionary<string, object>
            {
                ["tx_hash"] = ToDisplayTxHash(m.txHash),
                ["height"] = m.height,
                ["fee"] = m.fee
            };
        }).ToArray();
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.listunspent method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <returns>An array of unspent transaction outputs (UTXOs) for the scripthash.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private object GetScripthashUtxos(object?[] args)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");
        var scripthash = ScriptHashUtil.FromHex(scripthashHex);
        var utxos = _syncer.GetUtxos(scripthash);

        return utxos.Select(u =>
        {
            return new Dictionary<string, object>
            {
                ["tx_hash"] = ToDisplayTxHash(u.txHash),
                ["tx_pos"] = u.outputIndex,
                ["height"] = u.height,
                ["value"] = u.value
            };
        }).ToArray();
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.subscribe method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <param name="session">The client session to subscribe.</param>
    /// <returns>The current status hash for the scripthash.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private async Task<object?> ScripthashSubscribe(object?[] args, IClientSession session)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");

        try
        {
            _subscriptions.SubscribeScripthash(session, scripthashHex);

            var scripthash = ScriptHashUtil.FromHex(scripthashHex);
            var status = _syncer.GetStatus(scripthash);

            return status;
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
        catch (SubscriptionLimitException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    /// <summary>
    ///     Handles the blockchain.scripthash.unsubscribe method call.
    /// </summary>
    /// <param name="args">The method parameters: [scripthash].</param>
    /// <param name="session">The client session to unsubscribe.</param>
    /// <returns>True if the subscription was removed, false otherwise.</returns>
    /// <exception cref="JsonRpcException">Thrown if scripthash parameter is missing.</exception>
    private bool ScripthashUnsubscribe(object?[] args, IClientSession session)
    {
        var scripthashHex = GetStringArg(args, 0, "scripthash");
        return _subscriptions.UnsubscribeScripthash(session, scripthashHex);
    }

    /// <summary>
    ///     Handles the blockchain.transaction.get method call.
    /// </summary>
    /// <param name="args">The method parameters: [txid, verbose (optional)].</param>
    /// <returns>The raw transaction hex, or a dictionary with hex, txid, and confirmations if verbose is true.</returns>
    /// <exception cref="JsonRpcException">Thrown if txid parameter is missing or transaction not found.</exception>
    private async Task<object> GetTransaction(object?[] args, IClientSession session)
    {
        var txidHex = GetStringArg(args, 0, "txid");
        var verbose = args.Length > 1 && args[1] is true;

        // Check mempool first (has raw tx cached)
        var txidBytes = Convert.FromHexString(txidHex);
        Array.Reverse(txidBytes);
        var mempoolRaw = _syncer.Mempool?.GetRawTransaction(txidBytes);
        if (mempoolRaw is not null)
        {
            var rawHex = Convert.ToHexStringLower(mempoolRaw);
            if (!verbose)
                return rawHex;

            return new Dictionary<string, object>
            {
                ["hex"] = rawHex,
                ["txid"] = txidHex,
                ["confirmations"] = 0
            };
        }

        // For confirmed transactions without txindex, use blockhash when height is known.
        var txData = _store.GetTransaction(txidBytes);
        string hex;
        string? blockHash = null;
        try
        {
            if (txData.HasValue)
            {
                blockHash = await WithRpcThrottle(session, () => _rpc.GetBlockHashAsync(txData.Value.height));
                hex = await WithRpcThrottle(session, () => _rpc.GetRawTransactionHexAsync(txidHex, blockHash));
            }
            else
            {
                hex = await WithRpcThrottle(session, () => _rpc.GetRawTransactionHexAsync(txidHex));
            }
        }
        catch (RpcException)
        {
            try
            {
                // Fallback path for txindex-enabled nodes or stale local tx metadata.
                hex = await WithRpcThrottle(session, () => _rpc.GetRawTransactionHexAsync(txidHex));
            }
            catch (RpcException)
            {
                throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Transaction not found");
            }
        }

        if (string.IsNullOrWhiteSpace(hex))
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Transaction not found");

        if (!verbose)
            return hex;

        // Get confirmation count from our index if available
        var confirmations = txData.HasValue
            ? _syncer.CurrentHeight - txData.Value.height + 1
            : 0;

        if (confirmations > 0)
            return new Dictionary<string, object>
            {
                ["hex"] = hex,
                ["txid"] = txidHex,
                ["confirmations"] = confirmations
            };

        try
        {
            var verboseData =
                await WithRpcThrottle(session, () => _rpc.GetRawTransactionVerboseAsync(txidHex, blockHash));
            return (object?)verboseData ?? new Dictionary<string, object> { ["hex"] = hex, ["txid"] = txidHex };
        }
        catch (RpcException)
        {
            return new Dictionary<string, object> { ["hex"] = hex, ["txid"] = txidHex };
        }
    }

    /// <summary>
    ///     Handles the blockchain.transaction.get_merkle method call.
    /// </summary>
    /// <param name="args">The method parameters: [txid, height].</param>
    /// <returns>A dictionary containing the block height, Merkle proof branch, and transaction position in the block.</returns>
    /// <exception cref="JsonRpcException">Thrown if parameters are missing, block not found, or transaction not in block.</exception>
    private async Task<object> GetTransactionMerkle(object?[] args, IClientSession session)
    {
        if (args.Length < 2)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "Missing parameters");

        var txidHex = GetStringArg(args, 0, "txid");
        var height = GetIntArg(args, 1, "height");

        var cacheKey = $"{txidHex}:{height}";
        if (_merkleCache.TryGet(cacheKey, out var cached))
            return new Dictionary<string, object>
            {
                ["block_height"] = height,
                ["merkle"] = cached.branch.Select(b => Convert.ToHexStringLower(b)).ToArray(),
                ["pos"] = cached.pos
            };

        var blockHash = await WithRpcThrottle(session, () => _rpc.GetBlockHashAsync(height));
        var block = await WithRpcThrottle(session, () => _rpc.GetBlockAsync(blockHash));
        var txArray = block?["tx"]?.AsArray();

        if (txArray is null)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Block not found");

        var txHashes = new List<byte[]>();
        foreach (var tx in txArray)
        {
            var txid = string.Empty;
            if (tx is JsonValue txVal && txVal.TryGetValue<string>(out var txStr))
                txid = txStr;
            var hash = Convert.FromHexString(txid);
            Array.Reverse(hash);
            txHashes.Add(hash);
        }

        var targetHash = Convert.FromHexString(txidHex);
        Array.Reverse(targetHash);

        var (pos, branch) = MerkleTree.GetMerkleProof(txHashes, targetHash);
        if (pos < 0)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Transaction not found in block");

        _merkleCache.Set(cacheKey, (pos, branch));

        return new Dictionary<string, object>
        {
            ["block_height"] = height,
            ["merkle"] = branch.Select(b => Convert.ToHexStringLower(b)).ToArray(),
            ["pos"] = pos
        };
    }

    /// <summary>
    ///     Handles the blockchain.transaction.broadcast method call.
    /// </summary>
    /// <param name="args">The method parameters: [raw_transaction].</param>
    /// <returns>The transaction ID (txid) of the broadcast transaction.</returns>
    /// <exception cref="JsonRpcException">Thrown if raw transaction parameter is missing or broadcast fails.</exception>
    private async Task<object> BroadcastTransaction(object?[] args, IClientSession session)
    {
        var rawTxHex = GetStringArg(args, 0, "raw transaction");

        try
        {
            var txid = await WithRpcThrottle(session, () => _rpc.SendRawTransactionAsync(rawTxHex));
            return txid;
        }
        catch (JsonRpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    /// <summary>
    ///     Handles the blockchain.estimatefee method call.
    /// </summary>
    /// <param name="args">The method parameters: [blocks (optional, default 6)].</param>
    /// <returns>The estimated fee rate in BTC per kilobyte.</returns>
    /// <summary>
    ///     Handles the blockchain.estimatefee method call.
    ///     Protocol 1.6: Supports optional mode parameter (ECONOMICAL or CONSERVATIVE).
    /// </summary>
    private async Task<object> EstimateFee(object?[] args, IClientSession session)
    {
        var blocks = args.Length > 0 ? GetIntArg(args, 0, "blocks") : 6;
        var mode = args.Length > 1 ? args[1]?.ToString() : null;
        var fee = await WithRpcThrottle(session, () => _rpc.EstimateSmartFeeAsync(blocks, mode));
        return fee;
    }

    // ===== blockchain.address.* methods =====

    private byte[] ResolveAddressToScripthash(object?[] args)
    {
        var address = GetStringArg(args, 0, "address");
        try
        {
            return ScriptHashUtil.AddressToScriptHash(address, _network);
        }
        catch (Exception)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Invalid address: {address}");
        }
    }

    private object GetAddressBalance(object?[] args)
    {
        var scripthash = ResolveAddressToScripthash(args);

        try
        {
            var (confirmed, unconfirmed) = _syncer.GetBalance(scripthash);
            return new Dictionary<string, object>
            {
                ["confirmed"] = confirmed,
                ["unconfirmed"] = unconfirmed
            };
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    private object GetAddressHistory(object?[] args)
    {
        var scripthash = ResolveAddressToScripthash(args);

        try
        {
            var historyWithFee = _syncer.GetHistoryWithFee(scripthash);
            return historyWithFee.Select(h =>
            {
                var result = new Dictionary<string, object>
                {
                    ["height"] = h.height,
                    ["tx_hash"] = ToDisplayTxHash(h.txHash)
                };

                if (h is { height: <= 0, fee: > 0 })
                    result["fee"] = h.fee;

                return result;
            }).ToArray();
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    private object GetAddressMempool(object?[] args)
    {
        var scripthash = ResolveAddressToScripthash(args);

        var mempool = _syncer.Mempool;
        if (mempool is null)
            return Array.Empty<object>();

        var mempoolTxs = mempool.GetMempoolHistory(scripthash);
        return mempoolTxs.Select(m =>
        {
            return new Dictionary<string, object>
            {
                ["tx_hash"] = ToDisplayTxHash(m.txHash),
                ["height"] = m.height,
                ["fee"] = m.fee
            };
        }).ToArray();
    }

    private object GetAddressScripthash(object?[] args)
    {
        var address = GetStringArg(args, 0, "address");
        try
        {
            var scripthash = ScriptHashUtil.AddressToScriptHash(address, _network);
            return ScriptHashUtil.ToHex(scripthash);
        }
        catch (Exception)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Invalid address: {address}");
        }
    }

    private object GetAddressUtxos(object?[] args)
    {
        var scripthash = ResolveAddressToScripthash(args);
        var utxos = _syncer.GetUtxos(scripthash);

        return utxos.Select(u =>
        {
            return new Dictionary<string, object>
            {
                ["tx_hash"] = ToDisplayTxHash(u.txHash),
                ["tx_pos"] = u.outputIndex,
                ["height"] = u.height,
                ["value"] = u.value
            };
        }).ToArray();
    }

    private async Task<object?> AddressSubscribe(object?[] args, IClientSession session)
    {
        var scripthash = ResolveAddressToScripthash(args);
        var scripthashHex = ScriptHashUtil.ToHex(scripthash);

        try
        {
            _subscriptions.SubscribeScripthash(session, scripthashHex);
            var status = _syncer.GetStatus(scripthash);
            return status;
        }
        catch (HistoryLimitExceededException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
        catch (SubscriptionLimitException ex)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, ex.Message);
        }
    }

    private bool AddressUnsubscribe(object?[] args, IClientSession session)
    {
        var scripthash = ResolveAddressToScripthash(args);
        var scripthashHex = ScriptHashUtil.ToHex(scripthash);
        return _subscriptions.UnsubscribeScripthash(session, scripthashHex);
    }

    /// <summary>
    ///     Handles the mempool.get_fee_histogram method call.
    /// </summary>
    /// <returns>An array of [fee_rate, vsize] pairs representing the mempool fee distribution.</returns>
    private object GetFeeHistogram()
    {
        var mempool = _syncer.Mempool;
        if (mempool is null)
            return Array.Empty<object[]>();

        var histogram = mempool.GetFeeHistogram();
        return histogram.Select(h => new object[] { h.feeRate, h.vsize }).ToArray();
    }

    /// <summary>
    ///     Extracts and validates a string argument from the method parameters.
    /// </summary>
    /// <param name="args">The method parameters array.</param>
    /// <param name="index">The index of the parameter to extract.</param>
    /// <param name="name">The name of the parameter (for error messages).</param>
    /// <returns>The string value of the parameter.</returns>
    /// <exception cref="JsonRpcException">Thrown if the parameter is missing or cannot be converted to a string.</exception>
    private static string GetStringArg(object?[] args, int index, string name)
    {
        if (args.Length <= index)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Missing {name} parameter");

        var arg = args[index];

        if (arg is string s)
            return s;

        if (arg is not null)
            return arg.ToString() ??
                   throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Invalid {name} parameter");

        throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Missing {name} parameter");
    }

    private static int GetIntArg(object?[] args, int index, string name)
    {
        if (args.Length <= index)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Missing {name} parameter");

        try
        {
            return Convert.ToInt32(args[index]);
        }
        catch (Exception)
        {
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, $"Invalid {name} parameter");
        }
    }

    /// <summary>
    ///     Handles the blockchain.transaction.id_from_pos method call.
    ///     Returns a transaction hash given a block height and position in the block.
    ///     Protocol 1.6: Optionally returns a merkle proof.
    /// </summary>
    private async Task<object> IdFromPos(object?[] args, IClientSession session)
    {
        if (args.Length < 2)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "Missing parameters");

        var height = GetIntArg(args, 0, "height");
        var txPos = GetIntArg(args, 1, "tx_pos");
        var merkle = args.Length > 2 && args[2] is true;

        var blockHash = await WithRpcThrottle(session, () => _rpc.GetBlockHashAsync(height));
        var block = await WithRpcThrottle(session, () => _rpc.GetBlockAsync(blockHash));
        var txArray = block?["tx"]?.AsArray();

        if (txArray is null || txPos >= txArray.Count)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Transaction position out of range");

        var txid = txArray[txPos]?.GetValue<string>() ?? "";

        if (!merkle)
            return txid;

        // Compute merkle proof
        var txHashes = new List<byte[]>();
        foreach (var tx in txArray)
        {
            var txHashHex = tx?.GetValue<string>() ?? "";
            var hash = Convert.FromHexString(txHashHex);
            Array.Reverse(hash);
            txHashes.Add(hash);
        }

        var targetHash = Convert.FromHexString(txid);
        Array.Reverse(targetHash);

        var (pos, branch) = MerkleTree.GetMerkleProof(txHashes, targetHash);

        return new Dictionary<string, object>
        {
            ["tx_hash"] = txid,
            ["merkle"] = branch.Select(b => Convert.ToHexStringLower(b)).ToArray()
        };
    }

    /// <summary>
    ///     Handles the blockchain.transaction.broadcast_package method call.
    ///     Broadcasts a package of transactions via submitpackage RPC.
    ///     Protocol 1.6: Requires Bitcoin Core 28.0+.
    /// </summary>
    private async Task<object> BroadcastPackage(object?[] args, IClientSession session)
    {
        if (args.Length < 1 || args[0] is not object[] rawTxs)
            throw new JsonRpcException(JsonRpcErrorCodes.InvalidParams, "Missing raw_txs parameter");

        var verbose = args.Length > 1 && args[1] is true;
        var txHexArray = rawTxs.Select(tx => tx?.ToString() ?? "").ToArray();

        var result = await WithRpcThrottle(session, () => _rpc.SubmitPackageAsync(txHexArray));

        if (verbose)
            return result is not null ? result : new Dictionary<string, object>();

        // Parse result for non-verbose response
        var success = result?["package_msg"]?.GetValue<string>() == "success";
        var response = new Dictionary<string, object> { ["success"] = success };

        if (!success && result?["tx-results"] is JsonObject txResults)
        {
            var errors = new List<Dictionary<string, string>>();
            foreach (var kv in txResults)
                if (kv.Value is JsonObject txResult && txResult["error"] is JsonValue errorVal &&
                    errorVal.TryGetValue<string>(out var errorMsg))
                {
                    var txid = kv.Key;
                    errors.Add(new Dictionary<string, string> { ["txid"] = txid, ["error"] = errorMsg });
                }

            if (errors.Count > 0)
                response["errors"] = errors;
        }

        return response;
    }

    /// <summary>
    ///     Handles the mempool.get_info method call.
    ///     Returns mempool fee information.
    ///     Protocol 1.6: Replacement for blockchain.relayfee.
    /// </summary>
    private async Task<object> GetMempoolInfo(IClientSession session)
    {
        var info = await WithRpcThrottle(session, () => _rpc.GetMempoolInfoAsync());
        if (!info.HasValue)
            throw new JsonRpcException(JsonRpcErrorCodes.ServerError, "Failed to get mempool info");

        return new Dictionary<string, object>
        {
            ["mempoolminfee"] = info.Value.mempoolminfee,
            ["minrelaytxfee"] = info.Value.minrelaytxfee,
            ["incrementalrelayfee"] = info.Value.incrementalrelayfee
        };
    }

    private sealed record HeaderCheckpointCacheState(
        int CpHeight,
        List<byte[]> Leaves,
        List<List<byte[]>> Levels,
        byte[] TipHash)
    {
        public byte[] Root => Levels[^1][0];

        public static HeaderCheckpointCacheState Empty { get; } =
            new(-1, new List<byte[]>(), new List<List<byte[]>>(), Array.Empty<byte>());
    }
}

/// <summary>
///     Represents a JSON-RPC protocol error with an error code.
/// </summary>
public class JsonRpcException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the JsonRpcException class.
    /// </summary>
    /// <param name="code">The JSON-RPC error code.</param>
    /// <param name="message">The error message.</param>
    public JsonRpcException(int code, string message) : base(message)
    {
        Code = code;
    }

    /// <summary>
    ///     Gets the JSON-RPC error code.
    /// </summary>
    public int Code { get; }
}