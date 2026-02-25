using System.Globalization;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Electre.Metrics;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Electre.Bitcoin;

/// <summary>
///     Bitcoin Core JSON-RPC client with Polly-based resilience and retry policies.
/// </summary>
public sealed class RpcClient
{
    /// <summary>
    ///     HTTP client for communicating with Bitcoin Core.
    /// </summary>
    private readonly HttpClient _httpClient;

    /// <summary>
    ///     Logger instance for RPC operations.
    /// </summary>
    private readonly ILogger _logger;

    /// <summary>
    ///     Polly resilience pipeline for retry handling.
    /// </summary>
    private readonly ResiliencePipeline _resiliencePipeline;

    /// <summary>
    ///     The Bitcoin Core RPC endpoint URL.
    /// </summary>
    private readonly string _url;

    /// <summary>
    ///     Counter for JSON-RPC request IDs.
    /// </summary>
    private int _requestId;

    /// <summary>
    ///     Initializes a new instance of the RpcClient class.
    /// </summary>
    /// <param name="url">The Bitcoin Core RPC endpoint URL (e.g., "http://127.0.0.1:8332").</param>
    /// <param name="user">The RPC username for basic authentication.</param>
    /// <param name="password">The RPC password for basic authentication.</param>
    /// <param name="loggerFactory">The logger factory for creating loggers.</param>
    /// <param name="maxRetries">Maximum number of retry attempts (default 3).</param>
    /// <param name="retryDelayMs">Initial retry delay in milliseconds (default 1000).</param>
    /// <param name="timeoutSeconds">HTTP request timeout in seconds (default 300).</param>
    public RpcClient(
        string url,
        string user,
        string password,
        ILoggerFactory loggerFactory,
        int maxRetries = 3,
        int retryDelayMs = 1000,
        int timeoutSeconds = 300,
        HttpMessageHandler? httpMessageHandler = null)
    {
        _logger = loggerFactory.CreateLogger("RPC");
        _url = url;
        _httpClient = httpMessageHandler is null
            ? new HttpClient()
            : new HttpClient(httpMessageHandler);
        _httpClient.Timeout = TimeSpan.FromSeconds(timeoutSeconds);

        if (!string.IsNullOrWhiteSpace(user))
        {
            var auth = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{user}:{password}"));
            _httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Basic", auth);
        }

        _resiliencePipeline = new ResiliencePipelineBuilder()
            .AddRetry(new RetryStrategyOptions
            {
                MaxRetryAttempts = maxRetries,
                BackoffType = DelayBackoffType.Exponential,
                Delay = TimeSpan.FromMilliseconds(retryDelayMs),
                ShouldHandle = new PredicateBuilder().Handle<HttpRequestException>().Handle<TaskCanceledException>(),
                OnRetry = args =>
                {
                    _logger.LogWarning("RPC retry {Attempt}/{MaxAttempts} after {Delay}ms: {Exception}",
                        args.AttemptNumber + 1, maxRetries, args.RetryDelay.TotalMilliseconds,
                        args.Outcome.Exception?.Message ?? "unknown");
                    return ValueTask.CompletedTask;
                }
            })
            .Build();
    }

    /// <summary>
    ///     Executes a single JSON-RPC call to Bitcoin Core with retry resilience.
    /// </summary>
    /// <param name="method">The RPC method name (e.g., "getblockcount", "getrawtransaction").</param>
    /// <param name="parameters">The method parameters.</param>
    /// <returns>The result node from the RPC response, or null if no result.</returns>
    /// <exception cref="RpcException">Thrown if the RPC call returns an error.</exception>
    private async Task<JsonNode?> CallAsync(string method, params object[] parameters)
    {
        AppMetrics.RecordRpcCall(method);
        using var timer = AppMetrics.MeasureRpcCall(method);

        return await _resiliencePipeline.ExecuteAsync(async ct =>
        {
            var request = new
            {
                jsonrpc = "1.0",
                id = Interlocked.Increment(ref _requestId),
                method,
                @params = parameters
            };

            var json = JsonSerializer.Serialize(request);
            var result = await SendRpcRequestAsync(json, ct);
            var errorNode = result["error"];
            if (errorNode is JsonObject errorObj) throw CreateRpcException(errorObj);

            return result["result"];
        });
    }

    /// <summary>
    ///     Executes multiple JSON-RPC calls in a single batch request with retry resilience.
    /// </summary>
    /// <param name="calls">An array of tuples containing method names and their parameters.</param>
    /// <returns>An array of result nodes corresponding to each call, or null for failed calls.</returns>
    public async Task<JsonNode?[]> BatchCallAsync(params (string method, object[] parameters)[] calls)
    {
        if (calls.Length == 0)
            return [];

        if (calls.Length == 1)
        {
            var result = await CallAsync(calls[0].method, calls[0].parameters);
            return [result];
        }

        return await _resiliencePipeline.ExecuteAsync(async ct =>
        {
            var requestIds = new int[calls.Length];
            var requests = new object[calls.Length];
            for (var i = 0; i < calls.Length; i++)
            {
                var requestId = Interlocked.Increment(ref _requestId);
                requestIds[i] = requestId;
                requests[i] = new
                {
                    jsonrpc = "1.0",
                    id = requestId,
                    calls[i].method,
                    @params = calls[i].parameters
                };
            }

            var requestIndexById = new Dictionary<int, int>(calls.Length);
            for (var i = 0; i < requestIds.Length; i++)
                requestIndexById[requestIds[i]] = i;

            var json = JsonSerializer.Serialize(requests);
            var content = new StringContent(json, Encoding.UTF8, "application/json");

            var rootNode = await SendRpcRequestAsync(json, ct);
            if (rootNode is not JsonArray results)
                throw new RpcException("Bitcoin RPC batch response is not a JSON array");

            var ordered = new JsonNode?[calls.Length];
            foreach (var item in results)
            {
                if (item is null)
                    continue;
                if (item is not JsonObject)
                    continue;
                var idNode = item["id"];
                var id = idNode is JsonValue idVal && idVal.TryGetValue<int>(out var idInt) ? idInt : -1;
                if (requestIndexById.TryGetValue(id, out var idx))
                {
                    var errNode = item["error"];
                    if (errNode is JsonObject errObj)
                    {
                        var method = calls[idx].method;
                        var error = FormatRpcError(errNode);
                        if (ShouldLogBatchErrorAsDebug(method, errObj))
                            _logger.LogTrace("Batch RPC call failed for method {Method}: {Error}", method, error);
                        else
                            _logger.LogWarning("Batch RPC call failed for method {Method}: {Error}", method, error);
                        ordered[idx] = null;
                    }
                    else
                    {
                        ordered[idx] = item["result"];
                    }
                }
            }

            return ordered;
        });
    }

    private static bool ShouldLogBatchErrorAsDebug(string method, JsonObject errorObj)
    {
        if (!TryGetRpcErrorCode(errorObj, out var code))
            return false;

        // During mempool polling, txs can disappear between batched calls.
        // This is expected and should not flood warning logs.
        return code == -5 &&
               (string.Equals(method, "getmempoolentry", StringComparison.Ordinal) ||
                string.Equals(method, "getrawtransaction", StringComparison.Ordinal));
    }

    private static bool TryGetRpcErrorCode(JsonObject errorObj, out int code)
    {
        code = 0;
        if (errorObj["code"] is not JsonValue codeValue)
            return false;

        if (codeValue.TryGetValue(out code))
            return true;

        if (codeValue.TryGetValue<long>(out var codeLong) &&
            codeLong is >= int.MinValue and <= int.MaxValue)
        {
            code = (int)codeLong;
            return true;
        }

        if (codeValue.TryGetValue<decimal>(out var codeDecimal) &&
            decimal.Truncate(codeDecimal) == codeDecimal &&
            codeDecimal is >= int.MinValue and <= int.MaxValue)
        {
            code = (int)codeDecimal;
            return true;
        }

        if (codeValue.TryGetValue<double>(out var codeDouble) &&
            Math.Truncate(codeDouble) == codeDouble &&
            codeDouble is >= int.MinValue and <= int.MaxValue)
        {
            code = (int)codeDouble;
            return true;
        }

        if (codeValue.TryGetValue<string>(out var codeString))
            return int.TryParse(codeString, NumberStyles.Integer, CultureInfo.InvariantCulture, out code);

        return false;
    }

    private async Task<JsonNode> SendRpcRequestAsync(string jsonPayload, CancellationToken ct)
    {
        using var content = new StringContent(jsonPayload, Encoding.UTF8, "application/json");
        using var response = await _httpClient.PostAsync(_url, content, ct);
        var responseJson = await response.Content.ReadAsStringAsync(ct);

        if (!response.IsSuccessStatusCode)
        {
            var statusCode = (int)response.StatusCode;
            var bodyExcerpt = responseJson.Length > 256 ? responseJson[..256] : responseJson;
            var message = $"Bitcoin RPC HTTP {statusCode} ({response.ReasonPhrase}): {bodyExcerpt}";

            if (statusCode >= 500)
                throw new HttpRequestException(message);

            throw new RpcException(message, httpStatusCode: statusCode);
        }

        try
        {
            var node = JsonNode.Parse(responseJson);
            if (node is null)
                throw new RpcException("Bitcoin RPC returned empty JSON payload");
            return node;
        }
        catch (JsonException ex)
        {
            throw new RpcException("Bitcoin RPC returned invalid JSON", innerException: ex);
        }
    }

    private static RpcException CreateRpcException(JsonObject errorObj)
    {
        var message = FormatRpcError(errorObj);
        int? code = null;
        if (errorObj["code"] is JsonValue codeValue && codeValue.TryGetValue<int>(out var parsedCode))
            code = parsedCode;
        return new RpcException(message, code);
    }

    private static string FormatRpcError(JsonNode? errorNode)
    {
        if (errorNode is not JsonObject errorObj)
            return "Bitcoin RPC error";

        var message = "Bitcoin RPC error";
        if (errorObj["message"] is JsonValue msgValue && msgValue.TryGetValue<string>(out var parsedMessage))
            message = parsedMessage;

        if (errorObj["code"] is JsonValue codeValue && codeValue.TryGetValue<int>(out var code))
            return $"Bitcoin RPC error {code}: {message}";

        return $"Bitcoin RPC error: {message}";
    }

    /// <summary>
    ///     Gets the current block count (height) of the blockchain.
    /// </summary>
    /// <returns>The current block height, or 0 if the call fails.</returns>
    public async Task<int> GetBlockCountAsync()
    {
        var result = await CallAsync("getblockcount");
        if (result is JsonValue jv && jv.TryGetValue<int>(out var count))
            return count;
        return 0;
    }

    /// <summary>
    ///     Gets the block hash at the specified height.
    /// </summary>
    /// <param name="height">The block height.</param>
    /// <returns>The block hash as a hex string, or empty string if not found.</returns>
    public async Task<string> GetBlockHashAsync(int height)
    {
        var result = await CallAsync("getblockhash", height);
        if (result is JsonValue jv && jv.TryGetValue<string>(out var hash))
            return hash;
        return string.Empty;
    }

    /// <summary>
    ///     Gets block hashes for multiple heights in a single batch call.
    /// </summary>
    /// <param name="heights">An array of block heights.</param>
    /// <returns>An array of block hashes corresponding to each height.</returns>
    public async Task<string[]> GetBlockHashesAsync(int[] heights)
    {
        var calls = heights.Select(h => ("getblockhash", new object[] { h })).ToArray();
        var results = await BatchCallAsync(calls);
        return results.Select(r =>
        {
            if (r is JsonValue jv && jv.TryGetValue<string>(out var s))
                return s;
            return string.Empty;
        }).ToArray();
    }

    /// <summary>
    ///     Gets block data by hash.
    /// </summary>
    /// <param name="hash">The block hash.</param>
    /// <param name="verbosity">Verbosity level (0=raw hex, 1=object, 2=object with tx details).</param>
    /// <returns>The block data as a JSON node.</returns>
    public async Task<JsonNode?> GetBlockAsync(string hash, int verbosity = 1)
    {
        return await CallAsync("getblock", hash, verbosity);
    }

    /// <summary>
    ///     Gets block data for multiple hashes in a single batch call.
    /// </summary>
    /// <param name="hashes">An array of block hashes.</param>
    /// <param name="verbosity">Verbosity level (0=raw hex, 1=object, 2=object with tx details).</param>
    /// <returns>An array of block data JSON nodes.</returns>
    public async Task<JsonNode?[]> GetBlocksAsync(string[] hashes, int verbosity = 2)
    {
        var calls = hashes.Select(h => ("getblock", new object[] { h, verbosity })).ToArray();
        return await BatchCallAsync(calls);
    }

    /// <summary>
    ///     Gets the raw block header bytes for a block hash.
    /// </summary>
    /// <param name="hash">The block hash.</param>
    /// <returns>The raw block header as bytes, or empty array if not found.</returns>
    public async Task<byte[]> GetBlockHeaderRawAsync(string hash)
    {
        var result = await CallAsync("getblockheader", hash, false);
        if (result is JsonValue jv && jv.TryGetValue<string>(out var hex) && !string.IsNullOrWhiteSpace(hex))
            return Convert.FromHexString(hex);
        return [];
    }

    /// <summary>
    ///     Gets raw block headers for multiple hashes in a single batch call.
    /// </summary>
    /// <param name="hashes">An array of block hashes.</param>
    /// <returns>An array of raw block header byte arrays.</returns>
    public async Task<byte[][]> GetBlockHeadersRawAsync(string[] hashes)
    {
        var calls = hashes.Select(h => ("getblockheader", new object[] { h, false })).ToArray();
        var results = await BatchCallAsync(calls);
        return results.Select(r =>
        {
            if (r is JsonValue jv && jv.TryGetValue<string>(out var hex) && !string.IsNullOrWhiteSpace(hex))
                return Convert.FromHexString(hex);
            return [];
        }).ToArray();
    }

    /// <summary>
    ///     Gets the raw transaction hex for a transaction ID.
    /// </summary>
    /// <param name="txid">The transaction ID.</param>
    /// <param name="blockHash">Optional block hash for faster lookup (requires txindex or blockhash in wallet).</param>
    /// <returns>The raw transaction as a hex string, or empty string if not found.</returns>
    public async Task<string> GetRawTransactionHexAsync(string txid, string? blockHash = null)
    {
        JsonNode? result;
        if (string.IsNullOrWhiteSpace(blockHash))
            result = await CallAsync("getrawtransaction", txid, false);
        else
            result = await CallAsync("getrawtransaction", txid, false, blockHash);

        if (result is JsonValue jv && jv.TryGetValue<string>(out var hex))
            return hex;
        return string.Empty;
    }

    /// <summary>
    ///     Gets the raw transaction bytes for a transaction ID.
    /// </summary>
    /// <param name="txid">The transaction ID.</param>
    /// <returns>The raw transaction as bytes.</returns>
    public async Task<byte[]> GetRawTransactionAsync(string txid)
    {
        var hex = await GetRawTransactionHexAsync(txid);
        return Convert.FromHexString(hex);
    }

    /// <summary>
    ///     Gets verbose transaction data (decoded) for a transaction ID.
    /// </summary>
    /// <param name="txid">The transaction ID.</param>
    /// <param name="blockHash">Optional block hash for faster lookup.</param>
    /// <returns>The transaction data as a JSON node with decoded fields.</returns>
    public async Task<JsonNode?> GetRawTransactionVerboseAsync(string txid, string? blockHash = null)
    {
        if (string.IsNullOrWhiteSpace(blockHash))
            return await CallAsync("getrawtransaction", txid, true);

        return await CallAsync("getrawtransaction", txid, true, blockHash);
    }

    /// <summary>
    ///     Gets verbose transaction data for multiple transaction IDs in a single batch call.
    /// </summary>
    /// <param name="txids">An array of transaction IDs.</param>
    /// <returns>An array of transaction data JSON nodes.</returns>
    public async Task<JsonNode?[]> GetRawTransactionsVerboseAsync(string[] txids)
    {
        if (txids.Length == 0)
            return [];

        var calls = txids.Select(txid => ("getrawtransaction", new object[] { txid, true })).ToArray();
        return await BatchCallAsync(calls);
    }

    /// <summary>
    ///     Broadcasts a raw transaction to the network.
    /// </summary>
    /// <param name="hex">The raw transaction hex.</param>
    /// <returns>The transaction ID (txid) of the broadcast transaction, or empty string if broadcast fails.</returns>
    public async Task<string> SendRawTransactionAsync(string hex)
    {
        var result = await CallAsync("sendrawtransaction", hex);
        if (result is JsonValue jv && jv.TryGetValue<string>(out var txid))
            return txid;
        return string.Empty;
    }

    /// <summary>
    ///     Estimates the smart fee rate for a transaction to confirm within a target number of blocks.
    ///     Protocol 1.6: Supports optional mode parameter (ECONOMICAL or CONSERVATIVE).
    /// </summary>
    /// <param name="confTarget">The target number of blocks for confirmation.</param>
    /// <param name="mode">Optional estimate mode (ECONOMICAL or CONSERVATIVE).</param>
    /// <returns>The estimated fee rate in BTC per kilobyte, or -1 if estimation fails.</returns>
    public async Task<double> EstimateSmartFeeAsync(int confTarget, string? mode = null)
    {
        JsonNode? result;
        if (mode is not null)
            result = await CallAsync("estimatesmartfee", confTarget, mode);
        else
            result = await CallAsync("estimatesmartfee", confTarget);
        var feerateNode = result?["feerate"];
        if (feerateNode is JsonValue jv && jv.TryGetValue<double>(out var feerate))
            return feerate;
        return -1;
    }

    /// <summary>
    ///     Gets mempool entry data for a transaction ID.
    /// </summary>
    /// <param name="txid">The transaction ID.</param>
    /// <returns>The mempool entry data as a JSON node, or null if the transaction is not in the mempool.</returns>
    public async Task<JsonNode?> GetMempoolEntryAsync(string txid)
    {
        try
        {
            return await CallAsync("getmempoolentry", txid);
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "getmempoolentry failed for {Txid}", txid);
            return null;
        }
    }

    /// <summary>
    ///     Gets mempool entry data and raw transaction hex for multiple transaction IDs in a single batch call.
    /// </summary>
    /// <param name="txids">An array of transaction IDs.</param>
    /// <returns>An array of tuples containing mempool entry data and raw transaction hex for each txid.</returns>
    public async Task<(JsonNode? entry, string rawHex)[]> GetMempoolTxBatchAsync(string[] txids)
    {
        if (txids.Length == 0)
            return [];

        var calls = new List<(string method, object[] parameters)>();
        foreach (var txid in txids)
        {
            calls.Add(("getmempoolentry", [txid]));
            calls.Add(("getrawtransaction", [txid, false]));
        }

        var results = await BatchCallAsync(calls.ToArray());
        var output = new (JsonNode? entry, string rawHex)[txids.Length];

        for (var i = 0; i < txids.Length; i++)
        {
            var entry = results[i * 2];
            var rawNode = results[i * 2 + 1];
            var rawHex = string.Empty;

            if (rawNode is JsonValue jv && jv.TryGetValue<string>(out var s))
                rawHex = s;

            output[i] = (entry, rawHex);
        }

        return output;
    }

    /// <summary>
    ///     Gets all transaction IDs currently in the mempool.
    /// </summary>
    /// <returns>An array of transaction IDs in the mempool.</returns>
    public async Task<string[]> GetRawMempoolAsync()
    {
        var result = await CallAsync("getrawmempool");
        if (result is JsonArray array)
        {
            var txids = new List<string>();
            foreach (var item in array)
                if (item is JsonValue jv && jv.TryGetValue<string>(out var txid))
                    txids.Add(txid);
            return txids.ToArray();
        }

        return [];
    }

    /// <summary>
    ///     Gets blockchain information including chain name, block count, and sync status.
    /// </summary>
    /// <returns>Blockchain information as a JSON node.</returns>
    public async Task<JsonNode?> GetBlockchainInfoAsync()
    {
        return await CallAsync("getblockchaininfo");
    }

    /// <summary>
    ///     Gets the raw block data (serialized bytes) for a block hash.
    /// </summary>
    /// <param name="hash">The block hash.</param>
    /// <returns>The raw block data as bytes, or empty array if not found.</returns>
    public async Task<byte[]> GetBlockRawAsync(string hash)
    {
        var result = await CallAsync("getblock", hash, 0);
        if (result is JsonValue jv && jv.TryGetValue<string>(out var hex) && !string.IsNullOrWhiteSpace(hex))
            return Convert.FromHexString(hex);
        return [];
    }

    /// <summary>
    ///     Waits for a new block to be mined, with optional timeout and cancellation support.
    /// </summary>
    /// <param name="currentTip">
    ///     Optional current block hash to detect changes (if provided, returns immediately if tip has
    ///     changed).
    /// </param>
    /// <param name="timeoutMs">Timeout in milliseconds (default 60000).</param>
    /// <param name="ct">Cancellation token for early termination.</param>
    /// <returns>A tuple containing the new block hash and height, or null if timeout or error occurs.</returns>
    public async Task<(string hash, int height)?> WaitForNewBlockAsync(string? currentTip = null, int timeoutMs = 60000,
        CancellationToken ct = default)
    {
        AppMetrics.RecordRpcCall("waitfornewblock");
        using var timer = AppMetrics.MeasureRpcCall("waitfornewblock");

        var effectiveTimeoutMs = Math.Max(0, timeoutMs);
        var deadline = DateTime.UtcNow.AddMilliseconds(effectiveTimeoutMs);

        try
        {
            while (true)
            {
                var remainingMs = (int)Math.Max(0, (deadline - DateTime.UtcNow).TotalMilliseconds);
                if (remainingMs <= 0)
                    return null;

                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(remainingMs);

                var request = new
                {
                    jsonrpc = "1.0",
                    id = Interlocked.Increment(ref _requestId),
                    method = "waitfornewblock",
                    @params = new object[] { remainingMs }
                };

                var json = JsonSerializer.Serialize(request);
                var root = await SendRpcRequestAsync(json, cts.Token);
                var errorNode = root["error"];
                if (errorNode is JsonObject errorObj)
                    throw CreateRpcException(errorObj);

                if (!TryParseWaitForNewBlockResult(root["result"], out var parsed))
                    return null;

                if (string.IsNullOrWhiteSpace(currentTip) || !parsed.HasValue)
                    return parsed;

                if (!string.Equals(parsed.Value.hash, currentTip, StringComparison.OrdinalIgnoreCase))
                    return parsed;
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogTrace("waitfornewblock timeout or cancellation after {TimeoutMs}ms", timeoutMs);
            return null;
        }
        catch (RpcException ex)
        {
            _logger.LogWarning("waitfornewblock RPC error: {Error}", ex.Message);
            return null;
        }
        catch (HttpRequestException ex)
        {
            _logger.LogWarning("waitfornewblock HTTP error: {Error}", ex.Message);
            return null;
        }
        catch (Exception ex)
        {
            _logger.LogWarning("waitfornewblock unexpected error: {Error}", ex.Message);
            return null;
        }
    }

    private static bool TryParseWaitForNewBlockResult(JsonNode? resultNode, out (string hash, int height)? result)
    {
        result = null;
        if (resultNode is not JsonObject resultObj)
            return false;

        var hashNode = resultObj["hash"];
        var heightNode = resultObj["height"];
        if (hashNode is JsonValue hashVal &&
            hashVal.TryGetValue<string>(out var hash) &&
            heightNode is JsonValue heightVal &&
            heightVal.TryGetValue<int>(out var height))
        {
            result = (hash, height);
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Submits a package of raw transactions to the mempool via submitpackage RPC.
    ///     Protocol 1.6: Requires Bitcoin Core 28.0+ for functional package relay.
    /// </summary>
    /// <param name="rawTxs">Array of raw transaction hex strings.</param>
    /// <returns>The JSON result from submitpackage RPC.</returns>
    public async Task<JsonNode?> SubmitPackageAsync(string[] rawTxs)
    {
        try
        {
            return await CallAsync("submitpackage", rawTxs);
        }
        catch (Exception ex) when (ex.Message.Contains("submitpackage") || ex.Message.Contains("not found"))
        {
            throw new RpcException("submitpackage RPC not available. Requires Bitcoin Core 28.0+");
        }
    }

    /// <summary>
    ///     Gets mempool information including fee rates.
    ///     Protocol 1.6: Used by mempool.get_info method.
    /// </summary>
    /// <returns>Mempool info containing mempoolminfee, minrelaytxfee, incrementalrelayfee.</returns>
    public async Task<(double mempoolminfee, double minrelaytxfee, double incrementalrelayfee)?> GetMempoolInfoAsync()
    {
        var result = await CallAsync("getmempoolinfo");
        if (result is null)
            return null;

        var mempoolminfeeNode = result["mempoolminfee"];
        var minrelaytxfeeNode = result["minrelaytxfee"];
        var incrementalrelayfeeNode = result["incrementalrelayfee"];

        if (mempoolminfeeNode is not JsonValue mv1 || !mv1.TryGetValue<double>(out var mempoolminfee))
            return null;
        if (minrelaytxfeeNode is not JsonValue mv2 || !mv2.TryGetValue<double>(out var minrelaytxfee))
            return null;
        if (incrementalrelayfeeNode is not JsonValue mv3 || !mv3.TryGetValue<double>(out var incrementalrelayfee))
            return null;

        return (mempoolminfee, minrelaytxfee, incrementalrelayfee);
    }
}

/// <summary>
///     Represents an error returned by the Bitcoin Core RPC server.
/// </summary>
public sealed class RpcException : Exception
{
    /// <summary>
    ///     Initializes a new instance of the RpcException class.
    /// </summary>
    /// <param name="message">The error message from the RPC server.</param>
    public RpcException(string message, int? rpcCode = null, int? httpStatusCode = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        RpcCode = rpcCode;
        HttpStatusCode = httpStatusCode;
    }

    /// <summary>
    ///     Optional JSON-RPC error code.
    /// </summary>
    public int? RpcCode { get; }

    /// <summary>
    ///     Optional HTTP status code for transport-layer failures.
    /// </summary>
    public int? HttpStatusCode { get; }
}