using System.Collections.Concurrent;
using System.Threading.Channels;
using Electre.Bitcoin;
using Microsoft.Extensions.Logging;
using NBitcoin;

namespace Electre.Indexer;

/// <summary>
///     Downloads blocks in parallel from Bitcoin Core using multiple RPC connections.
///     Maintains ordering and queues blocks for sequential processing.
/// </summary>
public sealed class BlockDownloader : IAsyncDisposable
{
    private readonly Channel<BlockData> _channel;
    private readonly int _concurrency;
    private readonly Task[] _downloadTasks;
    private readonly object _lock = new();
    private readonly ILogger _logger;
    private readonly int _maxQueueSize;
    private readonly int _maxRetriesPerHeight;
    private readonly Network _network;
    private readonly SemaphoreSlim _queueSemaphore;
    private readonly ConcurrentDictionary<int, BlockData> _results = new();
    private readonly ConcurrentDictionary<int, int> _retryCountByHeight = new();

    private readonly RpcClient[] _rpcClients;
    private CancellationTokenSource? _cts;
    private Exception? _fatalError;
    private bool _isRunning;

    private int _targetHeight;

    /// <summary>
    ///     Initializes a new instance of the BlockDownloader class.
    /// </summary>
    /// <param name="rpcUrl">Bitcoin Core RPC URL.</param>
    /// <param name="rpcUser">RPC username.</param>
    /// <param name="rpcPassword">RPC password.</param>
    /// <param name="network">Bitcoin network.</param>
    /// <param name="loggerFactory">Logger factory.</param>
    /// <param name="maxRetries">Maximum RPC retry attempts.</param>
    /// <param name="retryDelayMs">Delay between retries in milliseconds.</param>
    /// <param name="timeoutSeconds">RPC timeout in seconds.</param>
    /// <param name="concurrency">Number of parallel download workers.</param>
    /// <param name="maxQueueSize">Maximum queue size for downloaded blocks.</param>
    public BlockDownloader(
        string rpcUrl,
        string rpcUser,
        string rpcPassword,
        Network network,
        ILoggerFactory loggerFactory,
        int maxRetries = 3,
        int retryDelayMs = 1000,
        int timeoutSeconds = 300,
        int concurrency = 4,
        int maxQueueSize = 100)
    {
        _logger = loggerFactory.CreateLogger("Downloader");
        _network = network;
        _concurrency = concurrency;
        _maxQueueSize = maxQueueSize;
        _maxRetriesPerHeight = Math.Max(1, maxRetries);
        _queueSemaphore = new SemaphoreSlim(maxQueueSize, maxQueueSize);
        _downloadTasks = new Task[concurrency];

        // Unbounded channel since we control the size via _maxQueueSize semaphore anyway
        _channel = Channel.CreateUnbounded<BlockData>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false
        });

        _rpcClients = new RpcClient[concurrency];
        for (var i = 0; i < concurrency; i++)
            _rpcClients[i] = new RpcClient(rpcUrl, rpcUser, rpcPassword, loggerFactory, maxRetries, retryDelayMs,
                timeoutSeconds);
    }

    /// <summary>
    ///     Gets the number of blocks currently queued for processing.
    /// </summary>
    public int QueuedCount => _results.Count;

    /// <summary>
    ///     Gets the next block height to process.
    /// </summary>
    public int NextToProcess { get; private set; }

    /// <summary>
    ///     Gets the next block height to download.
    /// </summary>
    public int NextToDownload { get; private set; }

    /// <summary>
    ///     Gets a value indicating whether all blocks have been downloaded and processed.
    /// </summary>
    public bool IsComplete => NextToProcess > _targetHeight;

    /// <summary>
    ///     Disposes the downloader and releases resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        Stop();

        try
        {
            await Task.WhenAll(_downloadTasks.Where(t => t is not null));
        }
        catch (Exception ex)
        {
            _logger.LogTrace(ex, "Error during download worker cleanup");
        }

        _cts?.Dispose();
        _queueSemaphore.Dispose();
    }

    /// <summary>
    ///     Starts the block download process for a height range.
    /// </summary>
    /// <param name="fromHeight">Starting block height (inclusive).</param>
    /// <param name="toHeight">Ending block height (inclusive).</param>
    public void Start(int fromHeight, int toHeight)
    {
        if (_isRunning)
            return;

        // Handle zero-block edge case: if range is empty, complete channel immediately
        if (fromHeight > toHeight)
        {
            _channel.Writer.TryComplete();
            return;
        }

        NextToProcess = fromHeight;
        NextToDownload = fromHeight;
        _targetHeight = toHeight;
        _retryCountByHeight.Clear();
        _fatalError = null;
        _isRunning = true;
        _cts = new CancellationTokenSource();

        _logger.LogInformation("Starting {Concurrency} download workers, queue size {QueueSize}", _concurrency,
            _maxQueueSize);

        for (var i = 0; i < _concurrency; i++)
        {
            var taskIndex = i;
            _downloadTasks[i] = Task.Run(() => DownloadWorkerAsync(taskIndex, _cts.Token));
        }
    }

    /// <summary>
    ///     Stops the block download process.
    /// </summary>
    public void Stop()
    {
        if (!_isRunning)
            return;
        _isRunning = false;
        _cts?.Cancel();
        _channel.Writer.TryComplete();
    }

    /// <summary>
    ///     Worker task that downloads blocks in parallel.
    /// </summary>
    /// <param name="workerIndex">Index of this worker.</param>
    /// <param name="ct">Cancellation token.</param>
    private async Task DownloadWorkerAsync(int workerIndex, CancellationToken ct)
    {
        var rpc = _rpcClients[workerIndex];

        while (!ct.IsCancellationRequested && _isRunning)
        {
            int heightToDownload;

            lock (_lock)
            {
                if (NextToDownload > _targetHeight)
                    break;

                heightToDownload = NextToDownload++;
            }

            try
            {
                await _queueSemaphore.WaitAsync(ct);
            }
            catch (OperationCanceledException)
            {
                break;
            }

            try
            {
                var block = await DownloadAndParseBlockAsync(rpc, heightToDownload);
                if (block is null)
                    throw new InvalidOperationException($"Downloaded empty block payload at height {heightToDownload}");

                _results[heightToDownload] = block;
                _retryCountByHeight.TryRemove(heightToDownload, out _);
                TryPushOrderedResults();
            }
            catch (Exception ex)
            {
                _queueSemaphore.Release();

                var attempt = _retryCountByHeight.AddOrUpdate(heightToDownload, 1, (_, current) => current + 1);
                if (attempt > _maxRetriesPerHeight)
                {
                    var fatal = new InvalidOperationException(
                        $"Block {heightToDownload} failed after {attempt} attempts",
                        ex);
                    _logger.LogError(fatal, "[Worker {WorkerIndex}] Giving up downloading block {Height}", workerIndex,
                        heightToDownload);
                    FailDownloader(fatal);
                    break;
                }

                _logger.LogWarning(
                    ex,
                    "[Worker {WorkerIndex}] Error downloading block {Height} (attempt {Attempt}/{MaxAttempts})",
                    workerIndex,
                    heightToDownload,
                    attempt,
                    _maxRetriesPerHeight);

                lock (_lock)
                {
                    if (heightToDownload < NextToDownload)
                        NextToDownload = heightToDownload;
                }

                await Task.Delay(1000, ct);
            }
        }
    }

    /// <summary>
    ///     Pushes downloaded blocks to the output channel in order.
    /// </summary>
    private void TryPushOrderedResults()
    {
        // Must be called under lock or ensure only one sequencer runs
        // Using lock for simplicity in sequencing
        lock (_lock)
        {
            while (_results.TryRemove(NextToProcess, out var block))
            {
                if (!_channel.Writer.TryWrite(block))
                {
                    // Re-insert if channel is full/closed (unlikely for Unbounded)
                    _results[block.Height] = block;
                    break;
                }

                NextToProcess++;
            }

            // Complete channel when all blocks have been pushed in order
            if (NextToProcess > _targetHeight && _fatalError is null)
                _channel.Writer.TryComplete();
        }
    }

    /// <summary>
    ///     Downloads and parses a single block.
    /// </summary>
    /// <param name="rpc">RPC client to use.</param>
    /// <param name="height">Block height to download.</param>
    /// <returns>Parsed block data, or null if download fails.</returns>
    private async Task<BlockData?> DownloadAndParseBlockAsync(RpcClient rpc, int height)
    {
        var hash = await rpc.GetBlockHashAsync(height);
        if (string.IsNullOrWhiteSpace(hash))
            return null;

        var rawBlock = await rpc.GetBlockRawAsync(hash);
        if (rawBlock.Length == 0)
            return null;

        var blockData = BlockParser.Parse(rawBlock, height, _network);

        return blockData;
    }

    /// <summary>
    ///     Waits for the next downloaded block in order.
    /// </summary>
    /// <param name="ct">Cancellation token.</param>
    /// <returns>Next block data, or null if download is complete or cancelled.</returns>
    public async Task<BlockData?> WaitForNextBlockAsync(CancellationToken ct)
    {
        try
        {
            if (await _channel.Reader.WaitToReadAsync(ct))
                if (_channel.Reader.TryRead(out var block))
                {
                    _queueSemaphore.Release();
                    return block;
                }
        }
        catch (OperationCanceledException)
        {
            _logger.LogTrace("Block download wait cancelled");
        }
        catch (ChannelClosedException) when (_fatalError is null)
        {
            _logger.LogTrace("Block download channel closed");
        }

        if (_fatalError is not null)
            throw new InvalidOperationException("Block downloader stopped due to unrecoverable download errors",
                _fatalError);

        return null;
    }

    private void FailDownloader(Exception fatalError)
    {
        lock (_lock)
        {
            if (_fatalError is not null)
                return;

            _fatalError = fatalError;
            _isRunning = false;
            _cts?.Cancel();
            _channel.Writer.TryComplete(fatalError);
        }
    }
}