using System.Text;
using Microsoft.Extensions.Logging;
using NetMQ;
using NetMQ.Sockets;

namespace Electre.Indexer;

/// <summary>
///     Listens to Bitcoin Core ZMQ notifications for new blocks.
///     Connects to a ZMQ socket, subscribes to "hashblock" messages,
///     and signals when a valid block hash is received.
/// </summary>
public class ZmqBlockNotifier : IDisposable
{
    private readonly ILogger<ZmqBlockNotifier> _logger;
    private readonly SemaphoreSlim _signal;
    private readonly string _zmqUrl;
    private bool _disposed;
    private SubscriberSocket? _socket;

    /// <summary>
    ///     Initializes a new instance of the ZmqBlockNotifier class.
    /// </summary>
    /// <param name="zmqUrl">The ZMQ connection URL (e.g., "tcp://127.0.0.1:28332")</param>
    /// <param name="signal">Semaphore to signal when a block is received</param>
    /// <param name="lf">Logger factory for creating loggers</param>
    public ZmqBlockNotifier(string zmqUrl, SemaphoreSlim signal, ILoggerFactory lf)
    {
        _zmqUrl = zmqUrl ?? throw new ArgumentNullException(nameof(zmqUrl));
        _signal = signal ?? throw new ArgumentNullException(nameof(signal));
        _logger = lf?.CreateLogger<ZmqBlockNotifier>() ?? throw new ArgumentNullException(nameof(lf));
    }

    /// <summary>
    ///     Disposes the ZMQ socket and releases resources.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
            return;

        _socket?.Dispose();
        _disposed = true;
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Runs the ZMQ block notifier loop until cancellation is requested.
    ///     Connects to the ZMQ socket, subscribes to "hashblock" messages,
    ///     and processes incoming block notifications.
    /// </summary>
    /// <param name="ct">Cancellation token to stop the notifier</param>
    public async Task RunAsync(CancellationToken ct)
    {
        try
        {
            _socket = new SubscriberSocket();
            _socket.Connect(_zmqUrl);
            _socket.Subscribe("hashblock");

            _logger.LogInformation("Connected to ZMQ at {ZmqUrl}, subscribed to hashblock", _zmqUrl);

            while (!ct.IsCancellationRequested)
            {
                try
                {
                    // Try to receive multipart message with 100ms timeout
                    var message = new NetMQMessage();
                    if (_socket.TryReceiveMultipartMessage(TimeSpan.FromMilliseconds(100), ref message))
                        if (message.FrameCount >= 2)
                        {
                            var topic = message[0].ToByteArray();
                            var hash = message[1].ToByteArray();

                            var blockHash = ParseHashBlockMessage(topic, hash);
                            if (blockHash is not null)
                            {
                                _logger.LogInformation("Received block notification: {BlockHash}", blockHash);
                                _signal.Release();
                            }
                        }
                }
                catch (OperationCanceledException)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error processing ZMQ message");
                }

                // Yield to allow cancellation checks
                await Task.Delay(10, ct).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("ZMQ block notifier cancelled");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Fatal error in ZMQ block notifier");
            throw;
        }
        finally
        {
            _socket?.Dispose();
        }
    }

    /// <summary>
    ///     Parses a ZMQ hashblock message and extracts the block hash.
    /// </summary>
    /// <param name="topic">The ZMQ topic frame (should be "hashblock")</param>
    /// <param name="hash">The ZMQ hash frame (32-byte block hash in little-endian)</param>
    /// <returns>The block hash as a hex string, or null if parsing fails</returns>
    public static string? ParseHashBlockMessage(byte[] topic, byte[] hash)
    {
        try
        {
            // Validate topic is "hashblock"
            var topicStr = Encoding.UTF8.GetString(topic);
            if (topicStr != "hashblock")
                return null;

            // Validate hash is 32 bytes
            if (hash is null || hash.Length != 32)
                return null;

            // Convert hash to hex string (little-endian as received from Bitcoin Core)
            return Convert.ToHexString(hash);
        }
        catch
        {
            return null;
        }
    }
}