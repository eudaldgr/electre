using System.Collections.Concurrent;
using System.Net;
using Electre.Protocol;

namespace Electre.Server;

/// <summary>
///     Thrown when a subscription limit (global or per-IP) is reached.
/// </summary>
public sealed class SubscriptionLimitException(string message) : Exception(message);

/// <summary>
///     Manages scripthash and header subscriptions across client sessions, broadcasting notifications to subscribers.
/// </summary>
public sealed class SubscriptionManager
{
    private const int MaxParallelNotificationSends = 64;
    private readonly HashSet<IClientSession> _headerSubs = [];
    private readonly object _lock = new();
    private readonly int _maxSubsGlobally;
    private readonly int _maxSubsPerIp;
    private readonly ConcurrentDictionary<string, HashSet<IClientSession>> _scripthashSubs = new();
    private readonly ConcurrentDictionary<IPAddress, int> _subsPerIp = new();

    private int _totalSubs;

    public SubscriptionManager(int maxSubsGlobally = 10_000_000, int maxSubsPerIp = 50_000)
    {
        _maxSubsGlobally = maxSubsGlobally;
        _maxSubsPerIp = maxSubsPerIp;
    }

    /// <summary>
    ///     Subscribes a session to notifications for a specific scripthash.
    /// </summary>
    /// <param name="session">The client session to subscribe.</param>
    /// <param name="scripthash">The scripthash to subscribe to.</param>
    /// <exception cref="SubscriptionLimitException">Thrown when global or per-IP limit is reached.</exception>
    public void SubscribeScripthash(IClientSession session, string scripthash)
    {
        if (_totalSubs >= _maxSubsGlobally)
            throw new SubscriptionLimitException("Global subscription limit reached");

        if (session.RemoteIp is not null &&
            _subsPerIp.TryGetValue(session.RemoteIp, out var ipCount) &&
            ipCount >= _maxSubsPerIp)
            throw new SubscriptionLimitException("Per-IP subscription limit reached");

        var subs = _scripthashSubs.GetOrAdd(scripthash, _ => []);
        lock (subs)
        {
            subs.Add(session);
        }

        if (session.SubscribedScripthashes.TryAdd(scripthash, 0))
        {
            Interlocked.Increment(ref _totalSubs);
            if (session.RemoteIp is not null)
                _subsPerIp.AddOrUpdate(session.RemoteIp, 1, (_, c) => c + 1);
        }
    }

    /// <summary>
    ///     Unsubscribes a session from notifications for a specific scripthash.
    /// </summary>
    /// <param name="session">The client session to unsubscribe.</param>
    /// <param name="scripthash">The scripthash to unsubscribe from.</param>
    /// <returns>True if the session was subscribed to the scripthash; otherwise, false.</returns>
    public bool UnsubscribeScripthash(IClientSession session, string scripthash)
    {
        if (_scripthashSubs.TryGetValue(scripthash, out var subs))
            lock (subs)
            {
                subs.Remove(session);
                if (subs.Count == 0)
                    _scripthashSubs.TryRemove(scripthash, out _);
            }

        if (session.SubscribedScripthashes.TryRemove(scripthash, out _))
        {
            Interlocked.Decrement(ref _totalSubs);
            if (session.RemoteIp is not null)
                _subsPerIp.AddOrUpdate(session.RemoteIp, 0, (_, c) => Math.Max(0, c - 1));
            return true;
        }

        return false;
    }

    /// <summary>
    ///     Subscribes a session to header notifications.
    /// </summary>
    /// <param name="session">The client session to subscribe.</param>
    public void SubscribeHeaders(IClientSession session)
    {
        lock (_lock)
        {
            _headerSubs.Add(session);
        }

        session.SubscribedToHeaders = true;
    }

    /// <summary>
    ///     Unsubscribes a session from all scripthash and header subscriptions.
    /// </summary>
    /// <param name="session">The client session to unsubscribe.</param>
    public void UnsubscribeAll(IClientSession session)
    {
        var subCount = 0;
        foreach (var scripthash in session.SubscribedScripthashes.Keys)
        {
            if (_scripthashSubs.TryGetValue(scripthash, out var subs))
                lock (subs)
                {
                    subs.Remove(session);
                    if (subs.Count == 0)
                        _scripthashSubs.TryRemove(scripthash, out _);
                }

            subCount++;
        }

        session.SubscribedScripthashes.Clear();

        if (subCount > 0)
        {
            Interlocked.Add(ref _totalSubs, -subCount);
            if (session.RemoteIp is not null)
                _subsPerIp.AddOrUpdate(session.RemoteIp, 0, (_, c) => Math.Max(0, c - subCount));
        }

        lock (_lock)
        {
            _headerSubs.Remove(session);
        }

        session.SubscribedToHeaders = false;
    }

    /// <summary>
    ///     Broadcasts a scripthash status notification to all subscribed sessions.
    /// </summary>
    /// <param name="scripthash">The scripthash that changed.</param>
    /// <param name="status">The new status hash for the scripthash, or null.</param>
    /// <returns>A task representing the asynchronous broadcast operation.</returns>
    public async Task NotifyScripthashAsync(string scripthash, string? status)
    {
        if (!_scripthashSubs.TryGetValue(scripthash, out var subs))
            return;

        IClientSession[] sessions;
        lock (subs)
        {
            sessions = subs.ToArray();
        }

        var notification = new JsonRpcNotification
        {
            Method = "blockchain.scripthash.subscribe",
            Params = [scripthash, status]
        };

        await NotifySessionsAsync(sessions, notification);
    }

    /// <summary>
    ///     Broadcasts a new block header notification to all subscribed sessions.
    /// </summary>
    /// <param name="height">The height of the new block.</param>
    /// <param name="header">The raw block header bytes.</param>
    /// <returns>A task representing the asynchronous broadcast operation.</returns>
    public async Task NotifyHeadersAsync(int height, byte[] header)
    {
        IClientSession[] sessions;
        lock (_lock)
        {
            sessions = _headerSubs.ToArray();
        }

        var notification = new JsonRpcNotification
        {
            Method = "blockchain.headers.subscribe",
            Params =
            [
                new Dictionary<string, object>
                {
                    ["height"] = height,
                    ["hex"] = Convert.ToHexStringLower(header)
                }
            ]
        };

        await NotifySessionsAsync(sessions, notification);
    }

    private static Task NotifySessionsAsync(IClientSession[] sessions, JsonRpcNotification notification)
    {
        return Parallel.ForEachAsync(
            sessions,
            new ParallelOptions { MaxDegreeOfParallelism = MaxParallelNotificationSends },
            async (session, _) =>
            {
                try
                {
                    await session.SendNotificationAsync(notification);
                }
                catch
                {
                    // Session might be disconnected
                }
            });
    }
}