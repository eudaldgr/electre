using System.Collections.Concurrent;
using System.Net;
using Electre.Protocol;

namespace Electre.Server;

/// <summary>
///     Interface for client sessions, enabling both TCP and WebSocket sessions
///     to work with subscriptions and method handlers.
/// </summary>
public interface IClientSession
{
    /// <summary>
    ///     Gets the unique session identifier.
    /// </summary>
    string Id { get; }

    /// <summary>
    ///     Gets the set of scripthashes this session is subscribed to (thread-safe).
    /// </summary>
    ConcurrentDictionary<string, byte> SubscribedScripthashes { get; }

    /// <summary>
    ///     Gets or sets a value indicating whether this session is subscribed to header notifications.
    /// </summary>
    bool SubscribedToHeaders { get; set; }

    /// <summary>
    ///     Gets the remote IP address of the client, or null if it cannot be determined.
    /// </summary>
    IPAddress? RemoteIp { get; }

    /// <summary>
    ///     Gets a value indicating whether this client is too slow to receive notifications.
    /// </summary>
    bool IsSlowClient { get; }

    /// <summary>
    ///     Gets or sets the client version string reported by the Electrum client.
    /// </summary>
    string? ClientVersion { get; set; }

    /// <summary>
    ///     Gets or sets the protocol version negotiated with the client.
    /// </summary>
    string? ProtocolVersion { get; set; }

    /// <summary>
    ///     Sends a JSON-RPC notification to the client.
    /// </summary>
    /// <param name="notification">The JSON-RPC notification to send.</param>
    /// <returns>A task representing the asynchronous send operation.</returns>
    Task SendNotificationAsync(JsonRpcNotification notification);
}