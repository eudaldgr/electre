using System.Net;
using System.Text;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Electre.Metrics;

/// <summary>
///     HTTP server for exposing Prometheus metrics and health check endpoints.
/// </summary>
/// <remarks>
///     Runs as a background service and listens on a configured port to serve:
///     - /metrics: Prometheus-format metrics
///     - /health: Liveness probe (tags: "live")
///     - /ready: Readiness probe (tags: "ready")
/// </remarks>
public sealed class MetricsServer : BackgroundService
{
    private readonly HealthCheckService _healthCheckService;

    private readonly HttpListener _listener;
    private readonly ILogger _logger;
    private readonly int _port;

    /// <summary>
    ///     Initializes a new instance of the <see cref="MetricsServer" /> class.
    /// </summary>
    /// <param name="config">Configuration containing the metrics port.</param>
    /// <param name="healthCheckService">Service for executing health checks.</param>
    /// <param name="loggerFactory">Factory for creating loggers.</param>
    public MetricsServer(IOptions<Config> config, HealthCheckService healthCheckService, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory.CreateLogger("Metrics");
        _healthCheckService = healthCheckService;
        _port = config.Value.MetricsPort;
        _listener = new HttpListener();
        _listener.Prefixes.Add($"http://+:{_port}/");
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        try
        {
            _listener.Start();
            _logger.LogInformation("Metrics server listening on port {Port}", _port);

            await RunAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to start metrics server");
        }
    }

    /// <summary>
    ///     Main server loop that accepts and handles HTTP requests.
    /// </summary>
    /// <param name="ct">Cancellation token to stop the server.</param>
    private async Task RunAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
            try
            {
                var context = await _listener.GetContextAsync().WaitAsync(ct);
                _ = HandleRequestAsync(context);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (HttpListenerException) when (ct.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Metrics server loop error");
            }

        _listener.Stop();
    }

    /// <summary>
    ///     Handles an incoming HTTP request by routing to the appropriate handler.
    /// </summary>
    /// <param name="context">The HTTP listener context containing request and response.</param>
    private async Task HandleRequestAsync(HttpListenerContext context)
    {
        try
        {
            var path = context.Request.Url?.AbsolutePath ?? "/";

            switch (path)
            {
                case "/metrics":
                    await HandleMetricsAsync(context);
                    break;
                case "/health":
                    await HandleHealthAsync(context, x => x.Tags.Contains("live"));
                    break;
                case "/ready":
                    await HandleHealthAsync(context, x => x.Tags.Contains("ready"));
                    break;
                default:
                    context.Response.StatusCode = 404;
                    await WriteResponseAsync(context, "Not Found");
                    break;
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Request handling error");
            try
            {
                context.Response.StatusCode = 500;
                await WriteResponseAsync(context, "Internal Server Error");
            }
            catch (Exception ex2)
            {
                _logger.LogDebug(ex2, "Error writing metrics response");
            }
        }
        finally
        {
            context.Response.Close();
        }
    }

    /// <summary>
    ///     Handles requests to the /metrics endpoint by exporting Prometheus metrics.
    /// </summary>
    /// <param name="context">The HTTP listener context.</param>
    private static async Task HandleMetricsAsync(HttpListenerContext context)
    {
        context.Response.ContentType = "text/plain; charset=utf-8";
        context.Response.StatusCode = 200;

        await using var stream = context.Response.OutputStream;
        await Prometheus.Metrics.DefaultRegistry.CollectAndExportAsTextAsync(stream);
    }

    /// <summary>
    ///     Handles health check requests by filtering checks by the provided predicate.
    /// </summary>
    /// <param name="context">The HTTP listener context.</param>
    /// <param name="predicate">Filter function to select which health checks to run.</param>
    private async Task HandleHealthAsync(HttpListenerContext context, Func<HealthCheckRegistration, bool> predicate)
    {
        var report = await _healthCheckService.CheckHealthAsync(predicate);

        context.Response.ContentType = "application/json";
        context.Response.StatusCode = report.Status == HealthStatus.Healthy ? 200 : 503;

        var json = new StringBuilder();
        json.Append('{');
        json.Append($"\"status\":\"{report.Status}\",");
        json.Append($"\"totalDuration\":\"{report.TotalDuration}\",");
        json.Append("\"entries\":{");

        var first = true;
        foreach (var entry in report.Entries)
        {
            if (!first) json.Append(',');
            first = false;

            json.Append($"\"{entry.Key}\":{{");
            json.Append($"\"status\":\"{entry.Value.Status}\",");
            json.Append($"\"description\":\"{entry.Value.Description}\",");
            json.Append($"\"duration\":\"{entry.Value.Duration}\"");
            json.Append('}');
        }

        json.Append("}}");

        await WriteResponseAsync(context, json.ToString());
    }

    /// <summary>
    ///     Writes a response string to the HTTP response stream.
    /// </summary>
    /// <param name="context">The HTTP listener context.</param>
    /// <param name="content">The response content to write.</param>
    private static async Task WriteResponseAsync(HttpListenerContext context, string content)
    {
        var bytes = Encoding.UTF8.GetBytes(content);
        context.Response.ContentLength64 = bytes.Length;
        await context.Response.OutputStream.WriteAsync(bytes);
    }
}