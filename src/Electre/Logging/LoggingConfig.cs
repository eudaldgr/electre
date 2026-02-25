namespace Electre.Logging;

/// <summary>
///     Configuration options for Serilog logging.
/// </summary>
/// <remarks>
///     Provides settings for controlling logging output to console and file sinks,
///     including log levels, file rotation, and retention policies.
/// </remarks>
public sealed class LoggingConfig
{
    /// <summary>
    ///     Gets or sets the minimum log level for all sinks.
    /// </summary>
    /// <remarks>
    ///     Default value is "Information". Valid values: Verbose, Debug, Information, Warning, Error, Fatal.
    /// </remarks>
    public string MinimumLevel { get; set; } = "Information";

    /// <summary>
    ///     Gets or sets a value indicating whether console logging is enabled.
    /// </summary>
    /// <remarks>
    ///     Default value is true.
    /// </remarks>
    public bool EnableConsole { get; set; } = true;

    /// <summary>
    ///     Gets or sets the minimum log level for console output.
    /// </summary>
    /// <remarks>
    ///     Default value is "Information". Valid values: Verbose, Debug, Information, Warning, Error, Fatal.
    /// </remarks>
    public string ConsoleLevel { get; set; } = "Information";

    /// <summary>
    ///     Gets or sets a value indicating whether file logging is enabled.
    /// </summary>
    /// <remarks>
    ///     Default value is true.
    /// </remarks>
    public bool EnableFile { get; set; } = true;

    /// <summary>
    ///     Gets or sets the file path for log output.
    /// </summary>
    /// <remarks>
    ///     Default value is "./logs/electre.log". Supports rolling file naming with date patterns.
    /// </remarks>
    public string FilePath { get; set; } = "./logs/electre.log";

    /// <summary>
    ///     Gets or sets the minimum log level for file output.
    /// </summary>
    /// <remarks>
    ///     Default value is "Debug". Valid values: Verbose, Debug, Information, Warning, Error, Fatal.
    /// </remarks>
    public string FileLevel { get; set; } = "Debug";

    /// <summary>
    ///     Gets or sets the number of log files to retain.
    /// </summary>
    /// <remarks>
    ///     Default value is 7. Older files are automatically deleted when this limit is exceeded.
    /// </remarks>
    public int RetainedFileCount { get; set; } = 7;

    /// <summary>
    ///     Gets or sets the maximum size of a single log file in megabytes.
    /// </summary>
    /// <remarks>
    ///     Default value is 100. When a file exceeds this size, a new file is created.
    /// </remarks>
    public int FileSizeLimitMb { get; set; } = 100;

    /// <summary>
    ///     Gets or sets the file output format: "text" for human-readable or "json" for compact JSON.
    /// </summary>
    /// <remarks>
    ///     Default value is "text". Valid values: "text", "json".
    /// </remarks>
    public string FileFormat { get; set; } = "text";
}