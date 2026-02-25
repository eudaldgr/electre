using System.Text.Json;
using System.Text.Json.Serialization;

namespace Electre.Protocol;

/// <summary>
///     Represents a JSON-RPC 2.0 request.
/// </summary>
public sealed class JsonRpcRequest
{
    /// <summary>
    ///     Gets or sets the request identifier, used to correlate responses with requests.
    /// </summary>
    [JsonPropertyName("id")]
    public JsonElement? Id { get; set; }

    /// <summary>
    ///     Gets or sets the JSON-RPC protocol version (typically "2.0").
    /// </summary>
    [JsonPropertyName("jsonrpc")]
    public string? JsonRpc { get; set; }

    /// <summary>
    ///     Gets or sets the name of the method to invoke.
    /// </summary>
    [JsonPropertyName("method")]
    public string Method { get; set; } = string.Empty;

    /// <summary>
    ///     Gets or sets the parameters for the method as a JSON element.
    /// </summary>
    [JsonPropertyName("params")]
    public JsonElement? Params { get; set; }

    /// <summary>
    ///     Extracts and converts the parameters from the JSON element to an object array.
    /// </summary>
    /// <returns>An array of parameter objects, or an empty array if no parameters are present.</returns>
    public object?[] GetParams()
    {
        if (Params is null || Params.Value.ValueKind == JsonValueKind.Undefined)
            return [];

        if (Params.Value.ValueKind == JsonValueKind.Array)
        {
            var list = new List<object?>();
            foreach (var item in Params.Value.EnumerateArray())
                list.Add(JsonElementToObject(item));

            return list.ToArray();
        }

        return [];
    }

    /// <summary>
    ///     Converts a JSON element to a corresponding C# object.
    /// </summary>
    /// <param name="element">The JSON element to convert.</param>
    /// <returns>The converted object, or null if the element is null or unknown.</returns>
    private static object? JsonElementToObject(JsonElement element)
    {
        return element.ValueKind switch
        {
            JsonValueKind.String => element.GetString(),
            JsonValueKind.Number => element.TryGetInt64(out var l) ? l : element.GetDouble(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            JsonValueKind.Array => element.EnumerateArray().Select(JsonElementToObject).ToArray(),
            JsonValueKind.Object => element,
            _ => null
        };
    }
}

/// <summary>
///     Represents a JSON-RPC 2.0 response.
/// </summary>
public sealed class JsonRpcResponse
{
    /// <summary>
    ///     Gets or sets the response identifier, matching the request id.
    /// </summary>
    [JsonPropertyName("id")]
    public object? Id { get; set; }

    /// <summary>
    ///     Gets or sets the JSON-RPC protocol version (always "2.0").
    /// </summary>
    [JsonPropertyName("jsonrpc")]
    public string JsonRpc { get; set; } = "2.0";

    /// <summary>
    ///     Gets or sets the result of the method invocation.
    /// </summary>
    [JsonPropertyName("result")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
    public ResultWrapper? Result { get; set; }

    /// <summary>
    ///     Gets or sets the error object if the method invocation failed.
    /// </summary>
    [JsonPropertyName("error")]
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public JsonRpcError? Error { get; set; }

    /// <summary>
    ///     Creates a successful JSON-RPC response.
    /// </summary>
    /// <param name="id">The request identifier.</param>
    /// <param name="result">The result value to return.</param>
    /// <returns>A new JsonRpcResponse with the result set.</returns>
    public static JsonRpcResponse Success(object? id, object? result)
    {
        return new JsonRpcResponse
        {
            Id = id,
            Result = new ResultWrapper { Value = result }
        };
    }

    /// <summary>
    ///     Creates a failed JSON-RPC response with an error.
    /// </summary>
    /// <param name="id">The request identifier.</param>
    /// <param name="code">The error code.</param>
    /// <param name="message">The error message.</param>
    /// <returns>A new JsonRpcResponse with the error set.</returns>
    public static JsonRpcResponse Failure(object? id, int code, string message)
    {
        return new JsonRpcResponse
        {
            Id = id,
            Error = new JsonRpcError { Code = code, Message = message }
        };
    }
}

/// <summary>
///     Wraps a JSON-RPC result value for custom serialization.
/// </summary>
[JsonConverter(typeof(ResultWrapperConverter))]
public sealed class ResultWrapper
{
    /// <summary>
    ///     Gets or sets the wrapped result value.
    /// </summary>
    public object? Value { get; set; }
}

/// <summary>
///     Custom JSON converter for ResultWrapper that serializes the wrapped value directly.
/// </summary>
public sealed class ResultWrapperConverter : JsonConverter<ResultWrapper>
{
    /// <summary>
    ///     Deserialization is not implemented for this converter.
    /// </summary>
    /// <param name="reader">The JSON reader.</param>
    /// <param name="typeToConvert">The type to convert to.</param>
    /// <param name="options">The serializer options.</param>
    /// <returns>This method is not implemented.</returns>
    /// <exception cref="NotImplementedException">Always thrown as deserialization is not supported.</exception>
    public override ResultWrapper? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        // Required by JsonConverter<T> interface — only Write is used
        throw new NotImplementedException();
    }

    /// <summary>
    ///     Serializes the wrapped value directly without the wrapper object.
    /// </summary>
    /// <param name="writer">The JSON writer.</param>
    /// <param name="value">The ResultWrapper instance to serialize.</param>
    /// <param name="options">The serializer options.</param>
    public override void Write(Utf8JsonWriter writer, ResultWrapper value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value.Value, options);
    }
}

/// <summary>
///     Represents a JSON-RPC 2.0 error object.
/// </summary>
public sealed class JsonRpcError
{
    /// <summary>
    ///     Gets or sets the error code.
    /// </summary>
    [JsonPropertyName("code")]
    public int Code { get; set; }

    /// <summary>
    ///     Gets or sets the error message.
    /// </summary>
    [JsonPropertyName("message")]
    public string Message { get; set; } = "";
}

/// <summary>
///     Represents a JSON-RPC 2.0 notification (a request without an id).
/// </summary>
public sealed class JsonRpcNotification
{
    /// <summary>
    ///     Gets or sets the JSON-RPC protocol version (always "2.0").
    /// </summary>
    [JsonPropertyName("jsonrpc")]
    public string JsonRpc { get; set; } = "2.0";

    /// <summary>
    ///     Gets or sets the name of the method to invoke.
    /// </summary>
    [JsonPropertyName("method")]
    public string Method { get; set; } = "";

    /// <summary>
    ///     Gets or sets the parameters for the method.
    /// </summary>
    [JsonPropertyName("params")]
    public object?[] Params { get; set; } = [];
}

/// <summary>
///     Standard JSON-RPC 2.0 error codes.
/// </summary>
public static class JsonRpcErrorCodes
{
    /// <summary>
    ///     Invalid JSON was received by the server.
    /// </summary>
    public const int ParseError = -32700;

    /// <summary>
    ///     The JSON sent is not a valid Request object.
    /// </summary>
    public const int InvalidRequest = -32600;

    /// <summary>
    ///     The method does not exist or is not available.
    /// </summary>
    public const int MethodNotFound = -32601;

    /// <summary>
    ///     Invalid method parameter(s).
    /// </summary>
    public const int InvalidParams = -32602;

    /// <summary>
    ///     Internal JSON-RPC error.
    /// </summary>
    public const int InternalError = -32603;

    /// <summary>
    ///     Server error (reserved for implementation-defined server errors).
    /// </summary>
    public const int ServerError = -32000;

    /// <summary>
    ///     Server is busy and cannot process the request.
    /// </summary>
    public const int ServerBusy = -32001;
}

/// <summary>
///     Provides JSON serialization and deserialization for JSON-RPC protocol objects.
/// </summary>
public static class JsonRpcSerializer
{
    /// <summary>
    ///     JSON serializer options configured for JSON-RPC protocol (camelCase naming, null handling).
    /// </summary>
    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    /// <summary>
    ///     Checks if a JSON string is a batch request (starts with '[').
    /// </summary>
    /// <param name="json">The JSON string to check.</param>
    /// <returns>True if the string starts with '[', indicating a batch request.</returns>
    public static bool IsBatchRequest(string json)
    {
        var trimmed = json.AsSpan().TrimStart();
        return trimmed.Length > 0 && trimmed[0] == '[';
    }

    /// <summary>
    ///     Parses a JSON string into an array of JsonRpcRequest objects (batch request).
    /// </summary>
    /// <param name="json">The JSON string to parse.</param>
    /// <returns>An array of JsonRpcRequest objects, or null if parsing fails.</returns>
    public static JsonRpcRequest[]? ParseBatchRequest(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<JsonRpcRequest[]>(json, Options);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Parses a JSON string into a JsonRpcRequest object.
    /// </summary>
    /// <param name="json">The JSON string to parse.</param>
    /// <returns>A JsonRpcRequest object, or null if parsing fails.</returns>
    public static JsonRpcRequest? ParseRequest(string json)
    {
        try
        {
            return JsonSerializer.Deserialize<JsonRpcRequest>(json, Options);
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    ///     Serializes a JsonRpcResponse object to a JSON string.
    /// </summary>
    /// <param name="response">The response to serialize.</param>
    /// <returns>A JSON string representation of the response.</returns>
    public static string Serialize(JsonRpcResponse response)
    {
        return JsonSerializer.Serialize(response, Options);
    }

    /// <summary>
    ///     Serializes a JsonRpcNotification object to a JSON string.
    /// </summary>
    /// <param name="notification">The notification to serialize.</param>
    /// <returns>A JSON string representation of the notification.</returns>
    public static string Serialize(JsonRpcNotification notification)
    {
        return JsonSerializer.Serialize(notification, Options);
    }
}