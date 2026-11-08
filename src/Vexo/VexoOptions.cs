namespace Vexo;

/// <summary>Options for <see cref="VexoClient"/>. The client reads them once, when you create it.</summary>
public sealed class VexoOptions
{
    /// <summary>The API base URL. Default: <c>https://api.vexo.co</c>.</summary>
    public string Host { get; set; } = "https://api.vexo.co";

    /// <summary>Send a batch when this many messages are queued, from 1 to 500. Default: 100.</summary>
    public int FlushAt { get; set; } = 100;

    /// <summary>Send whatever is queued this often. Default: 10 seconds.</summary>
    public TimeSpan FlushInterval { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>Retries for network errors, timeouts, 429, and 5xx responses. Default: 3.</summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>Messages kept in memory; new messages are dropped once the queue is full. Default: 10,000.</summary>
    public int MaxQueueSize { get; set; } = 10_000;

    /// <summary>The timeout of each request. Default: 10 seconds.</summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>How long disposing the client keeps sending what is queued. Default: 10 seconds.</summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Called with send failures and with the messages that Vexo dropped. Default: write the message to standard error.
    /// </summary>
    public Action<VexoException>? OnError { get; set; }

    /// <summary>
    /// The <see cref="System.Net.Http.HttpClient"/> to send with, for example one from <c>IHttpClientFactory</c>. The
    /// client doesn't dispose it. Default: a client that the <see cref="VexoClient"/> creates and disposes.
    /// </summary>
    public HttpClient? HttpClient { get; set; }
}
