using System.Net.Http.Headers;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Threading.Channels;

namespace Vexo;

/// <summary>
/// Sends events and identifies to the Vexo capture API. <see cref="Capture"/> and <see cref="Identify"/> only queue a
/// message; a background task sends the queue in batches. Create one client for your process and dispose it when the
/// process stops: disposing sends what is still queued, for up to <see cref="VexoOptions.ShutdownTimeout"/>.
/// </summary>
public sealed class VexoClient : IAsyncDisposable, IDisposable
{
    // The API rejects a batch of more than 500 messages or a body over 5 MB.
    internal const int MaxBatchSize = 500;
    internal const int MaxBodyBytes = 5 * 1024 * 1024;
    private static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan MaxRetryAfter = TimeSpan.FromSeconds(60);
    // Every send and backoff ends at the shutdown deadline; this grace only matters when OnError itself blocks.
    private static readonly TimeSpan ShutdownGrace = TimeSpan.FromSeconds(1);
    private static readonly string UserAgent = "vexo-dotnet/" + typeof(VexoClient).Assembly.GetName().Version!.ToString(3);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web)
    {
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
    };
    private static readonly JsonWriterOptions WriterOptions = new() { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping };
    private static readonly CaptureResult NoResult = new(null, null);
    // A plain object serializes as {}: the payload of a message sent without properties or traits.
    private static readonly object EmptyObject = new();

    private readonly string apiKey;
    private readonly Uri batchUri;
    private readonly int flushAt;
    private readonly TimeSpan flushInterval;
    private readonly int maxRetries;
    private readonly TimeSpan timeout;
    private readonly TimeSpan shutdownTimeout;
    private readonly Action<VexoException> onError;
    private readonly HttpClient http;
    private readonly bool ownsHttp;

    private readonly Channel<byte[]> queue;
    // Held by whoever sends (the worker or a FlushAsync caller), so the queue has one reader at a time.
    private readonly SemaphoreSlim sendLock = new(1, 1);
    private readonly SemaphoreSlim wake = new(0);
    private int wakeRequested;
    private readonly CancellationTokenSource stopping = new();
    // Cancelled at the shutdown deadline: ends the request in flight and any backoff.
    private readonly CancellationTokenSource deadline = new();
    private readonly Lazy<Task> shutdown;
    private readonly Task worker;
    // Set by a batch that used up its retries: later batches get one attempt until a request succeeds. Only used while
    // holding sendLock.
    private bool circuitOpen;

    /// <summary>Creates a client and starts its background sender.</summary>
    /// <param name="apiKey">Your app's API key, from the app's settings in the Vexo dashboard.</param>
    /// <param name="options">Optional settings; see <see cref="VexoOptions"/>.</param>
    /// <exception cref="ArgumentException">The API key is missing or an option is out of range.</exception>
    public VexoClient(string apiKey, VexoOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            throw new ArgumentException("An API key is required.", nameof(apiKey));
        }
        options ??= new VexoOptions();
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.FlushInterval, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.Timeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(options.ShutdownTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfNegative(options.MaxRetries);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(options.MaxQueueSize);

        this.apiKey = apiKey;
        batchUri = new Uri(options.Host.TrimEnd('/') + "/v1/batch");
        flushAt = Math.Clamp(options.FlushAt, 1, MaxBatchSize);
        flushInterval = options.FlushInterval;
        maxRetries = options.MaxRetries;
        timeout = options.Timeout;
        shutdownTimeout = options.ShutdownTimeout;
        onError = options.OnError ?? (error => Console.Error.WriteLine("[vexo] " + error.Message));
        ownsHttp = options.HttpClient is null;
        // A long-lived client must still notice DNS changes, so pooled connections are recycled.
        http = options.HttpClient ?? new HttpClient(new SocketsHttpHandler { PooledConnectionLifetime = TimeSpan.FromMinutes(5) });
        // FullMode Wait: TryWrite returns false when the queue is full, and the message is dropped and reported.
        queue = Channel.CreateBounded<byte[]>(options.MaxQueueSize);
        shutdown = new Lazy<Task>(ShutdownAsync);
        worker = Task.Run(RunAsync);
        // A process that ends without disposing the client still sends what it queued, within the shutdown timeout.
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
    }

    /// <summary>Queues an event. Never throws: an invalid message is dropped and reported to OnError.</summary>
    /// <param name="distinctId">The person's ID: the one you pass to identifyDevice() in your app.</param>
    /// <param name="eventName">The event name.</param>
    /// <param name="properties">An object that serializes to a JSON object: a dictionary, an anonymous type, or a class.</param>
    /// <param name="timestamp">When the event happened. Default: now.</param>
    /// <param name="messageId">A unique ID, so that a retried message is stored once. Default: a new UUID.</param>
    public void Capture(string distinctId, string eventName, object? properties = null, DateTimeOffset? timestamp = null,
        string? messageId = null)
    {
        if (string.IsNullOrWhiteSpace(distinctId) || string.IsNullOrWhiteSpace(eventName))
        {
            Report(new VexoException("Capture needs a distinctId and an event name; message dropped."));
            return;
        }
        Enqueue("capture", distinctId, eventName, "properties", properties,
            (timestamp ?? DateTimeOffset.UtcNow).ToUnixTimeMilliseconds(), messageId);
    }

    /// <summary>
    /// Queues an identify: merges the traits into the person's traits, and a trait set to null is removed. Never throws:
    /// an invalid message is dropped and reported to OnError.
    /// </summary>
    /// <param name="distinctId">The person's ID: the one you pass to identifyDevice() in your app.</param>
    /// <param name="traits">An object that serializes to a JSON object: a dictionary, an anonymous type, or a class.</param>
    public void Identify(string distinctId, object? traits = null)
    {
        if (string.IsNullOrWhiteSpace(distinctId))
        {
            Report(new VexoException("Identify needs a distinctId; message dropped."));
            return;
        }
        Enqueue("identify", distinctId, null, "traits", traits, null, null);
    }

    /// <summary>
    /// Sends every message queued now, and completes once each one is sent or reported to OnError.
    /// </summary>
    /// <param name="cancellationToken">
    /// Stops waiting. A batch already being sent finishes; messages not yet sent stay queued.
    /// </param>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await sendLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            for (int pending = queue.Reader.Count; pending > 0;)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (deadline.IsCancellationRequested)
                {
                    DropQueue();
                    return;
                }
                List<byte[]> batch = NextBatch(Math.Min(flushAt, pending));
                if (batch.Count == 0)
                {
                    return;
                }
                pending -= batch.Count;
                try
                {
                    await SendAsync(batch).ConfigureAwait(false);
                }
                catch (Exception e)
                {
                    // Every send failure is reported with its batch, never thrown.
                    Report(new VexoException("Batch could not be sent: " + e.Message, batch, innerException: e));
                }
            }
        }
        finally
        {
            sendLock.Release();
        }
    }

    /// <summary>
    /// Stops the background sender and sends everything queued, giving up after the shutdown timeout: messages still
    /// queued then are dropped and reported to OnError. Calling it again waits for the first call.
    /// </summary>
    public ValueTask DisposeAsync() => new(shutdown.Value);

    /// <inheritdoc cref="DisposeAsync"/>
    public void Dispose() => shutdown.Value.GetAwaiter().GetResult();

    private async Task ShutdownAsync()
    {
        AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;
        queue.Writer.TryComplete();
        deadline.CancelAfter(shutdownTimeout);
        stopping.Cancel();
        try
        {
            await worker.WaitAsync(shutdownTimeout + ShutdownGrace).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
        }
        if (ownsHttp)
        {
            http.Dispose();
        }
    }

    private void OnProcessExit(object? sender, EventArgs e) => Dispose();

    // Sends on the flush interval, or as soon as flushAt messages are queued; after a stop, sends once more and ends.
    private async Task RunAsync()
    {
        while (!stopping.IsCancellationRequested)
        {
            try
            {
                await wake.WaitAsync(flushInterval, stopping.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            Volatile.Write(ref wakeRequested, 0);
            await FlushAsync().ConfigureAwait(false);
        }
        // The stop can land during a flush that already counted what to send; the queue is closed now, so this one
        // sends the rest, or drops and reports it at the deadline.
        await FlushAsync().ConfigureAwait(false);
    }

    private void Enqueue(string type, string distinctId, string? eventName, string payloadName, object? payload,
        long? timestamp, string? messageId)
    {
        byte[] json;
        try
        {
            json = Serialize(type, distinctId, eventName, payloadName, payload, timestamp,
                string.IsNullOrWhiteSpace(messageId) ? Guid.NewGuid().ToString() : messageId);
        }
        catch (Exception e)
        {
            // Whatever the payload throws (a cycle, NaN, an unsupported type, a failing getter) drops only this message.
            Report(new VexoException($"Message dropped: it can't be sent as JSON ({e.Message})", innerException: e));
            return;
        }
        if (json.Length + BodyStart.Length + BodyEnd.Length > MaxBodyBytes)
        {
            Report(new VexoException("Message dropped: it is larger than 5 MB."));
            return;
        }
        if (!queue.Writer.TryWrite(json))
        {
            Report(new VexoException(shutdown.IsValueCreated
                ? "The client is disposed; message dropped." : "The queue is full; message dropped.", [json]));
            return;
        }
        if (queue.Reader.Count >= flushAt && Interlocked.Exchange(ref wakeRequested, 1) == 0)
        {
            wake.Release();
        }
    }

    // One message as compact UTF-8 JSON, serialized once at enqueue and sent as is on every attempt.
    private static byte[] Serialize(string type, string distinctId, string? eventName, string payloadName,
        object? payload, long? timestamp, string messageId)
    {
        JsonElement data = JsonSerializer.SerializeToElement(payload ?? EmptyObject, Json);
        if (data.ValueKind != JsonValueKind.Object)
        {
            throw new ArgumentException($"{payloadName} must serialize to a JSON object, not {data.ValueKind}");
        }
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        using (var writer = new Utf8JsonWriter(buffer, WriterOptions))
        {
            writer.WriteStartObject();
            writer.WriteString("type", type);
            writer.WriteString("distinctId", distinctId);
            if (eventName is not null)
            {
                writer.WriteString("event", eventName);
            }
            writer.WritePropertyName(payloadName);
            data.WriteTo(writer);
            if (timestamp is long millis)
            {
                writer.WriteNumber("timestamp", millis);
            }
            writer.WriteString("messageId", messageId);
            writer.WriteEndObject();
        }
        return buffer.WrittenSpan.ToArray();
    }

    // Up to max messages that fit in one request body. Only called while holding sendLock, so the message peeked is
    // the one read.
    private List<byte[]> NextBatch(int max)
    {
        var batch = new List<byte[]>();
        long bytes = BodyStart.Length + BodyEnd.Length;
        while (batch.Count < max && queue.Reader.TryPeek(out byte[]? json)
               && bytes + json.Length + batch.Count <= MaxBodyBytes)
        {
            queue.Reader.TryRead(out _);
            bytes += json.Length;
            batch.Add(json);
        }
        return batch;
    }

    private void DropQueue()
    {
        var dropped = new List<byte[]>();
        while (queue.Reader.TryRead(out byte[]? json))
        {
            dropped.Add(json);
        }
        if (dropped.Count > 0)
        {
            Report(new VexoException($"Shutdown deadline reached; {dropped.Count} message(s) dropped.", dropped));
        }
    }

    // POST /v1/batch. Network errors, timeouts, 429, and 5xx are retried with exponential backoff (or the server's
    // Retry-After); the messages the server reports as failed are sent again the same way. Dropped ones are reported.
    private async Task SendAsync(List<byte[]> batch)
    {
        int retries = circuitOpen ? 0 : maxRetries;
        for (int attempt = 0; ; attempt++)
        {
            bool LastAttempt() => attempt >= retries || deadline.IsCancellationRequested;
            TimeSpan wait;
            try
            {
                using HttpResponseMessage response = await PostAsync(batch).ConfigureAwait(false);
                int status = (int)response.StatusCode;
                string body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                if (response.IsSuccessStatusCode)
                {
                    circuitOpen = false;
                    CaptureResult result = ReadResult(body);
                    ReportResults(batch, result.Dropped, "The API dropped");
                    List<byte[]> failed = InRange(batch, result.Failed).Select(r => batch[r.Index]).ToList();
                    if (failed.Count == 0)
                    {
                        return;
                    }
                    if (LastAttempt())
                    {
                        ReportResults(batch, result.Failed, "The API could not store");
                        return;
                    }
                    batch = failed;
                    wait = Backoff(attempt);
                }
                else
                {
                    bool retryable = status == 429 || status >= 500;
                    if (!retryable || LastAttempt())
                    {
                        circuitOpen |= retryable;
                        Report(new VexoException($"Batch rejected with HTTP {status}: {body}", batch, response.StatusCode));
                        return;
                    }
                    wait = RetryDelay(response) ?? Backoff(attempt);
                }
            }
            catch (Exception e) when (e is HttpRequestException or OperationCanceledException)
            {
                if (LastAttempt())
                {
                    circuitOpen = true;
                    string why = deadline.IsCancellationRequested ? "the shutdown deadline was reached" : e.Message;
                    Report(new VexoException("Batch could not be sent: " + why, batch, innerException: e));
                    return;
                }
                wait = Backoff(attempt);
            }
            await DelayAsync(wait).ConfigureAwait(false);
        }
    }

    private async Task<HttpResponseMessage> PostAsync(List<byte[]> batch)
    {
        using var body = new MemoryStream();
        body.Write(BodyStart);
        for (int i = 0; i < batch.Count; i++)
        {
            if (i > 0)
            {
                body.WriteByte((byte)',');
            }
            body.Write(batch[i]);
        }
        body.Write(BodyEnd);

        using var request = new HttpRequestMessage(HttpMethod.Post, batchUri);
        request.Content = new ByteArrayContent(body.GetBuffer(), 0, (int)body.Length);
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        request.Headers.Add("X-Api-Key", apiKey);
        request.Headers.TryAddWithoutValidation("User-Agent", UserAgent);
        using var cancel = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        cancel.CancelAfter(timeout);
        return await http.SendAsync(request, cancel.Token).ConfigureAwait(false);
    }

    private static ReadOnlySpan<byte> BodyStart => "{\"batch\":["u8;

    private static ReadOnlySpan<byte> BodyEnd => "]}"u8;

    // A 2xx body that isn't the API's envelope counts as a success with nothing dropped or failed.
    private static CaptureResult ReadResult(string body)
    {
        try
        {
            return JsonSerializer.Deserialize<Envelope>(body, Json)?.Data ?? NoResult;
        }
        catch (JsonException)
        {
            return NoResult;
        }
    }

    private void ReportResults(List<byte[]> batch, List<MessageResult>? results, string what)
    {
        if (results is { Count: > 0 })
        {
            var messages = InRange(batch, results)
                .Select(r => new VexoDroppedMessage(System.Text.Encoding.UTF8.GetString(batch[r.Index]), r.Reason ?? ""))
                .ToList();
            Report(new VexoException($"{what} {results.Count} message(s): {results[0].Reason}", dropped: messages));
        }
    }

    private static IEnumerable<MessageResult> InRange(List<byte[]> batch, List<MessageResult>? results) =>
        results?.Where(r => r.Index >= 0 && r.Index < batch.Count) ?? [];

    private void Report(VexoException error)
    {
        try
        {
            onError(error);
        }
        catch
        {
            // A throwing callback must neither reach the caller of Capture nor stop the sender.
        }
    }

    // Waits, but never past the shutdown deadline.
    private async Task DelayAsync(TimeSpan wait)
    {
        try
        {
            await Task.Delay(wait, deadline.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static TimeSpan Backoff(int attempt)
    {
        double seconds = Math.Min(Math.Pow(2, attempt), MaxBackoff.TotalSeconds);
        return TimeSpan.FromSeconds(seconds / 2 + Random.Shared.NextDouble() * seconds / 2);
    }

    // Retry-After as seconds or an HTTP date, capped at 60 seconds; null when the response has none.
    internal static TimeSpan? RetryDelay(HttpResponseMessage response)
    {
        RetryConditionHeaderValue? retryAfter = response.Headers.RetryAfter;
        TimeSpan? delay = retryAfter?.Delta ?? retryAfter?.Date - DateTimeOffset.UtcNow;
        return delay is null ? null : TimeSpan.FromTicks(Math.Clamp(delay.Value.Ticks, 0, MaxRetryAfter.Ticks));
    }

    private sealed record Envelope(CaptureResult? Data);

    private sealed record CaptureResult(List<MessageResult>? Dropped, List<MessageResult>? Failed);

    private sealed record MessageResult(int Index, string? Reason);
}
