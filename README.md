# vexo-dotnet

Send events and identifies to [Vexo](https://vexo.co) from your .NET backend: purchases, subscription changes, background jobs, anything your app's SDK can't see. Server events land in the same app as your mobile and web events, bill as events, and join the person who has the same distinct ID.

- No dependencies beyond .NET 8 or later (`HttpClient`, `System.Text.Json`, and `System.Threading.Channels`).
- Batches messages on a background task, sends them on size and on an interval, and sends what's queued when you dispose the client or the process exits, within a deadline.
- Retries network errors, timeouts, `429`, and `5xx` responses (and the messages the API reports as `failed`) with exponential backoff, and honors `Retry-After`.
- Keeps each request under the API's 5 MB body limit, and is safe to call from many threads.

## Install

```bash
dotnet add package Vexo.Server
```

## Usage

```csharp
using Vexo;

await using var vexo = new VexoClient(Environment.GetEnvironmentVariable("VEXO_API_KEY")!);

// Attach traits to the person. Use the same ID you pass to identifyDevice() in the app SDK.
vexo.Identify("user@example.com", new { plan = "pro", company = "Acme" });

// Record an event.
vexo.Capture("user@example.com", "purchase_completed", new { amount = 49.9, currency = "USD" });
```

`Capture()` and `Identify()` only queue the message, so they never block a request. Create one client for your process and reuse it.

Copy your app's API key from the app's [settings](https://docs.vexo.co/settings/app-settings/app-general-information) in the Vexo dashboard. It's the same key your app SDK uses, so it isn't a secret; see [Security](https://docs.vexo.co/server-side/capture#security).

## Behavior on shutdown

Disposing the client (`await vexo.DisposeAsync()` or `vexo.Dispose()`) stops the background task and sends everything queued. It gives up after `ShutdownTimeout`: it cancels the request in flight, and reports the messages it couldn't send to `OnError`. Disposing again waits for the first call.

If your process exits without disposing the client, the client disposes itself from `AppDomain.ProcessExit`, with the same deadline, so a normal exit or a `SIGTERM` never waits longer than `ShutdownTimeout`. A process that's killed (for example, with `SIGKILL`) loses what's still queued.

## ASP.NET Core

Register the client as a singleton with a factory, so the container disposes it when the host stops, after in-flight requests finish:

```csharp
builder.Services.AddSingleton(services => new VexoClient(
    builder.Configuration["Vexo:ApiKey"]!,
    new VexoOptions
    {
        OnError = error => services.GetRequiredService<ILogger<VexoClient>>().LogWarning(error, "Vexo: {Message}", error.Message),
    }));

var app = builder.Build();

// Optional: start sending the queue as soon as shutdown begins. Disposal still waits for it, within ShutdownTimeout.
app.Lifetime.ApplicationStopping.Register(() => _ = app.Services.GetRequiredService<VexoClient>().FlushAsync());
```

Inject `VexoClient` where you need it. Don't register an instance you created yourself (`AddSingleton(vexo)`): the container doesn't dispose instances it didn't create. Keep `ShutdownTimeout` inside your platform's grace period, such as Kubernetes' `terminationGracePeriodSeconds`.

To send through `IHttpClientFactory`, pass a client from it as `VexoOptions.HttpClient`.

## Azure Functions and other serverless hosts

In the isolated worker model, register the client as a singleton in `Program.cs`, the same way as in ASP.NET Core. A serverless host can freeze an idle instance between invocations or recycle it without a normal exit, so a message queued in one invocation can be late or lost. When delivery matters, send the queue before the function returns:

```csharp
public class Checkout(VexoClient vexo)
{
    [Function("Checkout")]
    public async Task<HttpResponseData> Run([HttpTrigger(AuthorizationLevel.Function, "post")] HttpRequestData request)
    {
        vexo.Capture("user@example.com", "purchase_completed", new { amount = 49.9 });
        await vexo.FlushAsync();
        return request.CreateResponse(HttpStatusCode.OK);
    }
}
```

The same applies to AWS Lambda.

## API

### `new VexoClient(apiKey, options)`

The constructor throws `ArgumentException` when the API key is missing or an option is out of range. Every other failure goes to `OnError`.

| `VexoOptions` property | Default | Description |
| --- | --- | --- |
| `Host` | `https://api.vexo.co` | API base URL. |
| `FlushAt` | `100` | Send a batch when this many messages are queued (1 to 500). |
| `FlushInterval` | 10 seconds | Send whatever is queued this often. `TimeSpan.Zero` turns the timer off, so messages go out only on `FlushAt`, `FlushAsync()`, and disposal. |
| `MaxRetries` | `3` | Retries for network errors, timeouts, `429`, and `5xx` responses. |
| `MaxQueueSize` | `10000` | Messages kept in memory; new messages are dropped once it's full. |
| `Timeout` | 10 seconds | The timeout of each request. |
| `ShutdownTimeout` | 10 seconds | How long disposing the client keeps sending. |
| `OnError` | Write to standard error | Called with send failures and with the messages that Vexo dropped. |
| `HttpClient` | A client of its own | The `HttpClient` to send with. The client doesn't dispose one you pass. |

### `vexo.Capture(distinctId, eventName, properties, timestamp, messageId)`

Queues an event. Only `distinctId` and `eventName` are required.

- `properties`: an object that serializes to a JSON object, such as a dictionary, an anonymous type, or a class. Property names of anonymous types and classes are sent in camelCase, as ASP.NET Core does; dictionary keys are sent as they are.
- `timestamp`: a `DateTimeOffset`, sent as integer epoch milliseconds. It defaults to the time you call `Capture()`. A `DateTime` converts to it implicitly; .NET reads a `DateTime` of kind `Unspecified` as local time, so pass a UTC `DateTime` or a `DateTimeOffset`. For epoch milliseconds, use `DateTimeOffset.FromUnixTimeMilliseconds()`.
- `messageId`: a unique ID that makes retries safe. It defaults to a new UUID. Pass your own, such as an order ID, so that a message you send twice is stored once.

### `vexo.Identify(distinctId, traits)`

Queues an identify. Traits merge into the person's existing traits; a trait set to `null` is removed.

### `await vexo.FlushAsync(cancellationToken)`

Sends every message queued now, and completes once each one is sent or reported to `OnError`. The cancellation token stops the wait: a batch already being sent finishes, and the messages not yet sent stay queued.

### `await vexo.DisposeAsync()` and `vexo.Dispose()`

Stops the background task and sends everything queued, for up to `ShutdownTimeout`. See [Behavior on shutdown](#behavior-on-shutdown).

## Errors and dropped messages

The client never throws from `Capture()` or `Identify()`. Failures go to `OnError` as a `VexoException`:

- Values serialize with `System.Text.Json`: a `decimal` is a JSON number, and a `Guid`, `DateTime`, or `DateTimeOffset` is a string. A message that can't be serialized (for example, a circular reference, or a `NaN` or infinite number anywhere in it, which the API would reject with the whole batch), that isn't a JSON object, or that is over 5 MB is dropped when you queue it; the rest of the queue is unaffected.
- A batch that still fails after the retries, or that the API rejects with a `4xx`, arrives with `Batch` (the messages it held, as JSON) and, when the API answered, `StatusCode`. After that, the next batches get a single attempt until a request succeeds, so an unreachable API doesn't hold up your process.
- A message the API dropped (for example, properties over 64 KB) arrives in `Dropped`, a list of `VexoDroppedMessage` with the message and the reason. Messages the API reports as `failed` are retried, and reported the same way if they still fail.
- Messages still queued at the shutdown deadline arrive as one error with `Batch`.

Every message carries a `messageId`, so a retried batch is stored once.

## Development

You need the .NET 8 SDK, or Docker:

```bash
docker run --rm -v "$PWD":/src -w /src mcr.microsoft.com/dotnet/sdk:8.0 dotnet test
```

To run the shared conformance scenarios from a `vexo-sdk-conformance` checkout next to this one, build the driver once, then point the harness at it:

```bash
docker run --rm -v "$PWD":/src -w /src mcr.microsoft.com/dotnet/sdk:8.0 dotnet build conformance -c Release
python3 ../vexo-sdk-conformance/run.py --bind 0.0.0.0 --host-url "http://host.docker.internal:{port}" \
  --driver "docker run --rm -i --init -v $PWD:/src:ro -w /src mcr.microsoft.com/dotnet/sdk:8.0 dotnet conformance/bin/Release/net8.0/Vexo.Conformance.dll"
```

## License

MIT
