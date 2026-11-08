// The driver for the shared conformance harness (vexo-sdk-conformance/README.md): reads one scenario as JSON on stdin,
// runs its steps against a VexoClient, and prints a JSON line for each step and each reported error.
using System.Diagnostics;
using System.Globalization;
using System.Text.Json;
using Vexo;

JsonElement scenario = JsonDocument.Parse(Console.In.ReadToEnd()).RootElement;
JsonElement options = scenario.GetProperty("options");
var defaults = new VexoOptions();
var vexo = new VexoClient(scenario.GetProperty("apiKey").GetString()!, new VexoOptions
{
    Host = scenario.GetProperty("host").GetString()!,
    FlushAt = Count("flushAt") ?? defaults.FlushAt,
    FlushInterval = Millis("flushIntervalMs") ?? defaults.FlushInterval,
    MaxRetries = Count("maxRetries") ?? defaults.MaxRetries,
    MaxQueueSize = Count("maxQueueSize") ?? defaults.MaxQueueSize,
    Timeout = Millis("timeoutMs") ?? defaults.Timeout,
    ShutdownTimeout = Millis("shutdownTimeoutMs") ?? defaults.ShutdownTimeout,
    OnError = error => Print(new { type = "error", message = error.Message, status = (int?)error.StatusCode }),
});

foreach (JsonElement step in scenario.GetProperty("steps").EnumerateArray())
{
    string op = step.GetProperty("op").GetString()!;
    var stopwatch = Stopwatch.StartNew();
    switch (op)
    {
        case "capture":
            vexo.Capture(Text(step, "distinctId")!, Text(step, "event")!, Properties(step), Timestamp(step),
                Text(step, "messageId"));
            break;
        case "identify":
            vexo.Identify(Text(step, "distinctId")!, step.TryGetProperty("traits", out JsonElement traits) ? traits : null);
            break;
        case "flush":
            await vexo.FlushAsync();
            break;
        case "shutdown":
            await vexo.DisposeAsync();
            break;
        case "sleep":
            await Task.Delay(step.GetProperty("ms").GetInt32());
            break;
        case "exit":
            // Ends at once without exit hooks. In a container, run with --init: PID 1 ignores its own SIGKILL.
            Process.GetCurrentProcess().Kill();
            break;
    }
    Print(new { type = "step", op, ms = stopwatch.ElapsedMilliseconds });
}
// Returns without disposing the client: the exit_hook scenario checks that the SDK's ProcessExit hook sends the queue.

int? Count(string name) => options.TryGetProperty(name, out JsonElement value) ? value.GetInt32() : null;

TimeSpan? Millis(string name) =>
    options.TryGetProperty(name, out JsonElement value) ? TimeSpan.FromMilliseconds(value.GetDouble()) : null;

static string? Text(JsonElement step, string name) =>
    step.TryGetProperty(name, out JsonElement value) && value.ValueKind == JsonValueKind.String ? value.GetString() : null;

// The step's properties. "poison" adds a dictionary that contains itself, which System.Text.Json refuses as a cycle;
// "nan" adds double.NaN, which it refuses as a number JSON can't hold. "dateTime", "decimal", and "invalidUnicode" add
// a DateTimeOffset, a decimal, and a string with a lone surrogate.
static object? Properties(JsonElement step)
{
    bool poison = Flag(step, "poison"), nan = Flag(step, "nan");
    string? dateTime = Text(step, "dateTime"), number = Text(step, "decimal");
    bool invalidUnicode = Flag(step, "invalidUnicode");
    if (!step.TryGetProperty("properties", out JsonElement properties) && !poison && !nan && dateTime is null
        && number is null && !invalidUnicode)
    {
        return null;
    }
    var result = properties.ValueKind == JsonValueKind.Object
        ? properties.Deserialize<Dictionary<string, object?>>()!
        : new Dictionary<string, object?>();
    if (poison)
    {
        var cycle = new Dictionary<string, object?>();
        cycle["self"] = cycle;
        result["poison"] = cycle;
    }
    if (nan)
    {
        result["nan"] = double.NaN;
    }
    if (dateTime is not null)
    {
        result["dateTime"] = DateTimeOffset.Parse(dateTime, CultureInfo.InvariantCulture);
    }
    if (number is not null)
    {
        result["decimal"] = decimal.Parse(number, NumberStyles.Float, CultureInfo.InvariantCulture);
    }
    if (invalidUnicode)
    {
        result["invalidUnicode"] = "ok\uD800";
    }
    return result;
}

static bool Flag(JsonElement step, string name) => step.TryGetProperty(name, out JsonElement value) && value.GetBoolean();

static DateTimeOffset? Timestamp(JsonElement step) =>
    step.TryGetProperty("at", out JsonElement at) ? DateTimeOffset.Parse(at.GetString()!, CultureInfo.InvariantCulture)
    : step.TryGetProperty("timestamp", out JsonElement millis) ? DateTimeOffset.FromUnixTimeMilliseconds((long)millis.GetDouble())
    : null;

static void Print(object line)
{
    // The error callback runs on the sender's thread; Console.Out is synchronized and flushes each line.
    Console.Out.WriteLine(JsonSerializer.Serialize(line));
}
