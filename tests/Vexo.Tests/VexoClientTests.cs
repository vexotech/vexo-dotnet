using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http.Headers;
using System.Text.Json;

namespace Vexo.Tests;

public sealed class VexoClientTests : IDisposable
{
    private readonly List<VexoException> errors = new();
    private readonly List<VexoClient> clients = new();

    private VexoException[] Errors
    {
        get
        {
            lock (errors)
            {
                return errors.ToArray();
            }
        }
    }

    // A client of the fake API that records what it reports; a long flush interval unless the test sets one.
    private VexoClient Client(FakeApi api, VexoOptions? options = null)
    {
        options ??= new VexoOptions { FlushInterval = TimeSpan.FromMinutes(1) };
        options.Host = api.Host;
        options.OnError = error =>
        {
            lock (errors)
            {
                errors.Add(error);
            }
        };
        var vexo = new VexoClient("test-key", options);
        clients.Add(vexo);
        return vexo;
    }

    public void Dispose() => clients.ForEach(vexo => vexo.Dispose());

    [Fact]
    public async Task SendsQueuedMessagesAsOneBatchWithTheHeaders()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        vexo.Identify("user_1", new { plan = "pro" });
        vexo.Capture("user_1", "purchase", new Dictionary<string, object?> { ["amount"] = 42 },
            new DateTimeOffset(2026, 11, 7, 12, 0, 0, TimeSpan.Zero));
        await vexo.DisposeAsync();

        Request request = Assert.Single(api.Requests);
        Assert.Equal("/v1/batch", request.Path);
        Assert.Equal("test-key", request.Headers["X-Api-Key"]);
        Assert.Equal("application/json", request.Headers["Content-Type"]);
        Assert.Matches(@"^vexo-dotnet/\d+\.\d+\.\d+$", request.Headers["User-Agent"]);
        JsonElement identify = request.Batch[0], capture = request.Batch[1];
        Assert.Equal("identify", identify.GetProperty("type").GetString());
        Assert.Equal("user_1", identify.GetProperty("distinctId").GetString());
        Assert.Equal("""{"plan":"pro"}""", identify.GetProperty("traits").GetRawText());
        Assert.False(identify.TryGetProperty("timestamp", out _));
        Assert.Equal("capture", capture.GetProperty("type").GetString());
        Assert.Equal("purchase", capture.GetProperty("event").GetString());
        Assert.Equal("""{"amount":42}""", capture.GetProperty("properties").GetRawText());
        Assert.Equal("1794052800000", capture.GetProperty("timestamp").GetRawText());
        Assert.True(Guid.TryParse(request.MessageIds[0], out _));
        Assert.True(Guid.TryParse(request.MessageIds[1], out _));
        Assert.NotEqual(request.MessageIds[0], request.MessageIds[1]);
        Assert.Empty(Errors);
    }

    [Fact]
    public async Task UsesTheGivenMessageIdAndDefaultsTheTimestampToNowInIntegerMilliseconds()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);
        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        vexo.Capture("u", "a", messageId: "order-42-paid");
        vexo.Capture("u", "b");
        await vexo.FlushAsync();

        JsonElement[] batch = Assert.Single(api.Requests).Batch;
        Assert.Equal("order-42-paid", batch[0].GetProperty("messageId").GetString());
        Assert.Equal(JsonValueKind.Number, batch[1].GetProperty("timestamp").ValueKind);
        Assert.DoesNotContain(".", batch[1].GetProperty("timestamp").GetRawText());
        Assert.InRange(batch[1].GetProperty("timestamp").GetInt64(), before, before + 60_000);
    }

    [Fact]
    public async Task NeverThrowsOnBadInputAndReportsEachDroppedMessage()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        vexo.Capture(null!, "a");
        vexo.Capture("u", " ");
        vexo.Capture("u", "not_an_object", 42);
        vexo.Capture("u", "a_list", new[] { 1, 2 });
        vexo.Identify("");
        vexo.Identify("u", "a string");
        await vexo.FlushAsync();

        Assert.Empty(api.Requests);
        Assert.Equal(6, Errors.Length);

        var throwing = new VexoClient("test-key", new VexoOptions { Host = api.Host, OnError = _ => throw new Exception("boom") });
        clients.Add(throwing);
        throwing.Capture("", "a");
    }

    [Fact]
    public void CaptureDoesNotWaitForTheNetwork()
    {
        using var api = new FakeApi(new Reply(Delay: TimeSpan.FromSeconds(2)));
        VexoClient vexo = Client(api, new VexoOptions { FlushAt = 1 });

        var stopwatch = Stopwatch.StartNew();
        for (int i = 0; i < 3; i++)
        {
            vexo.Capture("u", "e" + i);
        }
        vexo.Identify("u", new { plan = "pro" });

        Assert.True(stopwatch.ElapsedMilliseconds < 500, $"Capture took {stopwatch.ElapsedMilliseconds} ms");
    }

    [Fact]
    public async Task SendsDotNetValuesAsJson()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        vexo.Capture("u", "priced", new Dictionary<string, object?>
        {
            ["amount"] = 9.90m,
            ["orderId"] = Guid.Parse("5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71"),
            ["at"] = new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero),
            ["coupon"] = null,
            ["city"] = "São Paulo",
        });
        vexo.Identify("u", new { Plan = "pro" });
        await vexo.FlushAsync();

        JsonElement[] batch = Assert.Single(api.Requests).Batch;
        Assert.Equal(
            """{"amount":9.90,"orderId":"5b0c8f0e-6d1e-4f62-9d55-3f1f0f6a2b71","at":"2026-01-02T00:00:00+00:00","coupon":null,"city":"São Paulo"}""",
            batch[0].GetProperty("properties").GetRawText());
        Assert.Equal("""{"plan":"pro"}""", batch[1].GetProperty("traits").GetRawText());
    }

    private sealed class Cycle
    {
        public Cycle? Self { get; set; }
    }

    [Fact]
    public async Task DropsOnlyTheMessageThatCannotBeSerialized()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);
        var cycle = new Cycle();
        cycle.Self = cycle;

        vexo.Capture("u", "ok");
        vexo.Capture("u", "cycle", cycle);
        vexo.Capture("u", "nan", new { ratio = double.NaN });
        vexo.Capture("u", "unsupported", new { type = typeof(string) });
        vexo.Capture("u", "ok_too");
        await vexo.FlushAsync();

        Assert.Equal(["ok", "ok_too"], Assert.Single(api.Requests).Events);
        Assert.Equal(3, Errors.Length);
        Assert.All(Errors, error => Assert.Contains("JSON", error.Message));
    }

    [Fact]
    public async Task DropsAMessageLargerThan5MB()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        vexo.Capture("u", "huge", new { blob = new string('x', VexoClient.MaxBodyBytes) });
        vexo.Capture("u", "ok");
        await vexo.FlushAsync();

        Assert.Equal(["ok"], Assert.Single(api.Requests).Events);
        Assert.Contains("larger than 5 MB", Assert.Single(Errors).Message);
    }

    [Fact]
    public void FlushesWhenTheQueueReachesFlushAt()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api, new VexoOptions { FlushAt = 2, FlushInterval = TimeSpan.FromMinutes(1) });

        vexo.Capture("u", "a");
        vexo.Capture("u", "b");

        api.WaitForRequests(1);
        Assert.Equal(["a", "b"], api.Requests[0].Events);
    }

    [Fact]
    public void FlushesOnTheInterval()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api, new VexoOptions { FlushInterval = TimeSpan.FromMilliseconds(50) });

        vexo.Capture("u", "a");

        api.WaitForRequests(1);
    }

    [Fact]
    public async Task SplitsBatchesToKeepEachRequestUnder5MB()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);
        string twoMegabytes = new('x', 2_000_000);

        for (int i = 0; i < 3; i++)
        {
            vexo.Capture("u", "e" + i, new { blob = twoMegabytes });
        }
        await vexo.FlushAsync();

        Assert.Equal([2, 1], api.Requests.Select(r => r.Batch.Length));
        Assert.All(api.Requests, r => Assert.InRange(r.BodyBytes, 0, VexoClient.MaxBodyBytes));
        Assert.Empty(Errors);
    }

    [Fact]
    public async Task SendsAtMost500MessagesARequest()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api, new VexoOptions { FlushAt = 1000, FlushInterval = TimeSpan.FromMinutes(1) });

        for (int i = 0; i < 501; i++)
        {
            vexo.Capture("u", "e" + i);
        }
        await vexo.FlushAsync();

        Assert.Equal([500, 1], api.Requests.Select(r => r.Batch.Length));
    }

    [Fact]
    public async Task DropsNewMessagesOnceTheQueueIsFull()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api, new VexoOptions { MaxQueueSize = 1, FlushInterval = TimeSpan.FromMinutes(1) });

        vexo.Capture("u", "kept");
        vexo.Capture("u", "dropped");
        await vexo.FlushAsync();

        Assert.Equal(["kept"], Assert.Single(api.Requests).Events);
        VexoException error = Assert.Single(Errors);
        Assert.Contains("queue is full", error.Message);
        Assert.Contains("\"dropped\"", Assert.Single(error.Batch));
    }

    [Fact]
    public async Task RetriesNetworkErrorsTimeouts503And429WithTheSameMessageIds()
    {
        using var api = new FakeApi(new Reply(Abort: true), new Reply(Delay: TimeSpan.FromSeconds(2)),
            new Reply(503, RetryAfter: "0"), new Reply(429, RetryAfter: "0"));
        VexoClient vexo = Client(api, new VexoOptions
        {
            MaxRetries = 4, Timeout = TimeSpan.FromMilliseconds(300), FlushInterval = TimeSpan.FromMinutes(1),
        });

        vexo.Capture("u", "a");
        vexo.Capture("u", "b");
        await vexo.FlushAsync();

        Assert.Empty(Errors);
        Assert.Equal(5, api.Requests.Length);
        Assert.All(api.Requests, r => Assert.Equal(api.Requests[0].MessageIds, r.MessageIds));
    }

    [Fact]
    public async Task WaitsForRetryAfterBeforeRetrying()
    {
        using var api = new FakeApi(new Reply(429, RetryAfter: "1"));
        VexoClient vexo = Client(api);

        vexo.Capture("u", "a");
        await vexo.FlushAsync();

        Request[] requests = api.Requests;
        Assert.Equal(2, requests.Length);
        Assert.True(Stopwatch.GetElapsedTime(requests[0].Timestamp, requests[1].Timestamp) >= TimeSpan.FromSeconds(0.95));
    }

    [Fact]
    public void ReadsRetryAfterAsSecondsOrADateCappedAt60Seconds()
    {
        static TimeSpan? Delay(RetryConditionHeaderValue? value)
        {
            using var response = new HttpResponseMessage(HttpStatusCode.TooManyRequests);
            response.Headers.RetryAfter = value;
            return VexoClient.RetryDelay(response);
        }

        Assert.Equal(TimeSpan.FromSeconds(3), Delay(new RetryConditionHeaderValue(TimeSpan.FromSeconds(3))));
        Assert.Equal(TimeSpan.FromSeconds(60), Delay(new RetryConditionHeaderValue(TimeSpan.FromHours(1))));
        Assert.Equal(TimeSpan.Zero, Delay(new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddMinutes(-1))));
        Assert.InRange(Delay(new RetryConditionHeaderValue(DateTimeOffset.UtcNow.AddSeconds(30)))!.Value.TotalSeconds, 25, 30);
        Assert.Null(Delay(null));
    }

    [Fact]
    public async Task DoesNotRetryOther4xxAndReportsTheBatch()
    {
        using var api = new FakeApi(new Reply(401));
        VexoClient vexo = Client(api);

        vexo.Capture("u", "a");
        await vexo.FlushAsync();

        Assert.Single(api.Requests);
        VexoException error = Assert.Single(Errors);
        Assert.Equal(HttpStatusCode.Unauthorized, error.StatusCode);
        Assert.Single(error.Batch);
    }

    [Fact]
    public async Task GivesUpAfterMaxRetries()
    {
        using var api = new FakeApi(Enumerable.Repeat(new Reply(500, RetryAfter: "0"), 5).ToArray());
        VexoClient vexo = Client(api, new VexoOptions { MaxRetries = 2, FlushInterval = TimeSpan.FromMinutes(1) });

        vexo.Capture("u", "a");
        await vexo.FlushAsync();

        Assert.Equal(3, api.Requests.Length);
        Assert.Equal(HttpStatusCode.InternalServerError, Assert.Single(Errors).StatusCode);
    }

    [Fact]
    public async Task ResendsOnlyTheMessagesTheApiReportsAsFailed()
    {
        using var api = new FakeApi(new Reply(Data: """{"accepted":1,"dropped":[],"failed":[{"index":1,"reason":"Storing failed; retry this message"}]}"""));
        VexoClient vexo = Client(api);

        vexo.Capture("a", "stored");
        vexo.Capture("b", "failed_once");
        await vexo.FlushAsync();

        Request[] requests = api.Requests;
        Assert.Equal(2, requests.Length);
        Assert.Equal(["failed_once"], requests[1].Events);
        Assert.Equal(requests[0].MessageIds[1], requests[1].MessageIds[0]);
        Assert.Empty(Errors);
    }

    [Fact]
    public async Task ReportsFailedMessagesThatUsedUpTheirRetries()
    {
        using var api = new FakeApi(new Reply(Data: """{"accepted":0,"dropped":[],"failed":[{"index":0,"reason":"Storing failed; retry this message"}]}"""));
        VexoClient vexo = Client(api, new VexoOptions { MaxRetries = 0, FlushInterval = TimeSpan.FromMinutes(1) });

        vexo.Capture("u", "a");
        await vexo.FlushAsync();

        Assert.Single(api.Requests);
        VexoDroppedMessage failed = Assert.Single(Assert.Single(Errors).Dropped);
        Assert.Equal("Storing failed; retry this message", failed.Reason);
    }

    [Fact]
    public async Task CountsA2xxBodyThatIsNotTheEnvelopeAsASuccess()
    {
        using var api = new FakeApi(new Reply(Data: "not json"));
        VexoClient vexo = Client(api);

        vexo.Capture("u", "a");
        await vexo.FlushAsync();

        Assert.Single(api.Requests);
        Assert.Empty(Errors);
    }

    [Fact]
    public async Task ReportsDroppedMessagesWithoutResendingThem()
    {
        using var api = new FakeApi(new Reply(Data: """{"accepted":1,"dropped":[{"index":1,"reason":"event exceeds 255 characters"}],"failed":[]}"""));
        VexoClient vexo = Client(api);

        vexo.Capture("u", "ok");
        vexo.Capture("u", "too_long");
        await vexo.FlushAsync();

        Assert.Single(api.Requests);
        VexoDroppedMessage dropped = Assert.Single(Assert.Single(Errors).Dropped);
        Assert.Equal("event exceeds 255 characters", dropped.Reason);
        Assert.Equal("too_long", JsonDocument.Parse(dropped.Message).RootElement.GetProperty("event").GetString());
    }

    [Fact]
    public async Task GivesLaterBatchesOneAttemptAfterABatchUsesUpItsRetriesUntilARequestSucceeds()
    {
        Reply error = new(500, RetryAfter: "0");
        using var api = new FakeApi(error, error, error, new Reply(), error, new Reply());
        VexoClient vexo = Client(api, new VexoOptions { MaxRetries = 1, FlushInterval = TimeSpan.FromMinutes(1) });

        async Task Send(string eventName, int requests, int errors)
        {
            vexo.Capture("u", eventName);
            await vexo.FlushAsync();
            Assert.Equal(requests, api.Requests.Length);
            Assert.Equal(errors, Errors.Length);
        }

        await Send("uses_up_its_retries", requests: 2, errors: 1);
        await Send("one_attempt", requests: 3, errors: 2);
        await Send("succeeds", requests: 4, errors: 2);
        await Send("retried_again", requests: 6, errors: 2);
    }

    [Fact]
    public async Task FlushAsyncSendsEverythingQueued()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        for (int i = 0; i < 250; i++)
        {
            vexo.Capture("u", "e" + i);
        }
        await vexo.FlushAsync();

        Assert.Equal(250, api.Requests.Sum(r => r.Batch.Length));
        Assert.All(api.Requests, r => Assert.InRange(r.Batch.Length, 1, 100));
    }

    [Fact]
    public void DisposeSendsWhatIsQueuedOnceAndRejectsLaterMessages()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api);

        vexo.Capture("u", "a");
        vexo.Dispose();
        vexo.Dispose();
        vexo.Capture("u", "late");

        Assert.Equal(["a"], Assert.Single(api.Requests).Events);
        Assert.Contains("disposed", Assert.Single(Errors).Message);
    }

    [Fact]
    public async Task ShutdownGivesUpAtTheShutdownTimeoutAndReportsWhatIsLeft()
    {
        using var api = new FakeApi(Enumerable.Repeat(new Reply(Delay: TimeSpan.FromSeconds(30)), 10).ToArray());
        VexoClient vexo = Client(api, new VexoOptions { FlushAt = 1, ShutdownTimeout = TimeSpan.FromSeconds(1) });

        for (int i = 0; i < 5; i++)
        {
            vexo.Capture("u", "e" + i);
        }
        api.WaitForRequests(1);
        var stopwatch = Stopwatch.StartNew();
        await vexo.DisposeAsync();

        Assert.InRange(stopwatch.Elapsed.TotalSeconds, 0.5, 2.5);
        Assert.Equal(5, Errors.Sum(e => e.Batch.Count));
        Assert.Contains(Errors, e => e.Message.Contains("deadline"));
    }

    [Fact]
    public async Task IsSafeForConcurrentCallers()
    {
        using var api = new FakeApi();
        VexoClient vexo = Client(api, new VexoOptions { FlushAt = 50, FlushInterval = TimeSpan.FromMilliseconds(5) });

        await Task.WhenAll(Enumerable.Range(0, 8).Select(t => Task.Run(() =>
        {
            for (int i = 0; i < 250; i++)
            {
                vexo.Capture("user_" + t, "e" + i);
            }
        })));
        await vexo.DisposeAsync();

        Assert.Equal(2000, api.Requests.SelectMany(r => r.MessageIds).Distinct().Count());
        Assert.Equal(2000, api.Requests.Sum(r => r.Batch.Length));
        Assert.All(api.Requests, r => Assert.InRange(r.Batch.Length, 1, 50));
        Assert.Empty(Errors);
    }

    [Fact]
    public void SendsTheQueueWhenTheProcessExits()
    {
        using var api = new FakeApi();

        (TimeSpan _, string stderr) = RunChild(api.Host, shutdownSeconds: 5);

        Assert.Equal(["from_child"], Assert.Single(api.Requests).Events);
        Assert.Equal("", stderr);
    }

    [Fact]
    public void ProcessExitWaitsNoLongerThanTheShutdownTimeout()
    {
        using var api = new FakeApi(new Reply(Delay: TimeSpan.FromSeconds(30)));

        (TimeSpan elapsed, string stderr) = RunChild(api.Host, shutdownSeconds: 1);

        // Process start-up takes part of this; without the deadline the request alone would take its 10 s timeout.
        Assert.InRange(elapsed.TotalSeconds, 1, 6);
        Assert.Contains("[vexo] Batch could not be sent: the shutdown deadline was reached", stderr);
    }

    // Runs Program.Main of this assembly in a new process, which exits without disposing its client.
    private static (TimeSpan Elapsed, string Stderr) RunChild(string host, double shutdownSeconds)
    {
        var start = new ProcessStartInfo("dotnet") { RedirectStandardError = true };
        start.ArgumentList.Add(typeof(Program).Assembly.Location);
        start.ArgumentList.Add(host);
        start.ArgumentList.Add(shutdownSeconds.ToString(CultureInfo.InvariantCulture));
        var stopwatch = Stopwatch.StartNew();
        using Process child = Process.Start(start)!;
        string stderr = child.StandardError.ReadToEnd();
        Assert.True(child.WaitForExit(30_000), "the child process did not exit");
        return (stopwatch.Elapsed, stderr);
    }
}
