using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace Vexo.Tests;

/// <summary>One scripted answer of <see cref="FakeApi"/>. Abort drops the connection in the middle of the response.</summary>
public sealed record Reply(int Status = 200, string? Data = null, string? RetryAfter = null, TimeSpan Delay = default,
    bool Abort = false);

public sealed record Request(string Path, NameValueCollection Headers, int BodyBytes, JsonElement[] Batch, long Timestamp)
{
    public string[] Events => Batch.Select(m => m.GetProperty("event").GetString()!).ToArray();

    public string[] MessageIds => Batch.Select(m => m.GetProperty("messageId").GetString()!).ToArray();
}

/// <summary>A local API on a real socket that records each request and answers with the next scripted reply.</summary>
public sealed class FakeApi : IDisposable
{
    private readonly HttpListener listener = new();
    private readonly ConcurrentQueue<Reply> replies;
    private readonly List<Request> requests = new();
    private readonly CancellationTokenSource closed = new();

    public FakeApi(params Reply[] replies)
    {
        this.replies = new ConcurrentQueue<Reply>(replies);
        Host = $"http://127.0.0.1:{FreePort()}";
        listener.Prefixes.Add(Host + "/");
        listener.Start();
        _ = Task.Run(ServeAsync);
    }

    public string Host { get; }

    public Request[] Requests
    {
        get
        {
            lock (requests)
            {
                return requests.ToArray();
            }
        }
    }

    public void WaitForRequests(int count, double seconds = 5)
    {
        var stopwatch = Stopwatch.StartNew();
        while (Requests.Length < count && stopwatch.Elapsed.TotalSeconds < seconds)
        {
            Thread.Sleep(10);
        }
        Assert.Equal(count, Requests.Length);
    }

    public void Dispose()
    {
        closed.Cancel();
        listener.Close();
    }

    private async Task ServeAsync()
    {
        while (!closed.IsCancellationRequested)
        {
            HttpListenerContext context;
            try
            {
                context = await listener.GetContextAsync();
            }
            catch (Exception) when (closed.IsCancellationRequested)
            {
                return;
            }
            _ = Task.Run(() => AnswerAsync(context));
        }
    }

    private async Task AnswerAsync(HttpListenerContext context)
    {
        try
        {
            using var body = new MemoryStream();
            await context.Request.InputStream.CopyToAsync(body);
            using JsonDocument json = JsonDocument.Parse(body.ToArray());
            JsonElement[] batch = json.RootElement.GetProperty("batch").EnumerateArray().Select(m => m.Clone()).ToArray();
            lock (requests)
            {
                requests.Add(new Request(context.Request.Url!.AbsolutePath, context.Request.Headers, (int)body.Length,
                    batch, Stopwatch.GetTimestamp()));
            }

            Reply reply = replies.TryDequeue(out Reply? next) ? next : new Reply();
            await Task.Delay(reply.Delay, closed.Token);
            if (reply.Abort)
            {
                // Promises a body, sends part of it, and drops the connection.
                context.Response.ContentLength64 = 100;
                await context.Response.OutputStream.WriteAsync("{"u8.ToArray());
                context.Response.Abort();
                return;
            }
            context.Response.StatusCode = reply.Status;
            context.Response.ContentType = "application/json";
            if (reply.RetryAfter is not null)
            {
                context.Response.Headers["Retry-After"] = reply.RetryAfter;
            }
            string data = reply.Data ?? $$"""{"accepted":{{batch.Length}},"dropped":[],"failed":[]}""";
            bool ok = reply.Status < 300;
            byte[] answer = Encoding.UTF8.GetBytes(
                $$"""{"success":{{(ok ? "true" : "false")}},"code":{{reply.Status}},"message":null,"data":{{(ok ? data : "null")}}}""");
            await context.Response.OutputStream.WriteAsync(answer);
            context.Response.Close();
        }
        catch (Exception)
        {
            // The client went away (a timeout or the shutdown deadline), or the test is over.
        }
    }

    private static int FreePort()
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        int port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        return port;
    }
}
