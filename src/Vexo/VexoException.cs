using System.Net;
using System.Text;

namespace Vexo;

/// <summary>
/// A send failure, or messages that Vexo dropped, reported to <see cref="VexoOptions.OnError"/>. The client never throws
/// it from <see cref="VexoClient.Capture"/> or <see cref="VexoClient.Identify"/>.
/// </summary>
public sealed class VexoException : Exception
{
    internal VexoException(string message, IEnumerable<byte[]>? batch = null, HttpStatusCode? statusCode = null,
        IReadOnlyList<VexoDroppedMessage>? dropped = null, Exception? innerException = null)
        : base(message, innerException)
    {
        Batch = batch?.Select(json => Encoding.UTF8.GetString(json)).ToArray() ?? [];
        StatusCode = statusCode;
        Dropped = dropped ?? [];
    }

    /// <summary>The messages that weren't sent, as JSON. Empty when the error isn't about a batch.</summary>
    public IReadOnlyList<string> Batch { get; }

    /// <summary>The HTTP status of a rejected batch; null for network errors and for messages dropped before sending.</summary>
    public HttpStatusCode? StatusCode { get; }

    /// <summary>The messages that the API dropped or couldn't store, with the reason.</summary>
    public IReadOnlyList<VexoDroppedMessage> Dropped { get; }
}

/// <summary>A message that the API dropped or couldn't store.</summary>
/// <param name="Message">The message, as JSON.</param>
/// <param name="Reason">Why the API didn't store it.</param>
public sealed record VexoDroppedMessage(string Message, string Reason);
