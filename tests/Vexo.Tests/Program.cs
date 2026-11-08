using System.Globalization;

namespace Vexo.Tests;

/// <summary>
/// The child process of the exit-hook tests: queues one message and returns from Main without disposing the client.
/// Arguments: the API host and the shutdown timeout in seconds.
/// </summary>
public static class Program
{
    public static void Main(string[] args)
    {
        var vexo = new VexoClient("test-key", new VexoOptions
        {
            Host = args[0],
            ShutdownTimeout = TimeSpan.FromSeconds(double.Parse(args[1], CultureInfo.InvariantCulture)),
        });
        vexo.Capture("child", "from_child");
    }
}
