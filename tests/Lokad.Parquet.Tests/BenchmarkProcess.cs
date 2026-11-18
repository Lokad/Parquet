using System.Diagnostics;

namespace Lokad.Parquet.Tests;

[CollectionDefinition("Benchmark integration", DisableParallelization = true)]
public sealed class BenchmarkIntegrationCollection;

internal static class BenchmarkProcess
{
    internal static async Task<(int ExitCode, string Output)> RunAsync(string executable, IEnumerable<string> arguments)
    {
        var start = new ProcessStartInfo(executable)
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true,
        };
        foreach (var argument in arguments)
            start.ArgumentList.Add(argument);

        using var process = Process.Start(start) ?? throw new InvalidOperationException($"Could not start {executable}.");
        var stdout = process.StandardOutput.ReadToEndAsync();
        var stderr = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        try
        {
            await process.WaitForExitAsync(timeout.Token);
        }
        catch (OperationCanceledException)
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            await process.WaitForExitAsync();
            throw new TimeoutException($"{executable} did not exit.");
        }
        return (process.ExitCode, (await stdout) + (await stderr));
    }
}
