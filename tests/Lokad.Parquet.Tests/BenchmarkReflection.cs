using System.Reflection;

namespace Lokad.Parquet.Tests;

// Shared benchmark-assembly reflection plumbing for the benchmark tests:
// assembly discovery, type and static-method lookup, invocation, and task
// unwrapping with missing-member diagnostics. Test logic keeps its own
// expected values, fixture bytes, failure cases and property reads; only
// the lookup and invocation mechanics live here. No shipped test APIs are
// added and pool observation plumbing stays where it is.
internal static class BenchmarkReflection
{
    private const BindingFlags StaticMembers =
        BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;

    internal static Assembly BenchmarkAssembly()
    {
        var testOutput = Path.GetDirectoryName(typeof(BenchmarkReflection).Assembly.Location) ??
            throw new InvalidOperationException("The test assembly has no output directory.");
        var framework = Path.GetFileName(testOutput);
        var configuration = Directory.GetParent(testOutput)?.Name ??
            throw new InvalidOperationException("The test assembly has no configuration directory.");
        return Assembly.LoadFrom(Path.Combine(
            RepositoryTestPaths.Root,
            "bench",
            "Lokad.Parquet.Benchmarks",
            "bin",
            configuration,
            framework,
            "Lokad.Parquet.Benchmarks.dll"));
    }

    internal static Type RequireType(Assembly assembly, string fullName)
    {
        return assembly.GetType(fullName) ??
            throw new InvalidOperationException($"The benchmark {fullName} type is unavailable.");
    }

    internal static MethodInfo RequireStaticMethod(Type type, string name, Type[]? parameterTypes)
    {
        var method = parameterTypes is null
            ? type.GetMethod(name, StaticMembers)
            : type.GetMethod(name, StaticMembers, parameterTypes);
        return method ??
            throw new InvalidOperationException($"The benchmark {type.FullName}.{name} method is unavailable.");
    }

    internal static object? InvokeStatic(Assembly assembly, string typeName, string methodName, params object?[] args)
    {
        var method = RequireStaticMethod(RequireType(assembly, typeName), methodName, null);
        return method.Invoke(null, args);
    }

    // Task<T> is invariant, so the result is read through the completed task
    // instead of casting, exactly like the previous per-site extraction.
    internal static async Task<object?> InvokeAsync(Assembly assembly, string typeName, string methodName, params object?[] args)
    {
        var method = RequireStaticMethod(RequireType(assembly, typeName), methodName, null);
        var task = (Task)(method.Invoke(null, args) ??
            throw new InvalidOperationException($"The benchmark {typeName}.{methodName} returned nothing."));
        await task.ConfigureAwait(false);
        return task.GetType().GetProperty("Result")?.GetValue(task);
    }
}
