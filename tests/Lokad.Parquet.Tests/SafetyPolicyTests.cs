using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Reflection.Emit;
using System.Text.RegularExpressions;

namespace Lokad.Parquet.Tests;

public sealed class SafetyPolicyTests
{
    private static readonly string[] ForbiddenTypeNames =
    [
        "System.Diagnostics.Process",
        "System.Net.Http.HttpClient",
        "System.Net.WebClient",
        "System.Reflection.Assembly",
        "System.Runtime.CompilerServices.Unsafe",
        "System.Runtime.InteropServices.CollectionsMarshal",
        "System.Runtime.InteropServices.DllImportAttribute",
        "System.Runtime.InteropServices.GCHandle",
        "System.Runtime.InteropServices.LibraryImportAttribute",
        "System.Runtime.InteropServices.Marshal",
        "System.Runtime.InteropServices.MemoryMarshal",
        "System.Runtime.InteropServices.NativeLibrary",
        "System.Runtime.InteropServices.NativeMemory",
    ];

    private static readonly Regex ForbiddenSourceTokens = new(
        @"\bunsafe\b|\bfixed\s*\(|\bGCHandle\b|\bUnsafe\b|\bMemoryMarshal\b|\bCollectionsMarshal\b|\bMarshal\b|\bNativeMemory\b|\bNativeLibrary\b|\bDllImport\b|\bLibraryImport\b|\bProcess\.Start\b|\bHttpClient\b|\bWebClient\b|\bAssembly\.Load\b|\bDynamicMethod\b|\.Compile\s*\(",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    private static readonly Regex NullForgivingTokens = new(
        @"(?:[A-Za-z0-9_\)\]])\x21(?!=)",
        RegexOptions.CultureInvariant);

    [Fact]
    public void ShippedSourcesContainNoForbiddenMemoryAccessApi()
    {
        var sourceRoot = Path.Combine(RepositoryTestPaths.Root, "src", "Lokad.Parquet");
        var violations = new List<string>();
        foreach (var path in Directory.EnumerateFiles(sourceRoot, "*.cs", SearchOption.AllDirectories))
        {
            var text = File.ReadAllText(path);
            foreach (Match match in ForbiddenSourceTokens.Matches(text))
                violations.Add($"{Path.GetRelativePath(RepositoryTestPaths.Root, path)}: {match.Value}");
        }

        Assert.True(violations.Count == 0, "Forbidden shipped source tokens:\n" + string.Join('\n', violations));
    }

    [Fact]
    public void ShippedAssemblyContainsNoForbiddenReferencesOrUnverifiableSignatures()
    {
        var assembly = typeof(ParquetFile).Assembly;
        var violations = new List<string>();

        using (var stream = File.OpenRead(assembly.Location))
        using (var peReader = new PEReader(stream))
        {
            var metadata = peReader.GetMetadataReader();
            foreach (var handle in metadata.TypeReferences)
            {
                var reference = metadata.GetTypeReference(handle);
                var fullName = metadata.GetString(reference.Namespace) + "." + metadata.GetString(reference.Name);
                if (ForbiddenTypeNames.Contains(fullName, StringComparer.Ordinal) ||
                    fullName.StartsWith("System.Reflection.Emit.", StringComparison.Ordinal))
                    violations.Add("reference " + fullName);
            }
        }

        foreach (var type in assembly.GetTypes())
        {
            if (type.IsImport)
                violations.Add("COM import type " + type.FullName);
            if (ContainsPointer(type))
                violations.Add("pointer type " + type.FullName);
            foreach (var field in type.GetFields(AllDeclaredMembers))
            {
                if (ContainsPointer(field.FieldType))
                    violations.Add("pointer field " + field);
            }
            foreach (var method in type.GetMethods(AllDeclaredMembers).Cast<MethodBase>()
                         .Concat(type.GetConstructors(AllDeclaredMembers)))
            {
                if ((method.Attributes & MethodAttributes.PinvokeImpl) != 0)
                    violations.Add("P/Invoke method " + method);
                if (method is MethodInfo methodInfo && ContainsPointer(methodInfo.ReturnType))
                    violations.Add("pointer return " + method);
                if (method.GetParameters().Any(static parameter => ContainsPointer(parameter.ParameterType)))
                    violations.Add("pointer parameter " + method);
                var body = method.GetMethodBody();
                if (body is not null && body.LocalVariables.Any(static local => local.IsPinned || ContainsPointer(local.LocalType)))
                    violations.Add("pinned or pointer local " + method);
            }
        }

        Assert.True(violations.Count == 0, "Forbidden shipped IL:\n" + string.Join('\n', violations));
    }

    [Fact]
    public void ShippedAssemblyHasOnlyFrameworkDependencies()
    {
        var unexpected = typeof(ParquetFile).Assembly.GetReferencedAssemblies()
            .Select(static reference => reference.Name ?? "")
            .Where(static name => name != "mscorlib" && name != "netstandard" &&
                !name.StartsWith("System", StringComparison.Ordinal) &&
                !name.StartsWith("Microsoft.Win32", StringComparison.Ordinal))
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Empty(unexpected);
    }

    [Fact]
    public void SolutionCodeMeetsMechanicalCSharpGuidelines()
    {
        var violations = new List<string>();
        var testAssembly = typeof(SafetyPolicyTests).Assembly;
        var testOutput = Path.GetDirectoryName(testAssembly.Location) ??
            throw new InvalidOperationException("The test assembly has no output directory.");
        var framework = Path.GetFileName(testOutput);
        var configuration = Directory.GetParent(testOutput)?.Name ??
            throw new InvalidOperationException("The test assembly has no configuration directory.");
        var benchmarkAssembly = Path.Combine(
            RepositoryTestPaths.Root,
            "bench",
            "Lokad.Parquet.Benchmarks",
            "bin",
            configuration,
            framework,
            "Lokad.Parquet.Benchmarks.dll");
        var assemblyPaths = new[] { typeof(ParquetFile).Assembly.Location, testAssembly.Location, benchmarkAssembly };
        foreach (var path in assemblyPaths)
        {
            if (!File.Exists(path))
            {
                violations.Add("missing solution assembly " + Path.GetRelativePath(RepositoryTestPaths.Root, path));
                continue;
            }

            using var stream = File.OpenRead(path);
            using var peReader = new PEReader(stream);
            var metadata = peReader.GetMetadataReader();
            foreach (var methodHandle in metadata.MethodDefinitions)
            {
                var method = metadata.GetMethodDefinition(methodHandle);
                foreach (var parameterHandle in method.GetParameters())
                {
                    var parameter = metadata.GetParameter(parameterHandle);
                    if (parameter.SequenceNumber != 0 &&
                        (parameter.Attributes & (ParameterAttributes.Optional | ParameterAttributes.HasDefault)) != 0)
                    {
                        violations.Add(
                            $"optional parameter in {Path.GetFileName(path)}: " +
                            metadata.GetString(method.Name) + "(" + metadata.GetString(parameter.Name) + ")");
                    }
                }
            }
        }

        var sourceFiles = new[] { "src", "tests", "bench" }
            .SelectMany(sourceFolder => Directory.EnumerateFiles(
                Path.Combine(RepositoryTestPaths.Root, sourceFolder),
                "*.cs",
                SearchOption.AllDirectories))
            .Where(static path =>
                !path.Split(Path.DirectorySeparatorChar).Contains("obj", StringComparer.OrdinalIgnoreCase))
            .Select(static path => (Path: path, Source: File.ReadAllText(path)))
            .ToArray();
        foreach (var (path, source) in sourceFiles)
        {
            var friendAssemblyToken = "Internals" + "VisibleTo";
            if (source.Contains(friendAssemblyToken, StringComparison.Ordinal))
                violations.Add("friend assembly declaration in " + Path.GetRelativePath(RepositoryTestPaths.Root, path));
            foreach (Match match in NullForgivingTokens.Matches(source))
                violations.Add($"null-forgiving token in {Path.GetRelativePath(RepositoryTestPaths.Root, path)}: {match.Value}");
        }

        var componentEntryPoints = new Dictionary<string, string>(StringComparer.Ordinal)
        {
            ["Lokad.Parquet.Internal.PageHeaderParser.TryParse"] =
                "cohesive bounded page-header parser entry point",
            ["Lokad.Parquet.Internal.ParquetFooterParser.Parse"] =
                "cohesive bounded footer parser entry point",
            ["Lokad.Parquet.Internal.ParquetScanMemoryBudget.Release"] =
                "paired reservation-accounting component operation",
            ["Lokad.Parquet.Internal.PooledArrayOwnerCache`1.Return"] =
                "owner-to-cache transfer operation across the ownership boundary",
            ["Lokad.Parquet.Internal.RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap"] =
                "independently tested optimized decoder entry point",
            ["Lokad.Parquet.Internal.ThriftCompactReader.ReadSByte"] =
                "compact-wire reader primitive used by the schema parser",
            ["Lokad.Parquet.Internal.ThriftCompactReader.ReadBinary"] =
                "compact-wire reader primitive used by the schema parser",
            ["Lokad.Parquet.Tests.DecoderKernelTests.GetMethod"] =
                "reflection bridge for testing internal decoders without a friend assembly",
            ["Lokad.Parquet.Tests.OwnershipAndMutationTests+ConcurrentRecordingSource.ResetPeak"] =
                "explicit measurement boundary on a sequential-I/O test double",
            ["Lokad.Parquet.Benchmarks.PairedParityRunner.RunAsync"] =
                "command-line runner entry point",
            ["Lokad.Parquet.Benchmarks.PairedParityRunner.MeasureAsync"] =
                "cohesive randomized paired-measurement pipeline",
            ["Lokad.Parquet.Benchmarks.ParityScanCase.MaterializeLokadAsync"] =
                "independent Lokad materialization benchmark operation",
            ["Lokad.Parquet.Benchmarks.ParityScanCase.MaterializeParquetNetAsync"] =
                "independent Parquet.NET materialization benchmark operation",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.RunAsync"] =
                "command-line runner entry point",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner+CountingMemoryStream.Reset"] =
                "explicit source-measurement boundary",
        };
        var opCodes = typeof(OpCodes).GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(static field => field.FieldType == typeof(OpCode))
            .Select(static field => (OpCode)(field.GetValue(null) ?? throw new InvalidOperationException("An IL opcode is missing.")))
            .ToDictionary(static code => unchecked((ushort)code.Value));
        var solutionAssemblies = assemblyPaths.Select(path =>
                string.Equals(path, testAssembly.Location, StringComparison.OrdinalIgnoreCase)
                    ? testAssembly
                    : Assembly.LoadFrom(path))
            .ToArray();
        var solutionMethods = solutionAssemblies
            .SelectMany(static assembly => assembly.GetTypes())
            .SelectMany(static type => type.GetMethods(AllDeclaredMembers).Cast<MethodBase>()
                .Concat(type.GetConstructors(AllDeclaredMembers)))
            .ToArray();
        var solutionMethodIdentities = solutionMethods
            .Where(static method => method.DeclaringType is not null)
            .Select(static method => method.DeclaringType?.FullName + "." + method.Name)
            .ToHashSet(StringComparer.Ordinal);
        foreach (var exception in componentEntryPoints.Keys)
        {
            if (!solutionMethodIdentities.Contains(exception))
                violations.Add("stale helper-locality exception: " + exception);
        }
        var callers = new Dictionary<(Guid Module, int Token), HashSet<(Guid Module, int Token)>>();
        foreach (var caller in solutionMethods)
        {
            var body = caller.GetMethodBody();
            var bytes = body?.GetILAsByteArray();
            if (bytes is null)
                continue;
            var position = 0;
            while (position < bytes.Length)
            {
                var first = bytes[position++];
                var value = first == 0xFE
                    ? (ushort)(0xFE00 | bytes[position++])
                    : first;
                var opCode = opCodes[value];
                if (opCode.OperandType == OperandType.InlineMethod)
                {
                    var token = BitConverter.ToInt32(bytes, position);
                    try
                    {
                        var called = caller.Module.ResolveMethod(
                            token,
                            caller.DeclaringType?.GetGenericArguments(),
                            caller is MethodInfo { IsGenericMethod: true } genericCaller
                                ? genericCaller.GetGenericArguments()
                                : null);
                        if (called is MethodInfo { IsGenericMethod: true } genericCalled)
                            called = genericCalled.GetGenericMethodDefinition();
                        if (called is not null)
                        {
                            var calledKey = (called.Module.ModuleVersionId, called.MetadataToken);
                            var callerKey = (caller.Module.ModuleVersionId, caller.MetadataToken);
                            if (!callers.TryGetValue(calledKey, out var distinctCallers))
                            {
                                distinctCallers = [];
                                callers.Add(calledKey, distinctCallers);
                            }
                            distinctCallers.Add(callerKey);
                        }
                    }
                    catch (ArgumentException)
                    {
                    }
                }

                position += opCode.OperandType switch
                {
                    OperandType.InlineNone => 0,
                    OperandType.ShortInlineBrTarget or OperandType.ShortInlineI or OperandType.ShortInlineVar => 1,
                    OperandType.InlineVar => 2,
                    OperandType.InlineBrTarget or OperandType.InlineField or OperandType.InlineI or
                        OperandType.InlineMethod or OperandType.InlineSig or OperandType.InlineString or
                        OperandType.InlineTok or OperandType.InlineType or OperandType.ShortInlineR => 4,
                    OperandType.InlineI8 or OperandType.InlineR => 8,
                    OperandType.InlineSwitch => checked(sizeof(int) +
                        sizeof(int) * BitConverter.ToInt32(bytes, position)),
                    _ => throw new InvalidOperationException("An unsupported IL operand type was encountered."),
                };
            }
        }

        foreach (var method in solutionMethods.OfType<MethodInfo>())
        {
            static bool IsEffectivelyPublic(Type type)
            {
                for (var current = type; current is not null; current = current.DeclaringType)
                {
                    if (current.DeclaringType is null ? !current.IsPublic : !current.IsNestedPublic)
                        return false;
                }
                return true;
            }

            var declaringType = method.DeclaringType ??
                throw new InvalidOperationException("A solution method has no declaring type.");
            if (method.IsSpecialName || method.IsAbstract || method.IsVirtual ||
                solutionAssemblies.Any(assembly => assembly.EntryPoint == method) ||
                method.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), false) ||
                declaringType.IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute), false) ||
                method.IsPublic && IsEffectivelyPublic(declaringType))
                continue;
            var identity = declaringType.FullName + "." + method.Name;
            if (componentEntryPoints.ContainsKey(identity))
                continue;
            var methodKey = (method.Module.ModuleVersionId, method.MetadataToken);
            var callerCount = callers.TryGetValue(methodKey, out var distinctCallers)
                ? distinctCallers.Count
                : 0;
            if (callerCount <= 1)
                violations.Add($"effectively non-public helper has {callerCount} caller(s): {identity}");
        }

        Assert.True(violations.Count == 0, "C# guideline violations:\n" + string.Join('\n', violations));
    }

    [Fact]
    public void PublicProjectsDoNotReferenceIgnoredExternalSources()
    {
        var projectFiles = Directory.EnumerateFiles(RepositoryTestPaths.Root, "*.csproj", SearchOption.AllDirectories)
            .Where(static path => !path.Split(Path.DirectorySeparatorChar).Contains("external", StringComparer.OrdinalIgnoreCase));
        var violations = projectFiles
            .Where(path => File.ReadAllText(path).Contains("external", StringComparison.OrdinalIgnoreCase))
            .Select(path => Path.GetRelativePath(RepositoryTestPaths.Root, path))
            .ToArray();

        Assert.Empty(violations);
    }

    [Fact]
    public void ShippedProjectDeclaresNoPackageDependency()
    {
        var project = File.ReadAllText(Path.Combine(
            RepositoryTestPaths.Root,
            "src",
            "Lokad.Parquet",
            "Lokad.Parquet.csproj"));

        Assert.DoesNotContain("PackageReference", project, StringComparison.Ordinal);
    }

    private const BindingFlags AllDeclaredMembers = BindingFlags.Public | BindingFlags.NonPublic |
        BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static bool ContainsPointer(Type type) => type.IsPointer ||
        (type.GetElementType() is Type elementType && ContainsPointer(elementType)) ||
        (type.IsGenericType && type.GetGenericArguments().Any(ContainsPointer));

}
