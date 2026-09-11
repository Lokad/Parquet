using System.Reflection;
using System.Text;
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
        // Textual guardrail only: comments and string literals are stripped so prose
        // mentioning an API cannot fail the build, and generated obj/bin outputs are
        // excluded. The IL assembly check below remains the authoritative
        // memory-safety gate.
        var sourceRoot = Path.Combine(RepositoryTestPaths.Root, "src", "Lokad.Parquet");
        var violations = new List<string>();
        var scanned = new List<string>();
        foreach (var path in Directory.EnumerateFiles(sourceRoot, "*.cs", SearchOption.AllDirectories))
        {
            if (IsGeneratedOutputPath(path))
                continue;
            scanned.Add(path);
            var text = StripCommentsAndStrings(File.ReadAllText(path));
            foreach (Match match in ForbiddenSourceTokens.Matches(text))
                violations.Add($"{Path.GetRelativePath(RepositoryTestPaths.Root, path)}: {match.Value}");
        }

        Assert.DoesNotContain(scanned, static path => IsGeneratedOutputPath(path));
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
            .Where(static path => !IsGeneratedOutputPath(path))
            .Select(static path => (Path: path, Source: File.ReadAllText(path)))
            .ToArray();
        foreach (var (path, source) in sourceFiles)
        {
            // Both textual checks inspect code only; prose in comments and string
            // literals cannot violate them.
            var code = StripCommentsAndStrings(source);
            var friendAssemblyToken = "Internals" + "VisibleTo";
            if (code.Contains(friendAssemblyToken, StringComparison.Ordinal))
                violations.Add("friend assembly declaration in " + Path.GetRelativePath(RepositoryTestPaths.Root, path));
            foreach (Match match in NullForgivingTokens.Matches(code))
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
            ["Lokad.Parquet.Internal.PooledArrayOwnerCache`1.EvictIdle"] =
                "idle-storage eviction across the file/cache ownership boundary",
            ["Lokad.Parquet.Internal.PagePayloadLease.SetOwned"] =
                "owned payload transfer into the page lease across the pool/page ownership boundary",
            ["Lokad.Parquet.Internal.PagePayloadLease.SetBorrowed"] =
                "borrowed payload transfer into the page lease across the file/page ownership boundary",
            ["Lokad.Parquet.Internal.PageValidityState.SetAllValid"] =
                "implicit all-valid transition for required and dropped-bitmap pages",
            ["Lokad.Parquet.Internal.PageValidityState.Dispose"] =
                "page-validity ownership release across the page/batch ownership boundary",
            ["Lokad.Parquet.Internal.ScanPageReader.ComputeCrc32"] =
                "page-integrity CRC across the I/O/decode ownership boundary",
            ["Lokad.Parquet.Internal.ScanPageReader.TryGetSourceMemory"] =
                "retained-memory borrow across the source/page ownership boundary",
            ["Lokad.Parquet.Internal.ScanPageReader.ReadPageHeaderAsync"] =
                "cohesive bounded page-header I/O entry point",
            ["Lokad.Parquet.Internal.ScanPageReader.ReadExactlyAsync"] =
                "exact source-read with truncation translation across the source/scan boundary",
            ["Lokad.Parquet.Benchmarks.BenchmarkEnvironment.EnsureNativeWorkspace"] =
                "benchmark workspace qualification entry point",
            ["Lokad.Parquet.ParquetErrorLocation.AtRowGroup"] =
                "row-group error scope with a single row-group-level use",
            ["Lokad.Parquet.ParquetErrorLocation.AtRowGroupColumnPage"] =
                "offset-less page error scope with a single batch-limit use",
            ["Lokad.Parquet.Internal.RleBitPackedHybridDecoder.DecodeBitWidthOneToBitmap"] =
                "independently tested optimized decoder entry point",
            ["Lokad.Parquet.Internal.ThriftCompactReader.ReadSByte"] =
                "compact-wire reader primitive used by the schema parser",
            ["Lokad.Parquet.Internal.ThriftCompactReader.ReadBinary"] =
                "compact-wire reader primitive used by the schema parser",
            ["Lokad.Parquet.Tests.DecoderKernelTests.GetMethod"] =
                "reflection bridge for testing internal decoders without a friend assembly",
            ["Lokad.Parquet.Tests.ScanCancellationTests.GetCrc32Method"] =
                "reflection bridge for testing the internal CRC helper without a friend assembly",
            ["Lokad.Parquet.Tests.ScalarTestMode.Initialize"] =
                "process-wide forced-scalar test mode for the CI scalar leg",
            ["Lokad.Parquet.Tests.OwnershipAndMutationTests+ConcurrentRecordingSource.ResetPeak"] =
                "explicit measurement boundary on a sequential-I/O test double",
            ["Lokad.Parquet.Benchmarks.PairedParityRunner.RunAsync"] =
                "command-line runner entry point",
            ["Lokad.Parquet.Benchmarks.PairedParityRunner.WriteCatalogAsync"] =
                "parity catalog export behind the command-line boundary",
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
            ["Lokad.Parquet.Internal.ParquetScanEnumerable.PreflightSelectedChunks"] =
                "cohesive selected-chunk preflight planning entry point",
            ["Lokad.Parquet.Tests.BenchmarkReportQuartet.DiagnosticCensusCase"] =
                "synthetic frozen census-case builder with one fixture-method caller",
            ["Lokad.Parquet.Tests.OpenFailureTests+PendingFailingSource.Fail"] =
                "explicit failure-trigger boundary on a pending-read test double",
            ["Lokad.Parquet.Tests.OpenFailureTests+GatedDisposalLengthStream.CompleteDisposal"] =
                "explicit disposal-gate boundary on a pending-cleanup test double",
            ["Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateInt32"] =
                "independent fixture writer shared across the test/benchmark boundary",
            ["Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateRequiredInt32Columns"] =
                "independent multi-column fixture writer shared across the test/benchmark boundary",
            ["Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateBinaryColumns"] =
                "independent binary fixture writer shared across the test/benchmark boundary",
            ["Lokad.Parquet.Tests.CompactTestWriter.BinaryField"] =
                "raw binary field writer for synthetic statistics extrema with one fixture caller",
            ["Lokad.Parquet.Tests.ParquetFixtureBuilder.EncodeSnappyLiteral"] =
                "literal-only test Snappy encoder behind the single copy-encoding switch",
            ["Lokad.Parquet.Tests.ParquetFixtureBuilder.CreateRequiredInt32RowGroups"] =
                "independent multi-row-group fixture writer shared across the test/benchmark boundary",
            ["Lokad.Parquet.Benchmarks.CensusCaseLayout.ForCatalogLane"] =
                "frozen-catalog workload-to-layout derivation for the work census",
            ["Lokad.Parquet.Benchmarks.LiveSessionRetention.ScanBaselineSessionAsync"] =
                "live baseline session scan across the retention observation boundary",
            ["Lokad.Parquet.Benchmarks.LiveSessionRetention+BaselineDestinations.Create"] =
                "reusable baseline destination allocation for the live retention session",
            ["Lokad.Parquet.Benchmarks.BenchmarkScan.ReadBaselineFileAsync"] =
                "independent Parquet.NET file source benchmark operation",
            ["Lokad.Parquet.Benchmarks.BenchmarkScan.ReadBaselineStreamAsync"] =
                "independent Parquet.NET stream source benchmark operation",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.EstablishCommittedTruthAsync"] =
                "committed-fixture truth oracle across the checksum/time boundary",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.DecodeNullablePrimitive"] =
                "shared nullable primitive decode helper across the census consumer branches",
            ["Lokad.Parquet.Benchmarks.DiagnosticPairedCases.CreateAsync"] =
                "optional diagnostic paired-case entry point outside the frozen claim",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.BeginCensusSession"] =
                "census session directory and running marker behind the census boundary",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.RecordCensusCheckpoint"] =
                "per-case census checkpoint behind the census measurement boundary",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.CompleteCensusSession"] =
                "census snapshot binding and completion marker behind the census boundary",
            ["Lokad.Parquet.Benchmarks.WorkCensusRunner.AbortCensusSession"] =
                "census failure marker behind the census abort boundary",
            ["Lokad.Parquet.Benchmarks.PairedSessionRecorder.FindIncompleteSessionDirectories"] =
                "incomplete paired-session discovery for concurrent-campaign detection",
            ["Lokad.Parquet.Internal.PooledValueLease.TryTake"] =
                "typed page-value transfer shared by the primitive batch handoff",
            ["Lokad.Parquet.Internal.PooledValueLease.TryGetValues"] =
                "typed page-value access shared by partial batch copies",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.DecodePage"] =
                "row-group dictionary page decoding behind the scan cursor",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.ExpandDataPage"] =
                "dictionary-encoded data page expansion behind the scan cursor",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.Reset"] =
                "row-group dictionary store release on page progression",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.ExpandBinaryDictionary"] =
                "variable-width dictionary expansion for one physical layout",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.ExpandFixedDictionary"] =
                "fixed-width dictionary expansion for one physical layout",
            ["Lokad.Parquet.Internal.ScanDictionaryDecoder.ExpandValues"] =
                "primitive dictionary expansion shared by the five fixed-width layouts",
            ["Lokad.Parquet.Internal.RleBitPackedHybridDecoder.Decode"] =
                "RLE/bit-packed hybrid index decoder behind the dictionary expansion boundary",
            ["Lokad.Parquet.ParquetFile+ColumnCacheProvider.RentColumnValues"] =
                "typed column-cache rent behind the file provider boundary",
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

    [Fact]
    public void ForbiddenTokenMatcherIgnoresCommentsAndStrings()
    {
        // Without stripping, each raw sample matches: the stripped form must not.
        Assert.NotEmpty(ForbiddenSourceTokens.Matches("// do not use unsafe code here"));
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("// do not use unsafe code here\nvar x = 1;")));
        Assert.NotEmpty(ForbiddenSourceTokens.Matches("/* Unsafe, fixed (, Marshal */"));
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("/* Unsafe, fixed (, Marshal */\nvar x = 1;")));
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var s = \"MemoryMarshal and GCHandle\";\nvar x = 1;")));
        // Negative controls: real code still matches after stripping.
        Assert.NotEmpty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("fixed (byte* p = buffer) { }")));
        Assert.NotEmpty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var h = GCHandle.Alloc(x);")));
        // Known limit: interpolation holes are skipped as string content, so a token
        // inside a hole stays invisible to this textual check. The IL assembly check
        // above remains the authoritative memory-safety gate.
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var s = $\"{value!}\";")));
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var s = $@\"{value!}\";")));
        Assert.Empty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var s = $$\"\"\"{value!}\"\"\";")));
        // Holes do not swallow surrounding code: a token outside the literal matches.
        Assert.NotEmpty(ForbiddenSourceTokens.Matches(StripCommentsAndStrings("var s = $\"{x}\";\nvar h = GCHandle.Alloc(x);")));
    }

    [Fact]
    public void NullForgivingMatcherIgnoresCommentsAndStrings()
    {
        Assert.NotEmpty(NullForgivingTokens.Matches("// retry!\n"));
        Assert.Empty(NullForgivingTokens.Matches(StripCommentsAndStrings("// retry! do not fail!\nvar x = 1;")));
        Assert.Empty(NullForgivingTokens.Matches(StripCommentsAndStrings("var s = \"a!b\";\nvar x = 1;")));
        Assert.NotEmpty(NullForgivingTokens.Matches(StripCommentsAndStrings("var x = value!;")));
        // Known limit: a null-forgiving operator inside an interpolation hole is
        // skipped as string content. The IL assembly check above stays the backstop.
        Assert.Empty(NullForgivingTokens.Matches(StripCommentsAndStrings("var s = $\"{value!}\";")));
        Assert.Empty(NullForgivingTokens.Matches(StripCommentsAndStrings("var s = $@\"{value!}\";")));
        Assert.Empty(NullForgivingTokens.Matches(StripCommentsAndStrings("var s = $$\"\"\"{value!}\"\"\";")));
        // Holes do not swallow surrounding code: an operator outside the literal matches.
        Assert.NotEmpty(NullForgivingTokens.Matches(StripCommentsAndStrings("var s = $\"{x}\";\nvar y = value!;")));
    }

    [Fact]
    public void GeneratedOutputExclusionRecognizesObjAndBin()
    {
        Assert.True(IsGeneratedOutputPath(Path.Combine("src", "Lokad.Parquet", "obj", "X.cs")));
        Assert.True(IsGeneratedOutputPath(Path.Combine("src", "Lokad.Parquet", "bin", "X.cs")));
        Assert.False(IsGeneratedOutputPath(Path.Combine("src", "Lokad.Parquet", "Internal", "X.cs")));
    }

    private static bool IsGeneratedOutputPath(string path) =>
        path.Split(Path.DirectorySeparatorChar).Any(static segment =>
            segment.Equals("obj", StringComparison.OrdinalIgnoreCase) ||
            segment.Equals("bin", StringComparison.OrdinalIgnoreCase));

    // Removes line comments, block comments, string literals (regular, verbatim,
    // interpolated, and raw), and character literals so textual guardrails only
    // inspect code. It assumes the input parses.
    private static string StripCommentsAndStrings(string source)
    {
        var builder = new StringBuilder(source.Length);
        var index = 0;
        while (index < source.Length)
        {
            var current = source[index];
            if (current == '/' && index + 1 < source.Length && source[index + 1] == '/')
            {
                var newline = source.IndexOf('\n', index + 2);
                if (newline < 0)
                    break;
                builder.Append('\n');
                index = newline + 1;
            }
            else if (current == '/' && index + 1 < source.Length && source[index + 1] == '*')
            {
                var end = source.IndexOf("*/", index + 2, StringComparison.Ordinal);
                index = end < 0 ? source.Length : end + 2;
            }
            else if (current == '"')
            {
                index = SkipString(source, index);
            }
            else if (current == '\'')
            {
                index = SkipCharacter(source, index);
            }
            else
            {
                builder.Append(current);
                index++;
            }
        }
        return builder.ToString();

        static int SkipString(string text, int quoteIndex)
        {
            var run = 0;
            while (quoteIndex + run < text.Length && text[quoteIndex + run] == '"')
                run++;
            if (run >= 3)
            {
                var position = quoteIndex + run;
                while (position < text.Length)
                {
                    if (text[position] == '"')
                    {
                        var close = 0;
                        while (position + close < text.Length && text[position + close] == '"')
                            close++;
                        if (close == run)
                            return position + run;
                        position++;
                    }
                    else
                    {
                        position++;
                    }
                }
                return text.Length;
            }
            var verbatim = quoteIndex > 0 && text[quoteIndex - 1] == '@';
            var offset = quoteIndex + 1;
            while (offset < text.Length)
            {
                if (text[offset] == '"')
                {
                    if (offset + 1 < text.Length && text[offset + 1] == '"')
                    {
                        offset += 2;
                        continue;
                    }
                    return offset + 1;
                }
                if (!verbatim && text[offset] == '\\')
                {
                    offset += 2;
                    continue;
                }
                offset++;
            }
            return offset;
        }

        static int SkipCharacter(string text, int quoteIndex)
        {
            var offset = quoteIndex + 1;
            while (offset < text.Length)
            {
                if (text[offset] == '\\')
                {
                    offset += 2;
                    continue;
                }
                if (text[offset] == '\'')
                    return offset + 1;
                if (text[offset] == '\n')
                    return offset;
                offset++;
            }
            return offset;
        }
    }

    private const BindingFlags AllDeclaredMembers = BindingFlags.Public | BindingFlags.NonPublic |
        BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    private static bool ContainsPointer(Type type) => type.IsPointer ||
        (type.GetElementType() is Type elementType && ContainsPointer(elementType)) ||
        (type.IsGenericType && type.GetGenericArguments().Any(ContainsPointer));

}







