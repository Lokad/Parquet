using System.Reflection;

namespace Lokad.Parquet.Tests;

public sealed class PublicApiTests
{
    [Fact]
    public void PublicApiMatchesReviewedSnapshot()
    {
        var expected = File.ReadAllLines(Path.Combine(AppContext.BaseDirectory, "PublicApi.Shipped.txt"))
            .Where(static line => line.Length != 0 && !line.StartsWith('#'))
            .ToArray();
        var actual = DescribePublicApi(typeof(ParquetFile).Assembly);

        Assert.True(expected.SequenceEqual(actual, StringComparer.Ordinal),
            "Public API differs from the reviewed snapshot. Actual API:\n" + string.Join('\n', actual));

        // Each line deliberately records the API shape: type kind (sealed,
        // abstract, static, struct, interface, enum) with base types and
        // interfaces, generic constraints, static members, init accessors,
        // fields, events, method type constraints, by-reference parameter kinds,
        // and reference nullability including array-level suffixes. A change to
        // any of those contracts must move the snapshot line;
        // PublicApiContractsCoverRepresentativeCases pins
        // one example per dimension independently of this file.
    }

    [Fact]
    public void DescriberSurfacesActualPublicMembers()
    {
        // The snapshot only moves when DescribePublicApi reports a change, so this
        // pins the describer itself: describing another assembly must surface its own
        // real public members, while the shipped description stays free of test members.
        var lines = DescribePublicApi(typeof(PublicApiTests).Assembly);
        Assert.Contains(lines, static line => line.Contains("PublicApiTests", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("PublicApiMatchesReviewedSnapshot", StringComparison.Ordinal));
        var shipped = DescribePublicApi(typeof(ParquetFile).Assembly);
        Assert.DoesNotContain(shipped, static line => line.Contains("PublicApiTests", StringComparison.Ordinal));
    }

    [Fact]
    public void DescriberRecordsFieldsEventsByRefAndGenericMethods()
    {
        // The shipped API has no public fields, events, generic methods, by-reference
        // parameters, or array members, so the snapshot cannot move for these
        // dimensions. This pins each extension against the probe below instead,
        // exercising the member behavior as well as its description.
        var probe = new ApiShapeProbe();
        var raised = 0;
        probe.Changed += () => raised++;
        probe.RaiseChanged();
        Assert.Equal(1, raised);
        var both = 1;
        probe.Split(ref both, out var rest, in both);
        Assert.Equal(2, rest);
        Assert.Equal("ok", probe.Echo("ok"));
        Assert.Null(probe.FindNames());
        Assert.Empty(probe.GetNames());
        var lines = DescribePublicApi(typeof(PublicApiTests).Assembly);
        Assert.Contains(lines, static line => line.Contains("field System.Int32 Counter", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("field const System.String Label", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("field readonly System.String Tag", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("field static System.Int32 LiveCount", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("event System.Action? Changed", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("Echo<T>(T value) where T : class", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("ref System.Int32 both", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("out System.Int32 rest", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("in System.Int32 witness", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("System.String[]? FindNames", StringComparison.Ordinal));
        Assert.Contains(lines, static line => line.Contains("System.String?[] GetNames", StringComparison.Ordinal));
    }

    [Fact]
    public void PublicApiContractsCoverRepresentativeCases()
    {
        // One independent pin per snapshot dimension: if the describer above
        // ever stops recording a dimension, the matching assertion below still
        // guards the contract.
        Assert.True(typeof(ParquetFile).IsSealed);
        Assert.True(typeof(IDisposable).IsAssignableFrom(typeof(ParquetBatch)));
        Assert.True(typeof(ParquetColumnBatch).IsAbstract);
        Assert.True(typeof(ParquetFile).GetMethod(nameof(ParquetFile.OpenAsync), [typeof(string)])?.IsStatic == true);
        Assert.False(typeof(ParquetFile).GetMethod(nameof(ParquetFile.ScanAsync), [typeof(ParquetScanOptions)])?.IsStatic == true);
        Assert.True(typeof(ParquetReaderOptions).GetProperty("MaximumRowsPerBatch")?.SetMethod?.ReturnParameter
            .GetRequiredCustomModifiers().Any(static modifier => modifier.FullName == "System.Runtime.CompilerServices.IsExternalInit") == true);
        var typeParameter = typeof(ParquetPrimitiveColumnBatch<>).GetGenericArguments()[0];
        Assert.Contains(typeof(ValueType), typeParameter.GetGenericParameterConstraints());
        Assert.True(typeParameter.GenericParameterAttributes.HasFlag(GenericParameterAttributes.NotNullableValueTypeConstraint));
        var unsupportedReason = typeof(ParquetColumn).GetProperty(nameof(ParquetColumn.UnsupportedReason));
        var columnName = typeof(ParquetColumn).GetProperty(nameof(ParquetColumn.Name));
        if (unsupportedReason is null || columnName is null)
            throw new InvalidOperationException("A representative contract property was not found.");
        var nullability = new NullabilityInfoContext();
        Assert.Equal(NullabilityState.Nullable, nullability.Create(unsupportedReason).ReadState);
        Assert.Equal(NullabilityState.NotNull, nullability.Create(columnName).ReadState);
    }

    // Probe shapes the shipped API does not currently use, so the snapshot extensions
    // stay honest: each new describer dimension is pinned against a real member here.
    public sealed class ApiShapeProbe
    {
        public int Counter;
        public const string Label = "probe";
        public readonly string Tag = "tag";
        public static int LiveCount;
        public event Action? Changed;
        public void RaiseChanged() => Changed?.Invoke();
        public T Echo<T>(T value) where T : class => value;
        public void Split(ref int both, out int rest, in int witness) => rest = both + witness;
        public string[]? FindNames() => null;
        public string?[] GetNames() => [];
    }

    // Shared by the snapshot and the describer-sensitivity test so both observe the same
    // API description logic.
    private static string[] DescribePublicApi(Assembly assembly)
    {
        var nullability = new NullabilityInfoContext();

        static string Parameters(ParameterInfo[] parameters, NullabilityInfoContext nullability)
        {
            return string.Join(", ", parameters.Select(parameter =>
                Parameter(parameter, nullability.Create(parameter)) +
                (parameter.HasDefaultValue ? $" = {DefaultValue(parameter.DefaultValue)}" : "")));
        }

        static string Parameter(ParameterInfo parameter, NullabilityInfo parameterInfo)
        {
            if (parameter.ParameterType.IsByRef)
            {
                var elementType = parameter.ParameterType.GetElementType() ?? throw new InvalidOperationException("A by-reference parameter has no element type.");
                var prefix = parameter.IsOut ? "out " : parameter.IsIn ? "in " : "ref ";
                return prefix + TypeName(elementType, parameterInfo.ElementType) + " " + parameter.Name;
            }

            return TypeName(parameter.ParameterType, parameterInfo) + " " + parameter.Name;
        }

        static string DefaultValue(object? value) => value switch
        {
            null => "null",
            bool boolean => boolean ? "true" : "false",
            string text => $"\"{text}\"",
            _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
        };

        var lines = new List<string>();
        foreach (var type in assembly.GetExportedTypes().OrderBy(static type => type.FullName, StringComparer.Ordinal))
        {
            lines.Add(TypeDeclaration(type));
            if (type.IsEnum)
            {
                foreach (var name in Enum.GetNames(type))
                    lines.Add($"  enum {name} = {Convert.ToInt64(Enum.Parse(type, name))}");
                continue;
            }

            foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                         .OrderBy(static constructor => constructor.ToString(), StringComparer.Ordinal))
                lines.Add($"  ctor ({Parameters(constructor.GetParameters(), nullability)})");
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static field => field.Name, StringComparer.Ordinal))
                lines.Add($"  field {FieldPrefix(field)}{TypeName(field.FieldType, nullability.Create(field))} {field.Name}");
            foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static property => property.Name, StringComparer.Ordinal))
            {
                var accessor = property.GetMethod?.IsPublic != true
                    ? ""
                    : property.SetMethod?.IsPublic == true
                        ? IsInitOnly(property) ? "get; init; " : "get; set; "
                        : "get; ";
                var owner = (property.GetMethod ?? property.SetMethod)?.IsStatic == true ? "static " : "";
                lines.Add($"  property {owner}{TypeName(property.PropertyType, nullability.Create(property))} {property.Name} {{ {accessor}}}");
            }
            foreach (var declaredEvent in type.GetEvents(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .OrderBy(static declaredEvent => declaredEvent.Name, StringComparer.Ordinal))
            {
                var owner = declaredEvent.AddMethod?.IsStatic == true ? "static " : "";
                lines.Add($"  event {owner}{TypeName(declaredEvent.EventHandlerType ?? throw new InvalidOperationException("An event has no handler type."), nullability.Create(declaredEvent))} {declaredEvent.Name}");
            }
            foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                         .Where(static method => !method.IsSpecialName)
                         .OrderBy(static method => method.Name, StringComparer.Ordinal)
                         .ThenBy(static method => method.ToString(), StringComparer.Ordinal))
                lines.Add($"  method {(method.IsStatic ? "static " : "")}{TypeName(method.ReturnType, nullability.Create(method.ReturnParameter))} {MethodName(method)}({Parameters(method.GetParameters(), nullability)}){MethodConstraints(method)}");
        }
        return lines.ToArray();

        static string TypeDeclaration(Type type)
        {
            var kind = type.IsEnum
                ? "enum"
                : type.IsInterface
                    ? "interface"
                    : type.IsValueType
                        ? "struct"
                        : type.IsAbstract && type.IsSealed
                            ? "static class"
                            : type.IsAbstract
                                ? "abstract class"
                                : type.IsSealed
                                    ? "sealed class"
                                    : "class";
            var name = TypeName(type, null);
            if (type.IsEnum)
                return $"type {kind} {name}";
            var bases = new List<string>();
            if (type.IsClass)
                bases.Add(TypeName(type.BaseType ?? throw new InvalidOperationException("A public class has no base type."), null));
            bases.AddRange(type.GetInterfaces()
                .Select(static face => TypeName(face, null))
                .OrderBy(static fullName => fullName, StringComparer.Ordinal));
            var declaration = bases.Count == 0 ? $"type {kind} {name}" : $"type {kind} {name} : {string.Join(", ", bases)}";
            if (type.IsGenericTypeDefinition)
            {
                var constraints = type.GetGenericArguments().Select(static parameter => ConstraintClause(parameter))
                    .Where(static clause => clause.Length != 0);
                var suffix = string.Join(" ", constraints);
                if (suffix.Length != 0)
                    declaration += " " + suffix;
            }
            return declaration;
        }

        static bool IsInitOnly(PropertyInfo property) =>
            property.SetMethod?.ReturnParameter.GetRequiredCustomModifiers()
                .Any(static modifier => modifier.FullName == "System.Runtime.CompilerServices.IsExternalInit") == true;

        static string TypeName(Type type, NullabilityInfo? info)
        {
            var suffix = info is { ReadState: NullabilityState.Nullable } && !type.IsValueType ? "?" : "";
            if (type.IsByRef)
                return TypeName(type.GetElementType() ?? throw new InvalidOperationException("A by-reference type has no element type."), info?.ElementType) + "&";
            if (type.IsArray)
            {
                var rank = new string(',', type.GetArrayRank() - 1);
                var arraySuffix = info is { ReadState: NullabilityState.Nullable } && !type.IsValueType ? "?" : "";
                return TypeName(type.GetElementType() ?? throw new InvalidOperationException("An array type has no element type."), info?.ElementType) + "[" + rank + "]" + arraySuffix;
            }
            if (!type.IsGenericType)
                return (type.FullName ?? type.Name) + suffix;
            var name = type.GetGenericTypeDefinition().FullName ??
                throw new InvalidOperationException("A generic type definition has no full name.");
            name = name[..name.IndexOf('`')];
            var arguments = type.GetGenericArguments();
            var argumentInfos = info?.GenericTypeArguments;
            return name + "<" + string.Join(",", arguments.Select((argument, index) =>
                TypeName(argument, argumentInfos is not null && index < argumentInfos.Length ? argumentInfos[index] : null))) + ">" + suffix;
        }

        static string ConstraintClause(Type parameter)
        {
            var bounds = new List<string>();
            var attributes = parameter.GenericParameterAttributes;
            if (attributes.HasFlag(GenericParameterAttributes.ReferenceTypeConstraint))
                bounds.Add("class");
            if (attributes.HasFlag(GenericParameterAttributes.NotNullableValueTypeConstraint))
                bounds.Add(IsUnmanaged(parameter) ? "unmanaged" : "struct");
            if (attributes.HasFlag(GenericParameterAttributes.DefaultConstructorConstraint) &&
                !bounds.Contains("struct") && !bounds.Contains("unmanaged"))
                bounds.Add("new()");
            bounds.AddRange(parameter.GetGenericParameterConstraints().Where(static bound => bound != typeof(ValueType)).Select(static bound => TypeName(bound, null)));
            return bounds.Count == 0 ? "" : $"where {parameter.Name} : {string.Join(", ", bounds)}";
        }

        static bool IsUnmanaged(Type parameter) =>
            parameter.GetCustomAttributesData().Any(static attribute =>
                attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsUnmanagedAttribute");

        static string FieldPrefix(FieldInfo field)
        {
            var owner = field.IsStatic && !field.IsLiteral ? "static " : "";
            var mutability = field.IsLiteral ? "const " : field.IsInitOnly ? "readonly " : "";
            var required = field.IsDefined(typeof(System.Runtime.CompilerServices.RequiredMemberAttribute), false) ? "required " : "";
            return owner + mutability + required;
        }

        static string MethodName(MethodInfo method)
        {
            if (!method.IsGenericMethodDefinition)
                return method.Name;
            return method.Name + "<" + string.Join(",", method.GetGenericArguments().Select(static argument => argument.Name)) + ">";
        }

        static string MethodConstraints(MethodInfo method)
        {
            if (!method.IsGenericMethodDefinition)
                return "";
            var suffix = string.Join(" ", method.GetGenericArguments().Select(static argument => ConstraintClause(argument)).Where(static clause => clause.Length != 0));
            return suffix.Length == 0 ? "" : " " + suffix;
        }
    }
}
