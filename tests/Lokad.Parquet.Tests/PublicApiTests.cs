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
        // interfaces, generic constraints, static members, init accessors, and
        // reference nullability. A change to any of those contracts must move
        // the snapshot line; PublicApiContractsCoverRepresentativeCases pins
        // one example per dimension independently of this file.
        static string[] DescribePublicApi(Assembly assembly)
        {
            var nullability = new NullabilityInfoContext();

            static string Parameters(ParameterInfo[] parameters, NullabilityInfoContext nullability)
            {
                static string DefaultValue(object? value) => value switch
                {
                    null => "null",
                    bool boolean => boolean ? "true" : "false",
                    string text => $"\"{text}\"",
                    _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
                };

                return string.Join(", ", parameters.Select(parameter =>
                    $"{TypeName(parameter.ParameterType, nullability.Create(parameter))} {parameter.Name}" +
                    (parameter.HasDefaultValue ? $" = {DefaultValue(parameter.DefaultValue)}" : "")));
            }

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
                foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                             .Where(static method => !method.IsSpecialName)
                             .OrderBy(static method => method.Name, StringComparer.Ordinal)
                             .ThenBy(static method => method.ToString(), StringComparer.Ordinal))
                    lines.Add($"  method {(method.IsStatic ? "static " : "")}{TypeName(method.ReturnType, nullability.Create(method.ReturnParameter))} {method.Name}({Parameters(method.GetParameters(), nullability)})");
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
                    var constraints = type.GetGenericArguments().Select(static parameter =>
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
                    }).Where(static clause => clause.Length != 0);
                    var suffix = string.Join(" ", constraints);
                    if (suffix.Length != 0)
                        declaration += " " + suffix;
                }
                return declaration;

                static bool IsUnmanaged(Type parameter) =>
                    parameter.GetCustomAttributesData().Any(static attribute =>
                        attribute.AttributeType.FullName == "System.Runtime.CompilerServices.IsUnmanagedAttribute");
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
                    return TypeName(type.GetElementType() ?? throw new InvalidOperationException("An array type has no element type."), info?.ElementType) + "[]";
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
        }
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
}
