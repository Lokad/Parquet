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

        static string[] DescribePublicApi(Assembly assembly)
        {
            static string Parameters(ParameterInfo[] parameters)
            {
                static string DefaultValue(object? value) => value switch
                {
                    null => "null",
                    bool boolean => boolean ? "true" : "false",
                    string text => $"\"{text}\"",
                    _ => Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "null",
                };

                return string.Join(", ", parameters.Select(parameter =>
                    $"{TypeName(parameter.ParameterType)} {parameter.Name}" +
                    (parameter.HasDefaultValue ? $" = {DefaultValue(parameter.DefaultValue)}" : "")));
            }

            var lines = new List<string>();
            foreach (var type in assembly.GetExportedTypes().OrderBy(static type => type.FullName, StringComparer.Ordinal))
            {
                lines.Add($"type {TypeName(type)}");
                if (type.IsEnum)
                {
                    foreach (var name in Enum.GetNames(type))
                        lines.Add($"  enum {name} = {Convert.ToInt64(Enum.Parse(type, name))}");
                    continue;
                }

                foreach (var constructor in type.GetConstructors(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                             .OrderBy(static constructor => constructor.ToString(), StringComparer.Ordinal))
                    lines.Add($"  ctor ({Parameters(constructor.GetParameters())})");
                foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                             .OrderBy(static property => property.Name, StringComparer.Ordinal))
                    lines.Add($"  property {TypeName(property.PropertyType)} {property.Name} {{ " +
                        (property.GetMethod?.IsPublic == true ? "get; " : "") +
                        (property.SetMethod?.IsPublic == true ? "set; " : "") + "}");
                foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly)
                             .Where(static method => !method.IsSpecialName)
                             .OrderBy(static method => method.Name, StringComparer.Ordinal)
                             .ThenBy(static method => method.ToString(), StringComparer.Ordinal))
                    lines.Add($"  method {TypeName(method.ReturnType)} {method.Name}({Parameters(method.GetParameters())})");
            }
            return lines.ToArray();
        }
    }

    private static string TypeName(Type type)
    {
        if (type.IsByRef)
            return TypeName(type.GetElementType() ?? throw new InvalidOperationException("A by-reference type has no element type.")) + "&";
        if (type.IsArray)
            return TypeName(type.GetElementType() ?? throw new InvalidOperationException("An array type has no element type.")) + "[]";
        if (!type.IsGenericType)
            return type.FullName ?? type.Name;
        var name = type.GetGenericTypeDefinition().FullName ??
            throw new InvalidOperationException("A generic type definition has no full name.");
        name = name[..name.IndexOf('`')];
        return name + "<" + string.Join(",", type.GetGenericArguments().Select(TypeName)) + ">";
    }
}
