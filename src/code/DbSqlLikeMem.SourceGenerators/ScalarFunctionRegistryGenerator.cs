using System.Collections.Immutable;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace DbSqlLikeMem.SourceGenerators;

/// <summary>
/// EN: Generates a registry for scalar functions marked with the ScalarFunctionAttribute.
/// PT-br: Gera um registro para funções escalares marcadas com o ScalarFunctionAttribute.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class ScalarFunctionRegistryGenerator : IIncrementalGenerator
{
    private const string AttributeMetadataName = "DbSqlLikeMem.ScalarFunctionAttribute";

    /// <summary>
    /// EN: Initialize
    /// PT-br: Inicializa
    /// </summary>
    /// <param name="context"></param>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var provider = context.SyntaxProvider.ForAttributeWithMetadataName(
            AttributeMetadataName,
            static (node, _) => node is MethodDeclarationSyntax,
            static (ctx, _) => GetRegistrations(ctx))
            .SelectMany(static (registrations, _) => registrations);

        context.RegisterSourceOutput(provider.Collect(), GenerateOutput);
    }

    private static ImmutableArray<FunctionRegistration> GetRegistrations(GeneratorAttributeSyntaxContext ctx)
    {
        if (ctx.TargetSymbol is not IMethodSymbol method
            || method.MethodKind != MethodKind.Ordinary
            || !method.IsStatic
            || method.ContainingType is null)
        {
            return [];
        }

        var builder = ImmutableArray.CreateBuilder<FunctionRegistration>(ctx.Attributes.Length);
        foreach (var attribute in ctx.Attributes)
        {
            if (attribute.ConstructorArguments.Length < 2)
                continue;

            if (attribute.ConstructorArguments[0].Value is not string functionName
                || attribute.ConstructorArguments[1].Value is not string returnTypeSql)
            {
                continue;
            }

            var ns = method.ContainingType.ContainingNamespace.IsGlobalNamespace
                ? string.Empty
                : method.ContainingType.ContainingNamespace.ToDisplayString();

            var invocationStyle = GetNamedInt(attribute, "InvocationStyle", 1);
            var temporalKind = GetNamedInt(attribute, "TemporalKind", -1);
            var minVersion = GetNamedInt(attribute, "MinVersion", 0);

            builder.Add(new FunctionRegistration(
                ns,
                method.ContainingType.Name,
                method.Name,
                functionName,
                returnTypeSql,
                invocationStyle,
                temporalKind,
                minVersion));
        }

        return builder.ToImmutable();
    }

    private static void GenerateOutput(SourceProductionContext context, ImmutableArray<FunctionRegistration> registrations)
    {
        var entries = registrations.ToArray();
        if (entries.Length == 0)
            return;

        foreach (var group in entries.GroupBy(static entry => (entry.NamespaceName, entry.TypeName)))
        {
            var source = new StringBuilder();
            source.AppendLine("using DbSqlLikeMem;");
            source.AppendLine();
            if (!string.IsNullOrEmpty(group.Key.NamespaceName))
            {
                source.Append("namespace ").Append(group.Key.NamespaceName).AppendLine(";");
                source.AppendLine();
            }

            source.Append("internal static partial class ").Append(group.Key.TypeName).AppendLine();
            source.AppendLine("{");
            source.AppendLine("    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect)");
            source.AppendLine("    {");

            foreach (var registration in group)
            {
                if (registration.MinVersion > 0)
                {
                    source.Append("        if (dialect.Version >= ").Append(registration.MinVersion).AppendLine(")");
                    source.AppendLine("        {");
                    source.Append("            ");
                    AppendRegistrationLine(source, registration);
                    source.AppendLine();
                    source.AppendLine("        }");
                }
                else
                {
                    source.Append("        ");
                    AppendRegistrationLine(source, registration);
                    source.AppendLine();
                }
            }

            source.AppendLine("    }");
            source.AppendLine("}");

            context.AddSource(
                $"{group.Key.TypeName}.ScalarFunctionRegistry.g.cs",
                SourceText.From(source.ToString(), Encoding.UTF8));
        }
    }

    private static void AppendRegistrationLine(StringBuilder builder, FunctionRegistration registration)
    {
        builder
            .Append("dialect.AddScalarFunction(\"")
            .Append(registration.FunctionName)
            .Append("\", \"")
            .Append(registration.ReturnTypeSql)
            .Append("\", ")
            .Append(registration.MethodName)
            .Append(", ")
            .Append(FormatInvocationStyle(registration.InvocationStyle))
            .Append(", ")
            .Append(FormatTemporalKind(registration.TemporalKind))
            .Append(");");
    }

    private static int GetNamedInt(AttributeData attribute, string name, int defaultValue)
    {
        foreach (var argument in attribute.NamedArguments)
        {
            if (argument.Key != name || argument.Value.Value is not int intValue)
                continue;

            return intValue;
        }

        return defaultValue;
    }

    private static string FormatInvocationStyle(int invocationStyle)
        => invocationStyle switch
        {
            0 => "global::DbSqlLikeMem.Models.DbInvocationStyle.None",
            1 => "global::DbSqlLikeMem.Models.DbInvocationStyle.Call",
            2 => "global::DbSqlLikeMem.Models.DbInvocationStyle.Identifier",
            _ => $"(global::DbSqlLikeMem.Models.DbInvocationStyle){invocationStyle}"
        };

    private static string FormatTemporalKind(int temporalKind)
        => temporalKind switch
        {
            -1 => "null",
            0 => "global::DbSqlLikeMem.SqlTemporalFunctionKind.Date",
            1 => "global::DbSqlLikeMem.SqlTemporalFunctionKind.Time",
            2 => "global::DbSqlLikeMem.SqlTemporalFunctionKind.DateTime",
            3 => "global::DbSqlLikeMem.SqlTemporalFunctionKind.DateTimeOffset",
            _ => $"(global::DbSqlLikeMem.SqlTemporalFunctionKind?){temporalKind}"
        };

    private readonly record struct FunctionRegistration(
        string NamespaceName,
        string TypeName,
        string MethodName,
        string FunctionName,
        string ReturnTypeSql,
        int InvocationStyle,
        int TemporalKind,
        int MinVersion);
}
