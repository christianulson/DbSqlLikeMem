using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace DbSqlLikeMem.SourceGenerators;

/// <summary>
/// EN: Source generator for registering scalar functions in the SQL dialect.
/// PT: Gerador de código para registrar funções escalares no dialeto SQL.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class ScalarFunctionRegistryGenerator : IIncrementalGenerator
{
    private const string AttributeMetadataName = "DbSqlLikeMem.ScalarFunctionAttribute";

    /// <summary>
    /// EN: Initializes the source generator with the provided context.
    /// PT: Inicializa o gerador de código com o contexto fornecido.
    /// </summary>
    /// <param name="context">
    /// EN: The initialization context.
    /// PT: O contexto de inicialização.
    /// </param>
    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var registrations = context.SyntaxProvider.ForAttributeWithMetadataName(
                AttributeMetadataName,
                static (node, _) => node is MethodDeclarationSyntax,
                static (ctx, _) => TryCreateRegistration(ctx))
            .Where(static registration => registration is not null);

        context.RegisterSourceOutput(
            registrations.Collect(),
            static (context, registrations) => Execute(context, registrations!));
    }

    private static FunctionRegistration? TryCreateRegistration(GeneratorAttributeSyntaxContext context)
    {
        if (context.TargetSymbol is not IMethodSymbol method
            || method.MethodKind != MethodKind.Ordinary
            || !method.IsStatic
            || method.ContainingType is null)
        {
            return null;
        }

        var attribute = context.Attributes.FirstOrDefault();
        if (attribute is null || attribute.ConstructorArguments.Length < 2)
            return null;

        if (attribute.ConstructorArguments[0].Value is not string functionName
            || attribute.ConstructorArguments[1].Value is not string returnTypeSql)
        {
            return null;
        }

        var invocationStyle = GetEnumNamedArgument(attribute, "InvocationStyle", "Call");
        var temporalKind = GetNullableEnumNamedArgument(attribute, "TemporalKind");
        var minVersion = GetIntNamedArgument(attribute, "MinVersion");

        return new FunctionRegistration(
            NamespaceName: method.ContainingType.ContainingNamespace.IsGlobalNamespace
                ? string.Empty
                : method.ContainingType.ContainingNamespace.ToDisplayString(),
            TypeName: method.ContainingType.Name,
            MethodName: method.Name,
            FunctionName: functionName,
            ReturnTypeSql: returnTypeSql,
            InvocationStyle: invocationStyle,
            TemporalKind: temporalKind,
            MinVersion: minVersion);
    }

    private static string GetEnumNamedArgument(AttributeData attribute, string name, string defaultValue)
    {
        foreach (var namedArgument in attribute.NamedArguments)
        {
            if (namedArgument.Key != name)
                continue;

            return namedArgument.Value.Value?.ToString() ?? defaultValue;
        }

        return defaultValue;
    }

    private static string? GetNullableEnumNamedArgument(AttributeData attribute, string name)
    {
        foreach (var namedArgument in attribute.NamedArguments)
        {
            if (namedArgument.Key != name)
                continue;

            return namedArgument.Value.Value?.ToString();
        }

        return null;
    }

    private static int GetIntNamedArgument(AttributeData attribute, string name)
    {
        foreach (var namedArgument in attribute.NamedArguments)
        {
            if (namedArgument.Key != name)
                continue;

            if (namedArgument.Value.Value is int value)
                return value;

            return 0;
        }

        return 0;
    }

    private static void Execute(SourceProductionContext context, ImmutableArray<FunctionRegistration?> registrations)
    {
        var items = registrations
            .Where(static registration => registration is not null)
            .Select(static registration => registration!.Value)
            .GroupBy(static registration => (registration.NamespaceName, registration.TypeName))
            .OrderBy(static group => group.Key.NamespaceName, StringComparer.Ordinal)
            .ThenBy(static group => group.Key.TypeName, StringComparer.Ordinal)
            .ToArray();

        foreach (var group in items)
        {
            var source = BuildSource(
                group.Key.NamespaceName,
                group.Key.TypeName,
                group.OrderBy(static item => item.FunctionName, StringComparer.Ordinal).ToArray());
            context.AddSource($"{group.Key.TypeName}.ScalarFunctionRegistry.g.cs", source);
        }
    }

    private static string BuildSource(string namespaceName, string typeName, IReadOnlyList<FunctionRegistration> registrations)
    {
        var builder = new StringBuilder();
        builder.AppendLine("#nullable enable");
        if (!string.IsNullOrEmpty(namespaceName))
        {
            builder.Append("namespace ").Append(namespaceName).AppendLine(";");
            builder.AppendLine();
        }

        builder.AppendLine("internal static partial class " + typeName);
        builder.AppendLine("{");
        builder.AppendLine("    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect)");
        builder.AppendLine("    {");

        foreach (var registration in registrations)
        {
            if (registration.MinVersion > 0)
            {
                builder.Append("        if (dialect.Version >= ")
                    .Append(registration.MinVersion)
                    .AppendLine(")");
                builder.AppendLine("        {");
            }

            builder.Append("        ");
            if (registration.MinVersion > 0)
                builder.Append("    ");

            builder.Append("dialect.AddScalarFunction(\"")
                .Append(Escape(registration.FunctionName))
                .Append("\", \"")
                .Append(Escape(registration.ReturnTypeSql))
                .Append("\", ")
                .Append(registration.MethodName);

            if (!string.Equals(registration.InvocationStyle, "Call", StringComparison.Ordinal))
                builder.Append(", DbInvocationStyle.").Append(registration.InvocationStyle);

            if (!string.IsNullOrEmpty(registration.TemporalKind))
            {
                if (string.Equals(registration.InvocationStyle, "Call", StringComparison.Ordinal))
                    builder.Append(", DbInvocationStyle.Call");

                builder.Append(", SqlTemporalFunctionKind.").Append(registration.TemporalKind);
            }

            builder.AppendLine(");");

            if (registration.MinVersion > 0)
                builder.AppendLine("        }");
        }

        builder.AppendLine("    }");
        builder.AppendLine("}");
        return builder.ToString();
    }

    private static string Escape(string value) => value.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private readonly record struct FunctionRegistration(
        string NamespaceName,
        string TypeName,
        string MethodName,
        string FunctionName,
        string ReturnTypeSql,
        string InvocationStyle,
        string? TemporalKind,
        int MinVersion);
}

