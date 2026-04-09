using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Resolves file names for template-based generation using the supported placeholder contract.
/// PT: Resolve nomes de arquivo para geracao baseada em templates usando o contrato de placeholders suportados.
/// </summary>
public static class TemplateFileNamePatternResolver
{
    /// <summary>
    /// EN: Resolves the file name for a template-generated artifact.
    /// PT: Resolve o nome do arquivo para um artefato gerado por template.
    /// </summary>
    /// <param name="fileNamePattern">EN: Optional configured file name pattern. PT: Padrao configurado opcional de nome de arquivo.</param>
    /// <param name="kindSuffix">EN: Default suffix like `Model` or `Repository`. PT: Sufixo padrao como `Model` ou `Repository`.</param>
    /// <param name="connection">EN: Generation connection metadata. PT: Metadados da conexao de geracao.</param>
    /// <param name="dbObject">EN: Database object that originated the artifact. PT: Objeto de banco que originou o artefato.</param>
    /// <param name="namespace">EN: Optional namespace used for `{Namespace}`. PT: Namespace opcional usado em `{Namespace}`.</param>
    public static string Resolve(
        string? fileNamePattern,
        string kindSuffix,
        ConnectionDefinition connection,
        DatabaseObjectReference dbObject,
        string? @namespace = null)
    {
        var safePattern = string.IsNullOrWhiteSpace(fileNamePattern)
            ? $"{{NamePascal}}{kindSuffix}.cs"
            : fileNamePattern;

        var resolved = safePattern!;
        resolved = ReplaceIgnoreCase(resolved, "{NamePascal}", GenerationRuleSet.ToPascalCase(dbObject.Name));
        resolved = ReplaceIgnoreCase(resolved, "{Name}", dbObject.Name);
        resolved = ReplaceIgnoreCase(resolved, "{Type}", dbObject.Type.ToString());
        resolved = ReplaceIgnoreCase(resolved, "{Schema}", dbObject.Schema);
        resolved = ReplaceIgnoreCase(resolved, "{DatabaseType}", connection.DatabaseType);
        resolved = ReplaceIgnoreCase(resolved, "{DatabaseName}", connection.DatabaseName);
        resolved = ReplaceIgnoreCase(resolved, "{Namespace}", @namespace ?? string.Empty);
        return resolved;
    }

    private static string ReplaceIgnoreCase(string value, string oldValue, string newValue)
    {
        var current = value;
        var index = current.IndexOf(oldValue, StringComparison.OrdinalIgnoreCase);

        while (index >= 0)
        {
            current = current.Remove(index, oldValue.Length).Insert(index, newValue);
            index = current.IndexOf(oldValue, index + newValue.Length, StringComparison.OrdinalIgnoreCase);
        }

        return current;
    }
}
