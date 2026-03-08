using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Renders documented generation-template tokens into concrete file content.
/// PT: Renderiza os tokens documentados de template de geração em conteúdo concreto de arquivo.
/// </summary>
public static class TemplateContentRenderer
{
    /// <summary>
    /// EN: Replaces the supported tokens in a template using the informed object, connection, and namespace context.
    /// PT: Substitui os tokens suportados em um template usando o contexto informado de objeto, conexão e namespace.
    /// </summary>
    /// <param name="template">EN: Raw template text. PT: Texto bruto do template.</param>
    /// <param name="className">EN: Generated class name. PT: Nome da classe gerada.</param>
    /// <param name="dbObject">EN: Database object used as generation source. PT: Objeto de banco usado como fonte da geração.</param>
    /// <param name="connection">EN: Connection metadata associated with the generation. PT: Metadados da conexão associados à geração.</param>
    /// <param name="namespace">EN: Optional namespace value for `{{Namespace}}`. PT: Valor opcional de namespace para `{{Namespace}}`.</param>
    public static string Render(
        string template,
        string className,
        DatabaseObjectReference dbObject,
        ConnectionDefinition connection,
        string? @namespace = null)
    {
        var content = ReplaceIgnoreCase(template, "{{ClassName}}", className);
        content = ReplaceIgnoreCase(content, "{{ObjectName}}", dbObject.Name);
        content = ReplaceIgnoreCase(content, "{{Schema}}", dbObject.Schema);
        content = ReplaceIgnoreCase(content, "{{ObjectType}}", dbObject.Type.ToString());
        content = ReplaceIgnoreCase(content, "{{DatabaseType}}", connection.DatabaseType);
        content = ReplaceIgnoreCase(content, "{{DatabaseName}}", connection.DatabaseName);
        content = ReplaceIgnoreCase(content, "{{Namespace}}", @namespace ?? string.Empty);
        return content.Contains("// DBSqlLikeMem:", StringComparison.Ordinal)
            ? content
            : BuildMetadataHeader(dbObject) + content;
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

    private static string BuildMetadataHeader(DatabaseObjectReference dbObject)
    {
        var lines = new List<string>
        {
            $"// DBSqlLikeMem:Schema={dbObject.Schema}",
            $"// DBSqlLikeMem:Object={dbObject.Name}",
            $"// DBSqlLikeMem:Type={dbObject.Type}"
        };

        AppendIfPresent(lines, dbObject, "Columns");
        AppendIfPresent(lines, dbObject, "PrimaryKey");
        AppendIfPresent(lines, dbObject, "Indexes");
        AppendIfPresent(lines, dbObject, "ForeignKeys");
        AppendIfPresent(lines, dbObject, "Triggers");
        AppendIfPresent(lines, dbObject, "StartValue");
        AppendIfPresent(lines, dbObject, "IncrementBy");
        AppendIfPresent(lines, dbObject, "CurrentValue");

        return string.Join(Environment.NewLine, lines) + Environment.NewLine;
    }

    private static void AppendIfPresent(List<string> lines, DatabaseObjectReference dbObject, string key)
    {
        if (dbObject.Properties is not null && dbObject.Properties.TryGetValue(key, out var value))
        {
            lines.Add($"// DBSqlLikeMem:{key}={value}");
        }
    }
}
