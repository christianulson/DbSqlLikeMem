using System.Text.RegularExpressions;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Exposes and validates the supported placeholders for template-based generation.
/// PT: Expoe e valida os placeholders suportados pela geracao baseada em templates.
/// </summary>
public static class TemplateTokenCatalog
{
    private static readonly Regex TemplateTokenRegex = new(@"\{\{[^{}\r\n]+\}\}", RegexOptions.Compiled);

    private static readonly string[] SupportedTokens =
    [
        "{{ClassName}}",
        "{{ObjectName}}",
        "{{Schema}}",
        "{{ObjectType}}",
        "{{DatabaseType}}",
        "{{DatabaseName}}",
        "{{Namespace}}",
    ];

    /// <summary>
    /// EN: Gets the placeholders currently supported by the generation runtime.
    /// PT: Obtem os placeholders atualmente suportados pelo runtime de geracao.
    /// </summary>
    public static IReadOnlyCollection<string> GetSupportedTokens() => SupportedTokens;

    /// <summary>
    /// EN: Finds placeholders present in a template that are not supported by the runtime.
    /// PT: Localiza placeholders presentes em um template que nao sao suportados pelo runtime.
    /// </summary>
    /// <param name="template">EN: Raw template content to inspect. PT: Conteudo bruto do template a inspecionar.</param>
    public static IReadOnlyCollection<string> FindUnsupportedTokens(string template)
    {
        if (string.IsNullOrWhiteSpace(template))
        {
            return [];
        }

        var supported = new HashSet<string>(SupportedTokens, StringComparer.OrdinalIgnoreCase);
        return TemplateTokenRegex
            .Matches(template)
            .Cast<Match>()
            .Select(match => match.Value)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Where(token => !supported.Contains(token))
            .OrderBy(token => token, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }
}
