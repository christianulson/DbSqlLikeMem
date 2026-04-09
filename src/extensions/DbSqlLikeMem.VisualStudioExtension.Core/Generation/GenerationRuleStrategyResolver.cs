namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public static class GenerationRuleStrategyResolver
{
    private static readonly IGenerationRuleStrategy Default = new DefaultGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy MySql = new MySqlGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy Oracle = new OracleGenerationRuleStrategy();

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public static IGenerationRuleStrategy Resolve(string? databaseType)
        => Normalize(databaseType) switch
        {
            "mysql" => MySql,
            "oracle" => Oracle,
            _ => Default
        };

    private static string Normalize(string? databaseType)
        => (databaseType ?? string.Empty).Trim().ToLowerInvariant();
}
