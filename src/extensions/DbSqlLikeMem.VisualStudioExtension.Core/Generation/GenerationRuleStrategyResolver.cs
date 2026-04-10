namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Resolves the generation strategy used to map database column types to CLR-friendly types.
/// PT: Resolve a estrategia de geracao usada para mapear tipos de coluna do banco para tipos amigaveis ao CLR.
/// </summary>
public static class GenerationRuleStrategyResolver
{
    private static readonly IGenerationRuleStrategy Default = new DefaultGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy Firebird = new FirebirdGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy MySql = new MySqlGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy Oracle = new OracleGenerationRuleStrategy();

    /// <summary>
    /// EN: Returns the strategy that matches the informed database type.
    /// PT: Retorna a estrategia que corresponde ao tipo de banco informado.
    /// </summary>
    public static IGenerationRuleStrategy Resolve(string? databaseType)
        => Normalize(databaseType) switch
        {
            "firebird" => Firebird,
            "mysql" => MySql,
            "oracle" => Oracle,
            _ => Default
        };

    private static string Normalize(string? databaseType)
        => (databaseType ?? string.Empty).Trim().ToLowerInvariant();
}
