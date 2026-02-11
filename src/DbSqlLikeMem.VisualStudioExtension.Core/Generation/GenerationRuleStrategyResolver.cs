namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public static class GenerationRuleStrategyResolver
{
    private static readonly IGenerationRuleStrategy Default = new DefaultGenerationRuleStrategy();
    private static readonly IGenerationRuleStrategy MySql = new MySqlGenerationRuleStrategy();

    public static IGenerationRuleStrategy Resolve(string? databaseType)
        => Normalize(databaseType) switch
        {
            "mysql" => MySql,
            _ => Default
        };

    private static string Normalize(string? databaseType)
        => (databaseType ?? string.Empty).Trim().ToLowerInvariant();
}
