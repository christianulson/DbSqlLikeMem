namespace DbSqlLikeMem;

/// <summary>
/// Resultado da tradução de expressão LINQ para SQL e parâmetros.
/// </summary>
public record TranslationResult(string Sql, object Params);
