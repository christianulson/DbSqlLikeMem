namespace DbSqlLikeMem;

/// <summary>
/// EN: Holds the SQL text and parameter object produced by a LINQ translation.
/// PT-br: Armazena o SQL e o objeto de parametros produzidos por uma traducao LINQ.
/// </summary>
public record TranslationResult(string Sql, object Params);
