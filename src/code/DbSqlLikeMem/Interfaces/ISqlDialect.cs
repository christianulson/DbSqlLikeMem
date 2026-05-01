namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines escape rules and behavior for a SQL dialect.
/// PT-br: Define regras de escape e comportamento de um dialeto SQL.
/// </summary>
internal interface ISqlDialect : ISqlDialectParser, ISqlDialectFunctions, ISqlDialectDdl, ISqlDialectQueryFeatures, ISqlDialectRuntime
{
    /// <summary>
    /// EN: Gets the compatibility version used by the dialect.
    /// PT-br: Obtém a versao de compatibilidade usada pelo dialeto.
    /// </summary>
    int Version { get; }
    /// <summary>
    /// EN: Gets the canonical dialect name used for comparisons and dispatch.
    /// PT-br: Obtém o nome canonico do dialeto usado para comparacoes e dispatch.
    /// </summary>
    string Name { get; }

    /// <summary>
    /// EN: Gets the scalar function registry supported by this dialect.
    /// PT-br: Obtém o registry de funcoes escalares suportadas por este dialeto.
    /// </summary>
    Dialect.FunctionDictionaryProcess Functions { get; }

    /// <summary>
    /// EN: Gets the stored procedure registry supported by this dialect.
    /// PT-br: Obtém o registry de procedimentos armazenados suportados por este dialeto.
    /// </summary>
    IDictionaryProcess<ProcedureDef> Procedures { get; }
}
