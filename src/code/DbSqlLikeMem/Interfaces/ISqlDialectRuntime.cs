namespace DbSqlLikeMem;

/// <summary>
/// EN: Composes the runtime capability subinterfaces for a SQL dialect.
/// PT-br: Compõe as subinterfaces de capacidade de runtime para um dialeto SQL.
/// </summary>
internal interface ISqlDialectRuntime : ISqlDialectDdl, ISqlDialectQueryFeatures, ISqlDialectSemantics, ISqlDialectCompatibility, ISqlServerDialectFeatures, IOracleDialectFeatures
{
}
