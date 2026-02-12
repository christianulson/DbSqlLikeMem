namespace DbSqlLikeMem;

/// <summary>
/// Abstrai a execução do AST produzido por <see cref="SqlQueryParser"/>.
///
/// Objetivo: permitir trocar o executor por dialeto (MySQL/SQL Server/Oracle/Postgre)
/// sem espalhar dependências no restante do código.
/// </summary>
internal interface IAstQueryExecutor
{
    TableResultMock ExecuteSelect(SqlSelectQuery q);

    TableResultMock ExecuteUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy = null,
        SqlRowLimit? rowLimit = null,
        string? sqlContextForErrors = null);
}
