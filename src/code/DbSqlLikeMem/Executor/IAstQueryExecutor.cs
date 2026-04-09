namespace DbSqlLikeMem;

/// <summary>
/// EN: Abstraction for executing the AST produced by <see cref="SqlQueryParser"/>.
/// PT: Abstracao para executar o AST produzido por <see cref="SqlQueryParser"/>.
///
/// EN: The interface lets the dialect-specific executor vary without spreading dependencies through the rest of the code.
/// PT: A interface permite variar o executor especifico de cada dialeto sem espalhar dependencias pelo restante do codigo.
/// </summary>
internal interface IAstQueryExecutor
{
    /// <summary>
    /// EN: Executes a SELECT AST against the current benchmark tables.
    /// PT: Executa um AST de SELECT contra as tabelas atuais do benchmark.
    /// </summary>
    TableResultMock ExecuteSelect(SqlSelectQuery q);

    /// <summary>
    /// EN: Executes a UNION AST against the current benchmark tables.
    /// PT: Executa um AST de UNION contra as tabelas atuais do benchmark.
    /// </summary>
    TableResultMock ExecuteUnion(
        IReadOnlyList<SqlSelectQuery> parts,
        IReadOnlyList<bool> allFlags,
        IReadOnlyList<SqlOrderByItem>? orderBy = null,
        SqlRowLimit? rowLimit = null,
        string? sqlContextForErrors = null);
}
