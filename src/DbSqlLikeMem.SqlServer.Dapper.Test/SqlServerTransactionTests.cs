namespace DbSqlLikeMem.SqlServer.Dapper.Test;
/// <summary>
/// EN: Defines the class SqlServerTransactionTests.
/// PT: Define a classe SqlServerTransactionTests.
/// </summary>
public sealed class SqlServerTransactionTests(
    ITestOutputHelper helper
) : DapperTransactionTestsBase<SqlServerDbMock, SqlServerConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override SqlServerConnectionMock CreateConnection(SqlServerDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies committed transactions persist their data changes.
    /// PT: Verifica se transacoes confirmadas persistem suas alteracoes de dados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionCommitShouldPersistData_Test() => TransactionCommitShouldPersistData();

    /// <summary>
    /// EN: Verifies rolled back transactions do not persist data changes.
    /// PT: Verifica se transacoes revertidas nao persistem alteracoes de dados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionRollbackShouldNotPersistData_Test() => TransactionRollbackShouldNotPersistData();

    /// <summary>
    /// EN: Verifies transaction rollback restores connection-scoped temporary tables.
    /// PT: Verifica se o rollback da transacao restaura tabelas temporarias do escopo da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper_Test() => TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper();

    /// <summary>
    /// EN: Verifies rollback to savepoint restores the snapshot of connection temporary tables.
    /// PT: Verifica se rollback para savepoint restaura o snapshot das tabelas temporarias da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper_Test() => RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper();

    /// <summary>
    /// EN: Verifies resetting volatile data clears rows and resets identity values.
    /// PT: Verifica se redefinir dados volateis limpa as linhas e reinicia os valores de identidade.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper_Test() => ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper();

    /// <summary>
    /// EN: Verifies database-level volatile reset respects the global temporary tables flag.
    /// PT: Verifica se o reset de dados volateis no banco respeita a flag de tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper_Test() => ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper();

    /// <summary>
    /// EN: Verifies database-level volatile reset keeps table definitions intact.
    /// PT: Verifica se o reset de dados volateis no banco mantem as definicoes das tabelas intactas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper_Test() => ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper();

    /// <summary>
    /// EN: Verifies database-level volatile reset does not affect connection temporary tables.
    /// PT: Verifica se o reset de dados volateis no banco nao afeta tabelas temporarias da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper_Test() => ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows from global temporary tables.
    /// PT: Verifica se redefinir todos os dados volateis limpa as linhas das tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper_Test() => ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data invalidates existing savepoints.
    /// PT: Verifica se redefinir todos os dados volateis invalida savepoints existentes.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper_Test() => ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper();

    /// <summary>
    /// EN: Verifies connection temporary tables remain isolated between different connections.
    /// PT: Verifica se tabelas temporarias da conexao permanecem isoladas entre conexoes diferentes.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper_Test() => ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection clears its session-specific state.
    /// PT: Verifica se fechar uma conexao limpa seu estado especifico de sessao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void Close_ShouldClearConnectionSessionState_Dapper_Test() => Close_ShouldClearConnectionSessionState_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection preserves permanent and globally shared state.
    /// PT: Verifica se fechar uma conexao preserva o estado permanente e globalmente compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState_Dapper_Test() => Close_ShouldPreservePermanentAndGlobalSharedState_Dapper();

    /// <summary>
    /// EN: Verifies reopening after close starts a fresh session while preserving shared state.
    /// PT: Verifica se reabrir apos fechar inicia uma nova sessao preservando o estado compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransaction")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper_Test() => OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper();
}
