namespace DbSqlLikeMem.Oracle.Test;
/// <summary>
/// EN: Defines the class OracleTransactionTests.
/// PT: Define a classe OracleTransactionTests.
/// </summary>
public sealed class OracleTransactionTests(
    ITestOutputHelper helper
) : DapperTransactionTestsBase<OracleDbMock, OracleConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies committed transactions persist inserted data.
    /// PT: Verifica se transacoes confirmadas persistem os dados inseridos.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void TransactionCommitShouldPersistData_Test() => TransactionCommitShouldPersistData();

    /// <summary>
    /// EN: Verifies rolled back transactions do not persist inserted data.
    /// PT: Verifica se transacoes revertidas nao persistem os dados inseridos.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void TransactionRollbackShouldNotPersistData_Test() => TransactionRollbackShouldNotPersistData();

    /// <summary>
    /// EN: Verifies rollback restores connection-scoped temporary table contents.
    /// PT: Verifica se rollback restaura o conteudo das tabelas temporarias de escopo da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper_Test() => TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper();

    /// <summary>
    /// EN: Verifies rollback to a savepoint restores the temporary table snapshot.
    /// PT: Verifica se rollback para um savepoint restaura o snapshot da tabela temporaria.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper_Test() => RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows and resets identities.
    /// PT: Verifica se resetar todos os dados volateis limpa linhas e reinicia identidades.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper_Test() => ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper();

    /// <summary>
    /// EN: Verifies volatile-data reset respects the global temporary table inclusion flag.
    /// PT: Verifica se o reset de dados volateis respeita a flag de inclusao de tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper_Test() => ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Dapper();

    /// <summary>
    /// EN: Verifies volatile-data reset keeps table definitions intact.
    /// PT: Verifica se o reset de dados volateis mantem as definicoes das tabelas intactas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper_Test() => ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper();

    /// <summary>
    /// EN: Verifies volatile-data reset on the database does not affect connection temporary tables.
    /// PT: Verifica se o reset de dados volateis no banco nao afeta tabelas temporarias da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper_Test() => ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows from global temporary tables.
    /// PT: Verifica se resetar todos os dados volateis limpa as linhas das tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper_Test() => ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data invalidates savepoints.
    /// PT: Verifica se resetar todos os dados volateis invalida savepoints.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper_Test() => ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper();

    /// <summary>
    /// EN: Verifies connection temporary tables remain isolated between different connections.
    /// PT: Verifica se tabelas temporarias de conexao permanecem isoladas entre conexoes diferentes.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper_Test() => ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection clears session-scoped state.
    /// PT: Verifica se fechar uma conexao limpa o estado de escopo da sessao.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void Close_ShouldClearConnectionSessionState_Dapper_Test() => Close_ShouldClearConnectionSessionState_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection preserves permanent and shared global state.
    /// PT: Verifica se fechar uma conexao preserva o estado permanente e o estado global compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState_Dapper_Test() => Close_ShouldPreservePermanentAndGlobalSharedState_Dapper();

    /// <summary>
    /// EN: Verifies reopening after close starts a fresh session while preserving shared state.
    /// PT: Verifica se reabrir apos fechar inicia uma nova sessao preservando o estado compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleTransaction")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper_Test() => OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper();
}
