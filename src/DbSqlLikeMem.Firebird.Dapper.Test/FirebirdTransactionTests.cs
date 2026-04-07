namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird transaction and lifecycle scenarios against the Dapper provider.
/// PT: Cobre cenarios de transacao e ciclo de vida do Firebird contra o provedor Dapper.
/// </summary>
public sealed class FirebirdTransactionTests(
    ITestOutputHelper helper
) : DapperTransactionTestsBase<FirebirdDbMock, FirebirdConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateConnection(FirebirdDbMock db) => new(db);

    /// <summary>
    /// EN: Verifies committing a transaction persists inserted data.
    /// PT: Verifica se confirmar uma transacao persiste os dados inseridos.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void TransactionCommit_ShouldPersistData_Test() => TransactionCommitShouldPersistData();

    /// <summary>
    /// EN: Verifies rolling back a transaction prevents inserted data from persisting.
    /// PT: Verifica se reverter uma transacao impede que os dados inseridos sejam persistidos.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void TransactionRollback_ShouldNotPersistData_Test() => TransactionRollbackShouldNotPersistData();

    /// <summary>
    /// EN: Verifies a rollback restores connection-scoped temporary table contents.
    /// PT: Verifica se um rollback restaura o conteudo das tabelas temporarias de escopo da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void TransactionRollback_ShouldRestoreConnectionTemporaryTable_Test() => TransactionRollback_ShouldRestoreConnectionTemporaryTable_Dapper();

    /// <summary>
    /// EN: Verifies rolling back to a savepoint restores the temporary table snapshot.
    /// PT: Verifica se reverter para um savepoint restaura o snapshot da tabela temporaria.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Test() => RollbackToSavepoint_ShouldRestoreConnectionTemporaryTableSnapshot_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows, resets identities, and clears session temp tables.
    /// PT: Verifica se resetar todos os dados volateis limpa linhas, reinicia identidades e limpa tabelas temporarias da sessao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Test() => ResetAllVolatileData_ShouldClearRowsAndResetIdentity_Dapper();

    /// <summary>
    /// EN: Verifies database volatile-data reset respects the global temporary table flag for Firebird rows.
    /// PT: Verifica se o reset de dados volateis no banco respeita a flag de tabelas temporarias globais para linhas do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetVolatileData_OnDb_ShouldRespectGlobalTemporaryTablesFlag_Test()
    {
        var db = CreateDb();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var connection = CreateConnection(db);
        connection.Open();
        connection.Execute("CREATE GLOBAL TEMPORARY TABLE gtmp_users AS SELECT id, name FROM users");

        var globalTemp = connection.GetTable("gtmp_users");
        globalTemp.Should().ContainSingle();

        db.ResetVolatileData(includeGlobalTemporaryTables: false);
        users.Should().BeEmpty();
        globalTemp.Should().ContainSingle();

        db.ResetVolatileData(includeGlobalTemporaryTables: true);
        globalTemp.Should().ContainSingle();
        globalTemp.Columns.Count.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies resetting volatile data on the database preserves table definitions.
    /// PT: Verifica se resetar dados volateis no banco preserva as definicoes das tabelas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Test() => ResetVolatileData_OnDb_ShouldKeepTableDefinitions_Dapper();

    /// <summary>
    /// EN: Verifies database volatile-data reset does not affect connection-scoped temporary tables.
    /// PT: Verifica se o reset de dados volateis no banco nao afeta tabelas temporarias de escopo da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Test() => ResetVolatileData_OnDb_ShouldNotAffectConnectionTemporaryTables_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data clears rows from global temporary tables.
    /// PT: Verifica se resetar todos os dados volateis limpa as linhas das tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Test() => ResetAllVolatileData_ShouldClearGlobalTemporaryTableRows_Dapper();

    /// <summary>
    /// EN: Verifies resetting all volatile data invalidates existing savepoints.
    /// PT: Verifica se resetar todos os dados volateis invalida os savepoints existentes.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ResetAllVolatileData_ShouldInvalidateSavepoints_Test() => ResetAllVolatileData_ShouldInvalidateSavepoints_Dapper();

    /// <summary>
    /// EN: Verifies connection-scoped temporary tables remain isolated between different connections.
    /// PT: Verifica se tabelas temporarias de escopo da conexao permanecem isoladas entre conexoes diferentes.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Test() => ConnectionTemporaryTables_ShouldBeIsolatedBetweenConnections_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection clears session-scoped transactional and temporary state.
    /// PT: Verifica se fechar uma conexao limpa o estado transacional e temporario de escopo da sessao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void Close_ShouldClearConnectionSessionState_Test() => Close_ShouldClearConnectionSessionState_Dapper();

    /// <summary>
    /// EN: Verifies closing a connection preserves permanent tables and shared global temporary state.
    /// PT: Verifica se fechar uma conexao preserva tabelas permanentes e o estado compartilhado de tabelas temporarias globais.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void Close_ShouldPreservePermanentAndGlobalSharedState_Test() => Close_ShouldPreservePermanentAndGlobalSharedState_Dapper();

    /// <summary>
    /// EN: Verifies reopening after close starts a fresh session while preserving shared database state.
    /// PT: Verifica se reabrir apos fechar inicia uma nova sessao preservando o estado compartilhado do banco.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransaction")]
    public void OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Test() => OpenAfterClose_ShouldStartFreshSessionPreservingSharedState_Dapper();
}
