namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT-br: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class SqliteTransactionReliabilityTests(
        ITestOutputHelper helper
    ) : ProviderDapperTransactionReliabilityTestsBase<SqliteDbMock, SqliteConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override SqliteDbMock CreateDb(int? version, bool threadSafe)
        => new(version) { ThreadSafe = threadSafe };

    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db)
        => new(db);

    /// <summary>
    /// EN: Ensures rolling back to a savepoint restores the intermediate state.
    /// PT-br: Garante que rollback para savepoint restaure o estado intermediário.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    /// <summary>
    /// EN: Ensures nested savepoints roll back to the selected outer snapshot.
    /// PT-br: Garante que savepoints aninhados façam rollback para o snapshot externo selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void NestedSavepointsShouldRollbackToOuterSnapshot()
        => AssertNestedSavepointsRollbackToOuterSnapshot();

    /// <summary>
    /// EN: Ensures the simplified isolation model is deterministic and visible.
    /// PT-br: Garante que o modelo simplificado de isolamento seja determinístico e visível.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    /// <summary>
    /// EN: Ensures savepoint release support follows provider compatibility rules.
    /// PT-br: Garante que o suporte a release de savepoint siga as regras de compatibilidade do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    /// <summary>
    /// EN: Ensures concurrent writes keep data consistent when thread safety is enabled.
    /// PT-br: Garante que escritas concorrentes mantenham dados consistentes com thread safety habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    /// <summary>
    /// EN: Ensures concurrent commit and rollback keep only committed writes across provider versions.
    /// PT-br: Garante que commit e rollback concorrentes mantenham apenas gravações confirmadas entre versões do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT-br: Versão do provedor em teste.</param>
    [Theory]
    [Trait("Category", "SqliteTransactionReliability")]
    [MemberDataSqliteVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    /// <summary>
    /// EN: Ensures concurrent commits persist combined writes deterministically across provider versions.
    /// PT-br: Garante que commits concorrentes persistam gravações combinadas de forma determinística entre versões do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT-br: Versão do provedor em teste.</param>
    [Theory]
    [Trait("Category", "SqliteTransactionReliability")]
    [MemberDataSqliteVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
