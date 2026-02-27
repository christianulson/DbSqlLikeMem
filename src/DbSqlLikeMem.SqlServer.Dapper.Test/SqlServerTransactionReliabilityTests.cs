namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class SqlServerTransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
    /// <inheritdoc />
    protected override bool SupportsReleaseSavepoint => false;

    /// <inheritdoc />
    protected override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = new SqlServerDbMock(version) { ThreadSafe = threadSafe };
        return () =>
        {
            var connection = new SqlServerConnectionMock(db);
            connection.Open();
            return connection;
        };
    }

    /// <summary>
    /// EN: Ensures rolling back to a savepoint restores the intermediate state.
    /// PT: Garante que rollback para savepoint restaure o estado intermediário.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    /// <summary>
    /// EN: Ensures the simplified isolation model is deterministic and visible.
    /// PT: Garante que o modelo simplificado de isolamento seja determinístico e visível.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    /// <summary>
    /// EN: Ensures savepoint release support follows provider compatibility rules.
    /// PT: Garante que o suporte a release de savepoint siga as regras de compatibilidade do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    /// <summary>
    /// EN: Ensures concurrent writes keep data consistent when thread safety is enabled.
    /// PT: Garante que escritas concorrentes mantenham dados consistentes com thread safety habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    /// <summary>
    /// EN: Ensures concurrent commit and rollback keep only committed writes across provider versions.
    /// PT: Garante que commit e rollback concorrentes mantenham apenas gravações confirmadas entre versões do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versão do provedor em teste.</param>
    [Theory]
    [Trait("Category", "SqlServerTransactionReliability")]
    [MemberDataSqlServerVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    /// <summary>
    /// EN: Ensures concurrent commits persist combined writes deterministically across provider versions.
    /// PT: Garante que commits concorrentes persistam gravações combinadas de forma determinística entre versões do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versão do provedor em teste.</param>
    [Theory]
    [Trait("Category", "SqlServerTransactionReliability")]
    [MemberDataSqlServerVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
