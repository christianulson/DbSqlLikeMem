namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for Firebird scenarios.
/// PT: Valida as adicoes de confiabilidade transacional para cenarios Firebird.
/// </summary>
public sealed class FirebirdTransactionReliabilityTests(
        ITestOutputHelper helper
    ) : ProviderDapperTransactionReliabilityTestsBase<FirebirdDbMock, FirebirdConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override FirebirdDbMock CreateDb(int? version, bool threadSafe)
        => new(version) { ThreadSafe = threadSafe };

    /// <inheritdoc />
    protected override FirebirdConnectionMock CreateConnection(FirebirdDbMock db)
        => new(db);

    /// <summary>
    /// EN: Ensures rolling back to a savepoint restores the intermediate state.
    /// PT: Garante que rollback para savepoint restaure o estado intermediario.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    /// <summary>
    /// EN: Ensures the simplified isolation model is deterministic and visible.
    /// PT: Garante que o modelo simplificado de isolamento seja deterministico e visivel.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    /// <summary>
    /// EN: Ensures savepoint release support follows provider compatibility rules.
    /// PT: Garante que o suporte a release de savepoint siga as regras de compatibilidade do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    /// <summary>
    /// EN: Ensures concurrent writes keep data consistent when thread safety is enabled.
    /// PT: Garante que escritas concorrentes mantenham dados consistentes com thread safety habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    /// <summary>
    /// EN: Ensures concurrent commit and rollback keep only committed writes across provider versions.
    /// PT: Garante que commit e rollback concorrentes mantenham apenas gravacoes confirmadas entre versoes do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versao do provedor em teste.</param>
    [Theory]
    [Trait("Category", "FirebirdTransactionReliability")]
    [MemberData(nameof(FirebirdVersions))]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    /// <summary>
    /// EN: Ensures concurrent commits persist combined writes deterministically across provider versions.
    /// PT: Garante que commits concorrentes persistam gravacoes combinadas de forma deterministica entre versoes do provedor.
    /// </summary>
    /// <param name="version">EN: Provider version under test. PT: Versao do provedor em teste.</param>
    [Theory]
    [Trait("Category", "FirebirdTransactionReliability")]
    [MemberData(nameof(FirebirdVersions))]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);

    /// <summary>
    /// EN: Returns the Firebird versions supported by the transaction reliability scenarios.
    /// PT: Retorna as versoes do Firebird suportadas pelos cenarios de confiabilidade transacional.
    /// </summary>
    public static IEnumerable<object[]> FirebirdVersions()
        => FirebirdDbVersions.Versions().Select(version => new object[] { version });
}
