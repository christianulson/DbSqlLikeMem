namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class PostgreSqlTransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
    /// <inheritdoc />
    protected override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = new NpgsqlDbMock(version) { ThreadSafe = threadSafe };
        return () =>
        {
            var connection = new NpgsqlConnectionMock(db);
            connection.Open();
            return connection;
        };
    }

    [Fact]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    [Fact]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    [Fact]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    [Fact]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    [Theory]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    [MemberDataNpgsqlVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    [Theory]
    [Trait("Category", "PostgreSqlTransactionReliability")]
    [MemberDataNpgsqlVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
