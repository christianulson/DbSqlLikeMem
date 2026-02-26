namespace DbSqlLikeMem.SqlServer.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class SqlServerTransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
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

    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    [Fact]
    [Trait("Category", "SqlServerTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    [Theory]
    [Trait("Category", "SqlServerTransactionReliability")]
    [MemberDataSqlServerVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    [Theory]
    [Trait("Category", "SqlServerTransactionReliability")]
    [MemberDataSqlServerVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
