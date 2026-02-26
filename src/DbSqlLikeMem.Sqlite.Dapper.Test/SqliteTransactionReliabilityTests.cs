namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class SqliteTransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
    /// <inheritdoc />
    protected override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = new SqliteDbMock(version) { ThreadSafe = threadSafe };
        return () =>
        {
            var connection = new SqliteConnectionMock(db);
            connection.Open();
            return connection;
        };
    }

    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    [Fact]
    [Trait("Category", "SqliteTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    [Theory]
    [Trait("Category", "SqliteTransactionReliability")]
    [MemberDataSqliteVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    [Theory]
    [Trait("Category", "SqliteTransactionReliability")]
    [MemberDataSqliteVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
