namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class Db2TransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
    /// <inheritdoc />
    protected override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = new Db2DbMock(version) { ThreadSafe = threadSafe };
        return () =>
        {
            var connection = new Db2ConnectionMock(db);
            connection.Open();
            return connection;
        };
    }

    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    [Theory]
    [Trait("Category", "Db2TransactionReliability")]
    [MemberDataDb2Version]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    [Theory]
    [Trait("Category", "Db2TransactionReliability")]
    [MemberDataDb2Version]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
