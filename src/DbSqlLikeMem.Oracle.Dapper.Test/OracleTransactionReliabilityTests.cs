using System.Data.Common;

namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Validates transactional reliability additions for P11 scenarios.
/// PT: Valida as adições de confiabilidade transacional para cenários do P11.
/// </summary>
public sealed class OracleTransactionReliabilityTests : DapperTransactionConcurrencyTestsBase
{
    /// <inheritdoc />
    protected override Func<DbConnectionMockBase> CreateOpenConnectionFactory(bool threadSafe, int? version = null)
    {
        var db = new OracleDbMock(version) { ThreadSafe = threadSafe };
        return () =>
        {
            var connection = new OracleConnectionMock(db);
            connection.Open();
            return connection;
        };
    }

    [Fact]
    [Trait("Category", "OracleTransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    [Fact]
    [Trait("Category", "OracleTransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    [Fact]
    [Trait("Category", "OracleTransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    [Fact]
    [Trait("Category", "OracleTransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    [Theory]
    [Trait("Category", "OracleTransactionReliability")]
    [MemberDataOracleVersion]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    [Theory]
    [Trait("Category", "OracleTransactionReliability")]
    [MemberDataOracleVersion]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
