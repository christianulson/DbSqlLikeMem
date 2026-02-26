using System.Data.Common;

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

    /// <summary>
    /// EN: Verifies that rolling back to a savepoint restores the intermediate transactional state.
    /// PT: Verifica se o rollback para um savepoint restaura o estado transacional intermediário.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void SavepointRollbackShouldRestoreIntermediateState()
        => AssertSavepointRollbackRestoresIntermediateState();

    /// <summary>
    /// EN: Verifies that the transaction isolation level is exposed in a deterministic way.
    /// PT: Verifica se o nível de isolamento da transação é exposto de forma determinística.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void IsolationLevelShouldBeExposedDeterministically()
        => AssertIsolationLevelExposedDeterministically();

    /// <summary>
    /// EN: Verifies that savepoint release compatibility follows provider-specific behavior.
    /// PT: Verifica se a compatibilidade de liberação de savepoint segue o comportamento específico do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void ReleaseSavepointCompatibilityShouldBeProviderSpecific()
        => AssertReleaseSavepointCompatibilityIsProviderSpecific();

    /// <summary>
    /// EN: Verifies that concurrent inserts remain consistent when thread-safe mode is enabled.
    /// PT: Verifica se inserções concorrentes permanecem consistentes quando o modo thread-safe está habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2TransactionReliability")]
    public void ConcurrentInsertsShouldRemainConsistentWhenThreadSafeEnabled()
        => AssertConcurrentInsertsRemainConsistentWhenThreadSafeEnabled();

    /// <summary>
    /// EN: Verifies that concurrent commit and rollback operations keep the expected state across Db2 versions.
    /// PT: Verifica se operações concorrentes de commit e rollback mantêm o estado esperado entre versões do Db2.
    /// </summary>
    [Theory]
    [Trait("Category", "Db2TransactionReliability")]
    [MemberDataDb2Version]
    public void ConcurrentCommitAndRollback_ShouldKeepExpectedStateAcrossVersions(int version)
        => AssertConcurrentCommitAndRollbackKeepsExpectedState(version);

    /// <summary>
    /// EN: Verifies that concurrent commits persist combined writes across Db2 versions.
    /// PT: Verifica se commits concorrentes persistem gravações combinadas entre versões do Db2.
    /// </summary>
    [Theory]
    [Trait("Category", "Db2TransactionReliability")]
    [MemberDataDb2Version]
    public void ConcurrentCommits_ShouldPersistCombinedWritesAcrossVersions(int version)
        => AssertConcurrentCommitsPersistCombinedWrites(version);
}
