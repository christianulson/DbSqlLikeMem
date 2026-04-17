using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared batch RETURNING fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de batch RETURNING compartilhados entre execucoes mock e container.
/// </summary>
public abstract class BatchReturningTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an INSERT RETURNING batch persists one row and returns one reader row for the current provider.
    /// PT: Verifica se um batch INSERT RETURNING persiste uma linha e retorna uma linha no reader para o provedor atual.
    /// </summary>
    [Fact]
    public async Task BatchReturningInsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, BatchInsertReturningServiceTest>();
    }
}
