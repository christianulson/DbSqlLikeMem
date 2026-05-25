using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared merge fidelity tests across mock and container runs.
/// PT-br: Fornece testes de fidelidade de merge compartilhados entre execucoes mock e container.
/// </summary>
public abstract class MergeTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an initial merge inserts a row and a second merge updates the same row.
    /// PT-br: Verifica se um merge inicial insere uma linha e um segundo merge atualiza a mesma linha.
    /// </summary>
    [FidelityFact]
    public async Task MergeInsertThenUpdateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<UsersScenario, DmlMutationMergeInsertThenUpdateServiceTest>();
    }
}

