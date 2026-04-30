using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared upsert fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de upsert compartilhados entre execucoes mock e container.
/// </summary>
public abstract class UpsertTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an initial upsert inserts a row and a second upsert updates the same row.
    /// PT: Verifica se um upsert inicial insere uma linha e um segundo upsert atualiza a mesma linha.
    /// </summary>
    [FidelityFact]
    public async Task UpsertInsertThenUpdateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<UsersScenario, DmlMutationUpsertInsertThenUpdateServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that the provider-specific upsert benchmark updates the existing row for the current provider.
    /// PT: Verifica se o benchmark de upsert especifico do provedor atualiza a linha existente para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task UpsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<UsersScenario, DmlMutationUpsertServiceTest>();
    }
}


