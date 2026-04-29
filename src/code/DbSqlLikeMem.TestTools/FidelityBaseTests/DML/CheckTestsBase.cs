using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared DML fidelity tests for check-constraint workflows across mock and container runs.
/// PT: Fornece testes de fidelidade DML compartilhados para fluxos de restricao check entre mock e container.
/// </summary>
public abstract class CheckTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that inserting rows with valid check values persists the expected defaults and nullable values for the current provider.
    /// PT: Verifica se inserir linhas com valores validos de check persiste os defaults esperados e os valores anulaveis para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CheckConstraintsValidInsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<CheckConstraintsScenario, CheckConstraintsValidInsertServiceTest>())
            .Should()
            .BeEquivalentTo(new
            {
                affected = 1,
                requiredNoDefault = 10,
                nullableWithDefault = 7,
                nullableNoDefault = (int?)null,
                checkedRequired = 5,
                checkedNullable = (int?)null
            });
    }

    /// <summary>
    /// EN: Verifies that inserting a row that violates a check constraint is rejected for the current provider.
    /// PT: Verifica se inserir uma linha que viola uma restricao check e rejeitado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CheckConstraintsInvalidInsertTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        Convert.ToBoolean(await testService.RunTestAsync<CheckConstraintsScenario, CheckConstraintsInvalidInsertServiceTest>())
            .Should()
            .BeTrue();
    }

    /// <summary>
    /// EN: Verifies that updating a row into an invalid check state is rejected and leaves the persisted row unchanged for the current provider.
    /// PT: Verifica se atualizar uma linha para um estado invalido de check e rejeitado e deixa a linha persistida inalterada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CheckConstraintsInvalidUpdateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        (await testService.RunTestAsync<CheckConstraintsScenario, CheckConstraintsInvalidUpdateServiceTest>())
            .Should()
            .BeEquivalentTo(new
            {
                requiredNoDefault = 10,
                nullableWithDefault = 7,
                nullableNoDefault = (int?)null,
                checkedRequired = 5,
                checkedNullable = (int?)null
            });
    }
}
