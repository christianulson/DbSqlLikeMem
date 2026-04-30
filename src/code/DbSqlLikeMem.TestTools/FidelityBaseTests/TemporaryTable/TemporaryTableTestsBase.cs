using DbSqlLikeMem.TestTools.TemporaryTable;

namespace DbSqlLikeMem.TestTools.Tests.TemporaryTable;

/// <summary>
/// EN: Provides shared temporary-table fidelity tests for source creation and projected row verification across mock and container runs.
/// PT: Fornece testes de fidelidade de tabela temporaria para criacao da origem e verificacao de linhas projetadas entre mock e container.
/// </summary>
public abstract class TemporaryTableTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that creating a temporary table from a filtered source query returns the expected projected rows.
    /// PT: Verifica se criar uma tabela temporaria a partir de uma consulta filtrada retorna as linhas projetadas esperadas.
    /// </summary>
    [FidelityFact]
    public async Task CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows()
    {
        var result = (List<int>?)await RunFidelityTestAsync<TemporaryTableScenario>(
            (s, a) => s.RunCreateTemporaryTableAsSelectThenSelectAsync(a));

        result.Should().Equal([1, 2]);
    }

    /// <summary>
    /// EN: Verifies creating a temporary table from a filtered source query returns the expected projected rows.
    /// PT: Verifica se criar uma tabela temporaria a partir de uma consulta filtrada retorna as linhas projetadas esperadas.
    /// </summary>
    [FidelityFact]
    public Task TempTableCreateAndUseTest()
        => CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows();

    /// <summary>
    /// EN: Verifies that rolling back a transaction clears rows written to a temporary users table.
    /// PT: Verifica se o rollback de uma transacao limpa as linhas gravadas em uma tabela temporaria de usuarios.
    /// </summary>
    [FidelityFact]
    public async Task CreateTemporaryUsersTable_Rollback_ShouldClearRows()
    {
        var result = await RunFidelityTestAsync<TemporaryUsersScenario>(
            async (s, a) =>
            {
                await s.RunTempTableRollback();
                return null;
            });

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies rolling back a transaction clears rows written to a temporary users table.
    /// PT: Verifica se o rollback de uma transacao limpa as linhas gravadas em uma tabela temporaria de usuarios.
    /// </summary>
    [FidelityFact]
    public Task TempTableRollbackTest()
        => CreateTemporaryUsersTable_Rollback_ShouldClearRows();

    /// <summary>
    /// EN: Verifies that a temporary users table accepts inserts and returns the expected row count.
    /// PT: Verifica se uma tabela temporaria de usuarios aceita inserts e retorna a contagem esperada de linhas.
    /// </summary>
    [FidelityFact]
    public async Task CreateTemporaryUsersTable_CreateAndUse_ShouldReturnOne()
    {
        var result = (int?)await RunFidelityTestAsync<TemporaryUsersScenario>(
            (s, a) => s.RunTempTableCreateAndUse(a));

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a temporary users table is not visible from a secondary connection.
    /// PT: Verifica se uma tabela temporaria de usuarios nao fica visivel a partir de uma conexao secundaria.
    /// </summary>
    [FidelityFact]
    public async Task CreateTemporaryUsersTable_CrossConnectionIsolation_ShouldReturnZero()
    {
        var result = (int?)await RunFidelityTestAsync<TemporaryUsersScenario>(
            (s, a) => s.RunTemporaryTableCrossConnectionIsolation(a));

        result.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that a temporary users table is not visible from a secondary connection.
    /// PT: Verifica se uma tabela temporaria de usuarios nao fica visivel a partir de uma conexao secundaria.
    /// </summary>
    [FidelityFact]
    public Task TempTableCrossConnectionIsolationTest()
        => CreateTemporaryUsersTable_CrossConnectionIsolation_ShouldReturnZero();

    private async Task<object?> RunFidelityTestAsync<TScenario>(
        Func<TemporaryTableServiceOpsTest, object[], Task<object?>> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        return await testService.RunTestAsync<TScenario, TemporaryTableServiceOpsTest>(runTest, args);
    }
}

