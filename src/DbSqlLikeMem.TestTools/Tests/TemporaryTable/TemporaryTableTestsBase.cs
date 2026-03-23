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
    [Fact]
    public void CreateTemporaryTable_AsSelect_ThenSelect_ShouldReturnProjectedRows()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new TemporaryTableScenario<T>(dialect);
        var serviceTest = new TemporaryTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users", uId);
        try
        {
            var resultMock = serviceTest.RunCreateTemporaryTableAsSelectThenSelect("Users", uId);
            if (RunContainerTests.Value
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new TemporaryTableScenario<T2>(dialect);
                var serviceTestContainer = new TemporaryTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario("Users", uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunCreateTemporaryTableAsSelectThenSelect("Users", uId);
                    Assert.Equal(resultMock, resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario("Users", uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario("Users", uId);
        }
    }

    /// <summary>
    /// EN: Verifies that rolling back a transaction clears rows written to a temporary users table.
    /// PT: Verifica se o rollback de uma transacao limpa as linhas gravadas em uma tabela temporaria de usuarios.
    /// </summary>
    [Fact]
    public void CreateTemporaryUsersTable_Rollback_ShouldClearRows()
    {
        var tableName = $"temp_users_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new TemporaryUsersScenario<T>(dialect);
        var serviceTest = new TemporaryTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(tableName);
        try
        {
            serviceTest.RunTempTableRollback(tableName);
            if (RunContainerTests.Value
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new TemporaryUsersScenario<T2>(dialect);
                var serviceTestContainer = new TemporaryTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(tableName);
                try
                {
                    serviceTestContainer.RunTempTableRollback(tableName);
                }
                finally
                {
                    serviceTestContainer.DropScenario(tableName);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(tableName);
        }
    }

    /// <summary>
    /// EN: Verifies that a temporary users table is not visible from a secondary connection.
    /// PT: Verifica se uma tabela temporaria de usuarios nao fica visivel a partir de uma conexao secundaria.
    /// </summary>
    [Fact]
    public void CreateTemporaryUsersTable_CrossConnectionIsolation_ShouldReturnZero()
    {
        var tableName = $"temp_users_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new TemporaryUsersScenario<T>(dialect);
        var serviceTest = new TemporaryTableServiceTest<T>(connMock, testScenario, dialect, connectionMock);
        serviceTest.CreateScenario(tableName);
        try
        {
            var resultMock = serviceTest.RunTemporaryTableCrossConnectionIsolation(tableName);
            if (RunContainerTests.Value
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new TemporaryUsersScenario<T2>(dialect);
                var serviceTestContainer = new TemporaryTableServiceTest<T2>(
                    connContainer,
                    testScenarioContainer,
                    dialect,
                    () => connectionContainer(connectionString));
                serviceTestContainer.CreateScenario(tableName);
                try
                {
                    var resultContainer = serviceTestContainer.RunTemporaryTableCrossConnectionIsolation(tableName);
                    Assert.Equal(resultMock, resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(tableName);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(tableName);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
