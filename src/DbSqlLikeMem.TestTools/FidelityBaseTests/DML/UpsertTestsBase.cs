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
    [Fact]
    public void UpsertInsertThenUpdateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = $"{users}_{uId}";
            var resultMock = serviceTest.RunUpsertInsertThenUpdate(tableName);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunUpsertInsertThenUpdate(tableName);
                    resultMock.Should().Be(resultContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}

