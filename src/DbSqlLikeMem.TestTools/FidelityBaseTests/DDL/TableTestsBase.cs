using DbSqlLikeMem.TestTools.DDL;

namespace DbSqlLikeMem.TestTools.Tests.DDL;

/// <summary>
/// EN: Provides shared DDL fidelity tests for table scenarios across mock and container runs.
/// PT: Fornece testes de fidelidade DDL compartilhados para cenarios de tabela entre mock e container.
/// </summary>
public abstract class TableTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that table creation works for the current provider.
    /// PT: Verifica se a criacao de tabela funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void CreateTableTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new CreateTableScenario<T>();
        var serviceTest = new CreateTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users", uId);
        serviceTest.RunTest("Users", uId);
        serviceTest.DropScenario("Users", uId);

        if (IsTableContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var testScenarioContainer = new CreateTableScenario<T2>();
            var serviceTestContainer = new CreateTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario("Users", uId);
            serviceTestContainer.RunTest("Users", uId);
            serviceTestContainer.DropScenario("Users", uId);
        }
    }

    /// <summary>
    /// EN: Verifies that table creation with a foreign key works for the current provider.
    /// PT: Verifica se a criacao de tabela com chave estrangeira funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void CreateTableWithFKTest()
    {
        var uId = NewToken();
        var users = "Users";
        var orders = "Orders";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new CreateTableWithFKScenario<T>();
        var serviceTest = new CreateTableWithFKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);
        serviceTest.RunTest(users, orders, uId);
        serviceTest.DropScenario(users, orders, uId);

        if (IsTableContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var testScenarioContainer = new CreateTableWithFKScenario<T2>();
            var serviceTestContainer = new CreateTableWithFKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario(users, uId);
            serviceTestContainer.RunTest(users, orders, uId);
            serviceTestContainer.DropScenario(users, orders, uId);
        }
    }

    /// <summary>
    /// EN: Verifies that a table created with a foreign key accepts a valid referenced insert for the current provider.
    /// PT: Verifica se uma tabela criada com chave estrangeira aceita um insert referenciado valido para o provedor atual.
    /// </summary>
    [Fact]
    public void CreateTableWithFKInsertTest()
    {
        var uId = NewToken();
        var users = "Users";
        var orders = "Orders";
        var usersTable = $"{users}_{uId}";
        var ordersTable = $"{orders}_{uId}";
        var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new CreateTableWithFKScenario<T>();
        var serviceTest = new CreateTableWithFKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            serviceTest.RunTest(users, orders, uId);
            serviceTest.ExecuteNonQuery(dialect.InsertUser(usersTable, 1, "Ana"));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTable, usersTable, 10, 1, "first", "o-10", 12.34m, 2, true, orderedAt));

            var joinCount = Convert.ToInt32(serviceTest.ExecuteScalar(dialect.CountJoinForUser(usersTable, ordersTable, 1)));
            joinCount.Should().Be(1);

            if (IsTableContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new CreateTableWithFKScenario<T2>();
                var serviceTestContainer = new CreateTableWithFKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    serviceTestContainer.RunTest(users, orders, uId);
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(usersTable, 1, "Ana"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTable, usersTable, 10, 1, "first", "o-10", 12.34m, 2, true, orderedAt));

                    var joinCountContainer = Convert.ToInt32(serviceTestContainer.ExecuteScalar(dialect.CountJoinForUser(usersTable, ordersTable, 1)));
                    joinCount.Should().Be(joinCountContainer);
                }
                finally
                {
                    serviceTestContainer.DropScenario(users, orders, uId);
                }
            }
        }
        finally
        {
            serviceTest.DropScenario(users, orders, uId);
        }
    }

    /// <summary>
    /// EN: Verifies that table dropping works for the current provider.
    /// PT: Verifica se a remocao de tabela funciona para o provedor atual.
    /// </summary>
    [Fact]
    public void DropTableTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new DropTableScenario<T>();
        var serviceTest = new DropTableServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users", "Orders", uId);
        serviceTest.RunTest("Users", uId);

        if (IsTableContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var testScenarioContainer = new DropTableScenario<T2>();
            var serviceTestContainer = new DropTableServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario("Users", "Orders", uId);
            serviceTestContainer.RunTest("Users", uId);
        }
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
