using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared select fidelity tests across mock and container runs.
/// PT: Fornece testes compartilhados de fidelidade de select entre execucoes mock e container.
/// </summary>
public abstract class SelectTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that primary-key selection returns the expected row for the current provider.
    /// PT: Verifica se a selecao por chave primaria retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectByPkTest()
    {
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new SelectByPKServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario("Users", uId);
        var resultMock = serviceTest.RunTest("Users", uId);
        serviceTest.DropScenario("Users", uId);

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var testScenarioContainer = new SelectTableScenario<T2>(dialect);
            var serviceTestContainer = new SelectByPKServiceTest<T2>(connContainer, testScenarioContainer, dialect);
            serviceTestContainer.CreateScenario("Users", uId);
            var resultContainer = serviceTestContainer.RunTest("Users", uId);
            serviceTestContainer.DropScenario("Users", uId);

            resultMock.Should().Be(resultContainer);
        }
    }

    /// <summary>
    /// EN: Verifies that selecting all rows returns the expected row count for the current provider.
    /// PT: Verifica se o select de todas as linhas retorna a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectAllRowsCountTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));
            var resultMock = serviceTest.RunRowCountAfterSelect(tableName);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bob"));
                    var resultContainer = serviceTestContainer.RunRowCountAfterSelect(containerTableName);
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

    /// <summary>
    /// EN: Verifies that a simple CTE query returns the expected result for the current provider.
    /// PT: Verifica se uma consulta CTE simples retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectCteSimpleTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunCteSimple(tableName);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunCteSimple(containerTableName);
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

    /// <summary>
    /// EN: Verifies correlated scalar subqueries with CASE expressions for the current provider.
    /// PT: Verifica subconsultas escalares correlacionadas com expressoes CASE para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectScalarSubqueryCaseMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "A"),
            (12, 1, "B"),
            (13, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";
            var ordersTableName = dialect.Provider == ProviderId.Oracle
                ? orders.ToLowerInvariant()
                : $"{orders}_{uId}";

            var resultMock = serviceTest.RunSelectScalarCaseMatrix(usersTableName, ordersTableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";
                    var ordersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? orders.ToLowerInvariant()
                        : $"{orders}_{uId}";

                    var resultContainer = serviceTestContainer.RunSelectScalarCaseMatrix(usersTableNameContainer, ordersTableNameContainer);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies that a NOT EXISTS predicate returns the expected anti-join count for the current provider.
    /// PT: Verifica se um predicado NOT EXISTS retorna a contagem esperada de anti-join para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectNotExistsPredicateTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";
            var ordersTableName = dialect.Provider == ProviderId.Oracle
                ? orders.ToLowerInvariant()
                : $"{orders}_{uId}";

            var resultMock = serviceTest.RunSelectNotExistsPredicate(usersTableName, ordersTableName);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";
                    var ordersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? orders.ToLowerInvariant()
                        : $"{orders}_{uId}";

                    var resultContainer = serviceTestContainer.RunSelectNotExistsPredicate(usersTableNameContainer, ordersTableNameContainer);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies that a NOT IN subquery returns the expected anti-join count for the current provider.
    /// PT: Verifica se uma subconsulta NOT IN retorna a contagem esperada de anti-join para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectNotInSubqueryTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";
            var ordersTableName = dialect.Provider == ProviderId.Oracle
                ? orders.ToLowerInvariant()
                : $"{orders}_{uId}";

            var resultMock = serviceTest.RunSelectNotInSubquery(usersTableName, ordersTableName);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";
                    var ordersTableNameContainer = dialect.Provider == ProviderId.Oracle
                        ? orders.ToLowerInvariant()
                        : $"{orders}_{uId}";

                    var resultContainer = serviceTestContainer.RunSelectNotInSubquery(usersTableNameContainer, ordersTableNameContainer);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies an IN-list predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado IN com lista retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectInListPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunInListPredicateMatrix(tableName);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunInListPredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a BETWEEN predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado BETWEEN retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectBetweenPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunBetweenPredicateMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunBetweenPredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a LIKE predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado LIKE retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectLikePredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunLikePredicateMatrix(tableName);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunLikePredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a combined BETWEEN, LIKE, and ORDER BY query returns the expected row order for the current provider.
    /// PT: Verifica se uma consulta combinada com BETWEEN, LIKE e ORDER BY retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectBetweenLikeOrderByTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Aaron"), (2, "Alice"), (3, "Bob"), (4, "Charlie"), (5, "Delta")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunBetweenLikeOrderByMatrix(tableName);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Aaron"), (2, "Alice"), (3, "Bob"), (4, "Charlie"), (5, "Delta")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunBetweenLikeOrderByMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a NOT LIKE predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado NOT LIKE retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectNotLikePredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunNotLikePredicateMatrix(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunNotLikePredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a not-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado diferente de retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectNotEqualPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunNotEqualPredicateMatrix(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunNotEqualPredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies an equality predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado de igualdade retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectEqualPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunEqualPredicateMatrix(tableName);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunEqualPredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a parameterized name lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por nome retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectParameterByNameTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterSelectByNameMatrix(users, "Bob");
            resultMock.Should().Be("Bob");

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterSelectByNameMatrix(users, "Bob");
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

    /// <summary>
    /// EN: Verifies a parameterized id lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por id retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectParameterByIdTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterSelectByIdMatrix(users, 3, "Charlie");
            resultMock.Should().Be("Charlie");

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterSelectByIdMatrix(users, 3, "Charlie");
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

    /// <summary>
    /// EN: Verifies parameter roundtrips over typed user columns for string, numeric, boolean, date, and null values.
    /// PT: Verifica roundtrips de parametros sobre colunas tipadas de usuario para valores de texto, numericos, booleanos, data e nulos.
    /// </summary>
    [Fact]
    public void SelectParameterRoundTripMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();
        var createdAt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterRoundTripMatrix(
                users,
                uId,
                1,
                "Param Alice",
                DBNull.Value,
                true,
                (short)31,
                12.34m,
                createdAt,
                DBNull.Value,
                DBNull.Value);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterRoundTripMatrix(
                        users,
                        uId,
                        1,
                        "Param Alice",
                        DBNull.Value,
                        true,
                        (short)31,
                        12.34m,
                        createdAt,
                        DBNull.Value,
                        DBNull.Value);
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

    /// <summary>
    /// EN: Verifies typed provider parameters roundtrip correctly for ANSI text, fixed-length text, numeric, boolean, temporal, GUID, binary, and null values.
    /// PT: Verifica se parametros tipados do provedor fazem roundtrip corretamente para texto ANSI, texto de comprimento fixo, numericos, booleanos, temporais, GUID, binario e nulos.
    /// </summary>
    [Fact]
    public void SelectParameterTypeMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();
        var createdAt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        var ansiFixedText = "Fixed ANSI";
        var fixedText = "Fixed Text";

        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new InsertUsersScenario<T>(dialect), dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterTypeMatrix(
                "Typed param",
                "Ansi param",
                ansiFixedText,
                fixedText,
                (short)12,
                34,
                56L,
                true,
                78.90m,
                12.5d,
                TimeSpan.FromHours(1.5),
                new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                createdAt,
                Guid.Parse("11111111-2222-3333-4444-555555555555"),
                new byte[] { 1, 2, 3, 4 });
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new InsertUsersScenario<T2>(dialect), dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterTypeMatrix(
                        "Typed param",
                        "Ansi param",
                        ansiFixedText,
                        fixedText,
                        (short)12,
                        34,
                        56L,
                        true,
                        78.90m,
                        12.5d,
                        TimeSpan.FromHours(1.5),
                        new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                        createdAt,
                        Guid.Parse("11111111-2222-3333-4444-555555555555"),
                        new byte[] { 1, 2, 3, 4 });
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

    /// <summary>
    /// EN: Verifies typed provider parameters roundtrip correctly for date and currency values.
    /// PT: Verifica se parametros tipados do provedor fazem roundtrip corretamente para valores de data e moeda.
    /// </summary>
    [Fact]
    public void SelectParameterDateCurrencyMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();
        var dateValue = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var currencyValue = 123.45m;

        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new InsertUsersScenario<T>(dialect), dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterDateCurrencyMatrix(dateValue, currencyValue);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new InsertUsersScenario<T2>(dialect), dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterDateCurrencyMatrix(dateValue, currencyValue);
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

    /// <summary>
    /// EN: Verifies a greater-than predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado maior que retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectGreaterThanPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunGreaterThanPredicateMatrix(users);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunGreaterThanPredicateMatrix(users);
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

    /// <summary>
    /// EN: Verifies a less-than predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado menor que retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectLessThanPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunLessThanPredicateMatrix(users);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunLessThanPredicateMatrix(users);
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

    /// <summary>
    /// EN: Verifies a greater-than-or-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado maior ou igual retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectGreaterThanOrEqualPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunGreaterThanOrEqualPredicateMatrix(users);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunGreaterThanOrEqualPredicateMatrix(users);
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

    /// <summary>
    /// EN: Verifies a less-than-or-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado menor ou igual retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectLessThanOrEqualPredicateTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunLessThanOrEqualPredicateMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunLessThanOrEqualPredicateMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectOrderByNameTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunOrderByNameMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunOrderByNameMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies that a UNION projection returns the expected distinct row count for the current provider.
    /// PT: Verifica se uma projecao UNION retorna a contagem distinta esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectUnionDistinctProjectionTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Carla"));

            var resultMock = serviceTest.RunUnionDistinctProjection(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bob"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Carla"));

                    var resultContainer = serviceTestContainer.RunUnionDistinctProjection(containerTableName);
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

    /// <summary>
    /// EN: Verifies grouped names by initial with distinct counts and HAVING filtering for the current provider.
    /// PT: Verifica nomes agrupados por inicial com contagens distintas e filtro HAVING para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectGroupByNameInitialMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunGroupByNameInitialMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunGroupByNameInitialMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies GROUP BY Name with HAVING filtering over the configured users table for the current provider.
    /// PT: Verifica GROUP BY Name com filtro HAVING na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectGroupByNameHavingTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunGroupByNameHavingMatrix(users);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunGroupByNameHavingMatrix(users);
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

    /// <summary>
    /// EN: Verifies GROUP BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de GROUP BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectGroupByOrdinalTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunGroupByOrdinalMatrix(users);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunGroupByOrdinalMatrix(users);
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

    /// <summary>
    /// EN: Verifies ORDER BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de ORDER BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectOrderByOrdinalTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunOrderByOrdinalMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunOrderByOrdinalMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies DISTINCT ordering by ordinal over the configured users table for the current provider.
    /// PT: Verifica a ordenacao DISTINCT por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectDistinctOrderByOrdinalTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunDistinctOrderByOrdinalMatrix(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunDistinctOrderByOrdinalMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies DISTINCT with a text filter ordered by ordinal over the configured users table for the current provider.
    /// PT: Verifica DISTINCT com filtro de texto ordenado por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectDistinctLikeOrderByOrdinalTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunDistinctLikeOrderByOrdinalMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunDistinctLikeOrderByOrdinalMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies descending ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao descendente por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectOrderByNameDescendingTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunOrderByNameDescendingMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunOrderByNameDescendingMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a paged ordered query built with ROW_NUMBER for the current provider.
    /// PT: Verifica uma consulta ordenada e paginada construida com ROW_NUMBER para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectNamePaginationMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunNamePaginationMatrix(tableName);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunNamePaginationMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies native pagination syntax over an ordered users table for the current provider.
    /// PT: Verifica sintaxe nativa de paginação sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectPagedNameProjectionTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            var resultMock = serviceTest.RunPagedNameProjectionMatrix(tableName);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    var resultContainer = serviceTestContainer.RunPagedNameProjectionMatrix(containerTableName);
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

    /// <summary>
    /// EN: Verifies a relational query bundle across users and orders tables for the current provider.
    /// PT: Verifica um conjunto de consultas relacionais nas tabelas de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectRelationalCompositeTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultsMock = RunRelationalCompositeAssertions(serviceTest, users, orders);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultsContainer = RunRelationalCompositeAssertions(serviceTestContainer, users, orders);
                    resultsMock.Should().Be(resultsContainer);
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
    /// EN: Verifies that common window functions return the expected row counts for the current provider.
    /// PT: Verifica se funcoes de janela comuns retornam a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowFunctionsTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Charlie"));

            var rowNumberMock = serviceTest.RunWindowRowNumber(tableName);
            var lagMock = serviceTest.RunWindowLag(tableName);
            var leadMock = serviceTest.RunWindowLead(tableName);
            rowNumberMock.Should().Be(3);
            lagMock.Should().Be(3);
            leadMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bob"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Charlie"));

                    var rowNumberContainer = serviceTestContainer.RunWindowRowNumber(containerTableName);
                    var lagContainer = serviceTestContainer.RunWindowLag(containerTableName);
                    var leadContainer = serviceTestContainer.RunWindowLead(containerTableName);

                    rowNumberMock.Should().Be(rowNumberContainer);
                    lagMock.Should().Be(lagContainer);
                    leadMock.Should().Be(leadContainer);
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

    /// <summary>
    /// EN: Verifies ranking window functions with duplicate names for the current provider.
    /// PT: Verifica funcoes de janela de ranking com nomes duplicados para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowRankDenseRankTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 4, "Charlie"));

            var resultMock = serviceTest.RunWindowRankDenseRank(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 4, "Charlie"));

                    var resultContainer = serviceTestContainer.RunWindowRankDenseRank(containerTableName);
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

    /// <summary>
    /// EN: Verifies FIRST_VALUE and LAST_VALUE window projections for the current provider.
    /// PT: Verifica projeções FIRST_VALUE e LAST_VALUE de janela para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowFirstLastValueTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 4, "Charlie"));

            var resultMock = serviceTest.RunWindowFirstLastValue(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 4, "Charlie"));

                    var resultContainer = serviceTestContainer.RunWindowFirstLastValue(containerTableName);
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

    /// <summary>
    /// EN: Verifies NTILE distribution over an ordered users table for the current provider.
    /// PT: Verifica a distribuicao NTILE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowNtileTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 4, "Charlie"));

            var resultMock = serviceTest.RunWindowNtile(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 4, "Charlie"));

                    var resultContainer = serviceTestContainer.RunWindowNtile(containerTableName);
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

    /// <summary>
    /// EN: Verifies PERCENT_RANK and CUME_DIST window projections for the current provider.
    /// PT: Verifica projeções de janela PERCENT_RANK e CUME_DIST para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowPercentRankCumeDistTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 4, "Charlie"));

            var resultMock = serviceTest.RunWindowPercentRankCumeDist(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 4, "Charlie"));

                    var resultContainer = serviceTestContainer.RunWindowPercentRankCumeDist(containerTableName);
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

    /// <summary>
    /// EN: Verifies NTH_VALUE over an ordered users table for the current provider.
    /// PT: Verifica NTH_VALUE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectWindowNthValueTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 3, "Bravo"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 4, "Charlie"));

            var resultMock = serviceTest.RunWindowNthValue(tableName);
            resultMock.Should().Be(4);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 3, "Bravo"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 4, "Charlie"));

                    var resultContainer = serviceTestContainer.RunWindowNthValue(containerTableName);
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

    /// <summary>
    /// EN: Verifies range filtering and pivot-style aggregation for the current provider.
    /// PT: Verifica filtragem por faixa e agregacao no estilo pivot para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectRangeAndPivotTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new SelectTableScenario<T>(dialect);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = dialect.Provider == ProviderId.Oracle
                ? users.ToLowerInvariant()
                : $"{users}_{uId}";

            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));
            for (var id = 3; id <= 12; id++)
            {
                serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, id, $"User-{id}"));
            }

            var partitionCountMock = serviceTest.RunPartitionPruningSelect(tableName);
            var pivotCountMock = serviceTest.RunPivotCount(tableName);
            partitionCountMock.Should().Be(6);
            pivotCountMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new SelectTableScenario<T2>(dialect);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var containerTableName = dialect.Provider == ProviderId.Oracle
                        ? users.ToLowerInvariant()
                        : $"{users}_{uId}";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, 2, "Bob"));
                    for (var id = 3; id <= 12; id++)
                    {
                        serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(containerTableName, id, $"User-{id}"));
                    }

                    var partitionCountContainer = serviceTestContainer.RunPartitionPruningSelect(containerTableName);
                    var pivotCountContainer = serviceTestContainer.RunPivotCount(containerTableName);
                    partitionCountMock.Should().Be(partitionCountContainer);
                    pivotCountMock.Should().Be(pivotCountContainer);
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

    /// <summary>
    /// EN: Verifies a grouped join projection that combines typed columns, aggregates, and calculations for the current provider.
    /// PT: Verifica uma projecao com junção agrupada que combina colunas tipadas, agregacoes e calculos para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinTypedExpressionMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinTypedExpressionMatrix(users, orders);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinTypedExpressionMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a left join aggregate projection that preserves users without orders for the current provider.
    /// PT: Verifica uma projeção agregada com left join que preserva usuarios sem pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinNullAggregateMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinNullAggregateMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinNullAggregateMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a grouped left join projection that blends casts, null handling, and aggregate formatting for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que mistura casts, tratamento de null e formatacao de agregados para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinCastNullMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinCastNullMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinCastNullMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a grouped left join projection that casts aggregate values to text and compares them for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que converte agregados para texto e os compara para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinCastTextComparisonMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinCastTextComparisonMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinCastTextComparisonMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a grouped join query with HAVING filters and casted aggregate outputs for the current provider.
    /// PT: Verifica uma consulta agrupada com filtros HAVING e saidas agregadas convertidas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinHavingCastMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinHavingCastMatrix(users, orders);
            resultMock.Should().Be(1);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinHavingCastMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a grouped join projection that mixes string-length expressions with numeric conversions and aggregates for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura expressoes de comprimento de texto com conversoes numericas e agregados para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinLengthNumericMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinLengthNumericMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinLengthNumericMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a grouped join projection that blends string case, string length, and aggregate comparisons for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura caixa de texto, comprimento de texto e comparacoes agregadas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinTextCaseLengthMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var usersTableName = $"{users}_{uId}";
            var ordersTableName = $"{orders}_{uId}";
            var orderedAt = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAt));
            serviceTest.ExecuteNonQuery(dialect.InsertOrder(ordersTableName, usersTableName, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAt));

            var resultMock = serviceTest.RunJoinTextCaseLengthMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, Array.Empty<(int id, int userId, string note)>());
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var usersTableNameContainer = $"{users}_{uId}";
                    var ordersTableNameContainer = $"{orders}_{uId}";
                    var orderedAtContainer = dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP";

                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 10, 1, "A", "o-10", 1.25m, 2, false, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 11, 1, "B", "o-11", 2.75m, 1, true, orderedAtContainer));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertOrder(ordersTableNameContainer, usersTableNameContainer, 12, 2, "C", "o-12", 5.50m, 4, false, orderedAtContainer));

                    var resultContainer = serviceTestContainer.RunJoinTextCaseLengthMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies grouped left-join reporting with distinct counts, repeated values, and CASE expressions for the current provider.
    /// PT: Verifica um relatorio agrupado com left join, contagens distintas, valores repetidos e expressoes CASE para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinDistinctCaseMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "A"),
            (12, 1, "B"),
            (13, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinDistinctCaseMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinDistinctCaseMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies grouped left-join filtering with HAVING and distinct note counts for the current provider.
    /// PT: Verifica filtragem agrupada com left join, HAVING e contagens distintas de notas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinDistinctHavingMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "A"),
            (12, 1, "B"),
            (13, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinDistinctHavingMatrix(users, orders);
            resultMock.Should().Be(2);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinDistinctHavingMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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

    private static (int Cte, int Exists, int Correlated, int GroupBy, int UnionAll, int Distinct, int MultiJoin, object? ScalarSubquery, int InSubquery, int Pivot) RunRelationalCompositeAssertions<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users,
        string orders)
        where TConnection : DbConnection
    {
        var cte = serviceTest.RunCteSimple(users, orders);
        var existsPredicate = serviceTest.RunSelectExistsPredicate(users, orders);
        var correlatedCount = serviceTest.RunSelectCorrelatedCount(users, orders);
        var groupByHaving = serviceTest.RunGroupByHaving(users, orders);
        var unionAll = serviceTest.RunUnionAllProjection(users);
        var distinct = serviceTest.RunDistinctProjection(users);
        var multiJoin = serviceTest.RunMultiJoinAggregate(users, orders);
        var scalarSubquery = serviceTest.RunSelectScalarSubquery(users, orders);
        var inSubquery = serviceTest.RunSelectInSubquery(users, orders);
        var pivot = serviceTest.RunPivotCount(users);

        cte.Should().Be(1);
        existsPredicate.Should().Be(2);
        correlatedCount.Should().Be(2);
        groupByHaving.Should().Be(1);
        unionAll.Should().Be(2);
        distinct.Should().Be(2);
        multiJoin.Should().Be(4);
        Convert.ToString(scalarSubquery, CultureInfo.InvariantCulture).Should().Be("2");
        inSubquery.Should().Be(2);
        pivot.Should().Be(2);

        return (cte, existsPredicate, correlatedCount, groupByHaving, unionAll, distinct, multiJoin, scalarSubquery, inSubquery, pivot);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that blends temporal comparisons with aggregate counts for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura comparacoes temporais com contagens agregadas para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinTemporalMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinTemporalMatrix(users, orders);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinTemporalMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies a joined window-function projection over the shared users and orders scenario for the current provider.
    /// PT: Verifica uma projeção com funcoes de janela em join no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectJoinWindowMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinWindowMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinWindowMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies joined window projections together with temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public void SelectJoinWindowTemporalMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinWindowTemporalMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinWindowTemporalMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies joined window projections together with aggregate and temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes agregadas e temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public void SelectJoinWindowAggregateTemporalMatrixTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultMock = serviceTest.RunJoinWindowAggregateTemporalMatrix(users, orders);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunJoinWindowAggregateTemporalMatrix(users, orders);
                    resultMock.Should().Be(resultContainer);
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
    /// EN: Verifies CROSS APPLY and OUTER APPLY projections against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeções CROSS APPLY e OUTER APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public void SelectApplyProjectionTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var resultCrossApplyMock = serviceTest.RunCrossApplyProjection(users, orders);
            var resultOuterApplyMock = serviceTest.RunOuterApplyProjection(users, orders);

            resultCrossApplyMock.Should().Be(2);
            resultOuterApplyMock.Should().Be(3);
            (resultOuterApplyMock >= resultCrossApplyMock).Should().BeTrue();

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var resultCrossApplyContainer = serviceTestContainer.RunCrossApplyProjection(users, orders);
                    var resultOuterApplyContainer = serviceTestContainer.RunOuterApplyProjection(users, orders);

                    resultCrossApplyMock.Should().Be(resultCrossApplyContainer);
                    resultOuterApplyMock.Should().Be(resultOuterApplyContainer);
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
    /// EN: Verifies APPLY projections together with temporal join comparisons for the shared users and orders scenario.
    /// PT: Verifica projeções APPLY junto com comparacoes temporais em join no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public void SelectApplyTemporalCompositeTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var crossApplyMock = serviceTest.RunCrossApplyProjection(users, orders);
            var outerApplyMock = serviceTest.RunOuterApplyProjection(users, orders);
            var temporalMock = serviceTest.RunJoinTemporalMatrix(users, orders);

            crossApplyMock.Should().Be(2);
            outerApplyMock.Should().Be(3);
            temporalMock.Should().Be(3);
            (outerApplyMock >= crossApplyMock).Should().BeTrue();

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var crossApplyContainer = serviceTestContainer.RunCrossApplyProjection(users, orders);
                    var outerApplyContainer = serviceTestContainer.RunOuterApplyProjection(users, orders);
                    var temporalContainer = serviceTestContainer.RunJoinTemporalMatrix(users, orders);

                    crossApplyMock.Should().Be(crossApplyContainer);
                    outerApplyMock.Should().Be(outerApplyContainer);
                    temporalMock.Should().Be(temporalContainer);
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
    /// EN: Verifies APPLY, window functions, and temporal join comparisons together for the shared users and orders scenario.
    /// PT: Verifica APPLY, funcoes de janela e comparacoes temporais em join juntas no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public void SelectApplyWindowTemporalCompositeTest()
    {
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var seedUsers = new (int id, string name)[]
        {
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carla")
        };

        var seedOrders = new (int id, int userId, string note)[]
        {
            (10, 1, "A"),
            (11, 1, "B"),
            (12, 2, "C")
        };

        var testScenario = new UsersOrdersScenario<T>(dialect, seedUsers, seedOrders);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, orders, uId);

        try
        {
            var crossApplyMock = serviceTest.RunCrossApplyProjection(users, orders);
            var outerApplyMock = serviceTest.RunOuterApplyProjection(users, orders);
            var windowMock = serviceTest.RunJoinWindowMatrix(users, orders);
            var temporalMock = serviceTest.RunJoinWindowTemporalMatrix(users, orders);

            crossApplyMock.Should().Be(2);
            outerApplyMock.Should().Be(3);
            windowMock.Should().Be(3);
            temporalMock.Should().Be(3);
            (outerApplyMock >= crossApplyMock).Should().BeTrue();

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersOrdersScenario<T2>(dialect, seedUsers, seedOrders);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, orders, uId);
                try
                {
                    var crossApplyContainer = serviceTestContainer.RunCrossApplyProjection(users, orders);
                    var outerApplyContainer = serviceTestContainer.RunOuterApplyProjection(users, orders);
                    var windowContainer = serviceTestContainer.RunJoinWindowMatrix(users, orders);
                    var temporalContainer = serviceTestContainer.RunJoinWindowTemporalMatrix(users, orders);

                    crossApplyMock.Should().Be(crossApplyContainer);
                    outerApplyMock.Should().Be(outerApplyContainer);
                    windowMock.Should().Be(windowContainer);
                    temporalMock.Should().Be(temporalContainer);
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

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}



