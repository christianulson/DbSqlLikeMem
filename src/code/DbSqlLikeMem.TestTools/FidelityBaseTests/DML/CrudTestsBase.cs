using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared CRUD fidelity tests for update and delete workflows across mock and container runs.
/// PT: Fornece testes de fidelidade CRUD compartilhados para fluxos de update e delete entre mock e container.
/// </summary>
public abstract class CrudTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an update followed by a delete keeps the expected row count and remaining value for the current provider.
    /// PT: Verifica se um update seguido de delete mantem a contagem esperada de linhas e o valor restante para o provedor atual.
    /// </summary>
    [Fact]
    public void UpdateDeleteRoundTripTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob")]);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = $"{users}_{uId}";
            var resultMock = serviceTest.RunUpdateDeleteRoundTrip(tableName);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob")]);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunUpdateDeleteRoundTrip(tableName);
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
    /// EN: Verifies that update and delete actions committed inside a transaction keep the expected final state.
    /// PT: Verifica se acoes de update e delete confirmadas dentro de uma transacao mantem o estado final esperado.
    /// </summary>
    [Fact]
    public void TransactionalUpdateDeleteCommitTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob")]);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = $"{users}_{uId}";
            var resultMock = serviceTest.RunTransactionalUpdateDeleteCommit(tableName);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob")]);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunTransactionalUpdateDeleteCommit(tableName);
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
    /// EN: Verifies that updating a single user row persists the expected value for the current provider.
    /// PT: Verifica se a atualizacao de uma unica linha de usuario persiste o valor esperado para o provedor atual.
    /// </summary>
    [Fact]
    public void UpdateByPkTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob")]);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunUpdateByPk(tableName);
            resultMock.Should().Be("Alice-v2");

            var persistedNameMock = Convert.ToString(serviceTest.ExecuteScalar(dialect.SelectUserNameById(tableName, 1)), CultureInfo.InvariantCulture);
            persistedNameMock.Should().Be("Alice-v2");

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob")]);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunUpdateByPk(tableName);
                    resultMock.Should().Be(resultContainer);

                    var persistedNameContainer = Convert.ToString(serviceTestContainer.ExecuteScalar(dialect.SelectUserNameById(tableName, 1)), CultureInfo.InvariantCulture);
                    persistedNameMock.Should().Be(persistedNameContainer);
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
    /// EN: Verifies that deleting a single user row keeps the expected remaining row for the current provider.
    /// PT: Verifica se a exclusao de uma unica linha de usuario mantem a linha restante esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void DeleteByPkTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob")]);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunDeleteByPk(tableName);
            resultMock.Should().Be(1);

            var remainingNameMock = Convert.ToString(serviceTest.ExecuteScalar(dialect.SelectUserNameById(tableName, 2)), CultureInfo.InvariantCulture);
            remainingNameMock.Should().Be("Bob");

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob")]);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunDeleteByPk(tableName);
                    resultMock.Should().Be(resultContainer);

                    var remainingNameContainer = Convert.ToString(serviceTestContainer.ExecuteScalar(dialect.SelectUserNameById(tableName, 2)), CultureInfo.InvariantCulture);
                    remainingNameMock.Should().Be(remainingNameContainer);
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
    /// EN: Verifies that an update reports a valid affected-row count and persists the new value for the current provider.
    /// PT: Verifica se uma atualizacao retorna uma contagem valida de linhas afetadas e persiste o novo valor para o provedor atual.
    /// </summary>
    [Fact]
    public void RowCountAfterUpdateTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Alice"), (2, "Bob")]);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunRowCountAfterUpdate(tableName);
            resultMock.Should().BeGreaterThan(0);

            var updatedNameMock = Convert.ToString(serviceTest.ExecuteScalar(dialect.SelectUserNameById(tableName, 1)), CultureInfo.InvariantCulture);
            updatedNameMock.Should().Be("Alice-v2");

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Alice"), (2, "Bob")]);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunRowCountAfterUpdate(tableName);
                    resultMock.Should().Be(resultContainer);

                    var updatedNameContainer = Convert.ToString(serviceTestContainer.ExecuteScalar(dialect.SelectUserNameById(tableName, 1)), CultureInfo.InvariantCulture);
                    updatedNameMock.Should().Be(updatedNameContainer);
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
    /// EN: Verifies that the returning-update benchmark updates the same row as the primary-key update path for the current provider.
    /// PT: Verifica se o benchmark de returning update atualiza a mesma linha do caminho de update por chave primaria para o provedor atual.
    /// </summary>
    [Fact]
    public void ReturningUpdateTest()
        => UpdateByPkTest();

    /// <summary>
    /// EN: Verifies typed provider parameters update and delete rows correctly in the users table for the current provider.
    /// PT: Verifica se parametros tipados do provedor atualizam e excluem linhas corretamente na tabela de usuarios do provedor atual.
    /// </summary>
    [Fact]
    public void ParameterUpdateDeleteRoundTripTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";
        var updatedAt = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 1, "Alice"));
            serviceTest.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));

            var resultMock = serviceTest.RunParameterUpdateDeleteRoundTrip(
                tableName,
                "Alice-v2",
                "alice@example.com",
                true,
                (short)31,
                123.45m,
                updatedAt,
                "{\"theme\":\"dark\"}",
                2);
            resultMock.Should().Be(2);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(tableName, 1, "Alice"));
                    serviceTestContainer.ExecuteNonQuery(dialect.InsertUser(tableName, 2, "Bob"));

                    var resultContainer = serviceTestContainer.RunParameterUpdateDeleteRoundTrip(
                        tableName,
                        "Alice-v2",
                        "alice@example.com",
                        true,
                        (short)31,
                        123.45m,
                        updatedAt,
                        "{\"theme\":\"dark\"}",
                        2);
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
    /// EN: Verifies typed provider parameters insert rows correctly in the users table for the current provider.
    /// PT: Verifica se parametros tipados do provedor inserem linhas corretamente na tabela de usuarios do provedor atual.
    /// </summary>
    [Fact]
    public void ParameterInsertRoundTripTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";
        var createdAt1 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));
        var updatedAt1 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Unspecified));
        var updatedAt2 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 4, 5, 6, 7, 8, DateTimeKind.Unspecified));

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterInsertRoundTrip(
                tableName,
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                true,
                false,
                (short)31,
                (short)22,
                123.45m,
                67.89m,
                createdAt1,
                createdAt2,
                updatedAt1,
                updatedAt2,
                "{\"theme\":\"dark\"}",
                "{\"theme\":\"light\"}");
            resultMock.Should().Be(2);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterInsertRoundTrip(
                        tableName,
                        "Alice-v2",
                        "Bob-v2",
                        "alice@example.com",
                        "bob@example.com",
                        true,
                        false,
                        (short)31,
                        (short)22,
                        123.45m,
                        67.89m,
                        createdAt1,
                        createdAt2,
                        updatedAt1,
                        updatedAt2,
                        "{\"theme\":\"dark\"}",
                        "{\"theme\":\"light\"}");
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
    /// EN: Verifies typed provider parameters insert nullable values correctly in the users table for the current provider.
    /// PT: Verifica se parametros tipados do provedor inserem valores anulaveis corretamente na tabela de usuarios do provedor atual.
    /// </summary>
    [Fact]
    public void ParameterInsertNullRoundTripTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";
        var createdAt = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterInsertNullRoundTrip(
                tableName,
                "Alice-v2",
                (string?)null,
                true,
                (short)31,
                123.45m,
                createdAt,
                (DateTime?)null,
                (string?)null);
            resultMock.Should().Be(1);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterInsertNullRoundTrip(
                        tableName,
                        "Alice-v2",
                        (string?)null,
                        true,
                        (short)31,
                        123.45m,
                        createdAt,
                        (DateTime?)null,
                        (string?)null);
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
    /// EN: Verifies typed provider parameters insert rows correctly inside a committed transaction for the current provider.
    /// PT: Verifica se parametros tipados do provedor inserem linhas corretamente dentro de uma transacao confirmada para o provedor atual.
    /// </summary>
    [Fact]
    public void ParameterTransactionCommitTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";
        var createdAt1 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterTransactionCommit(
                tableName,
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                createdAt1,
                createdAt2);
            resultMock.Should().Be(2);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterTransactionCommit(
                        tableName,
                        "Alice-v2",
                        "Bob-v2",
                        "alice@example.com",
                        "bob@example.com",
                        createdAt1,
                        createdAt2);
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
    /// EN: Verifies typed provider parameters roll back correctly inside a transaction for the current provider.
    /// PT: Verifica se parametros tipados do provedor fazem rollback corretamente dentro de uma transacao para o provedor atual.
    /// </summary>
    [Fact]
    public void ParameterTransactionRollbackTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = $"{users}_{uId}";
        var createdAt1 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeNpgsqlDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new InsertUsersScenario<T>(dialect);
        var serviceTest = new DmlMutationServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = serviceTest.RunParameterTransactionRollback(
                tableName,
                "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                createdAt1,
                createdAt2);
            resultMock.Should().Be(0);

            if (IsInsertContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new InsertUsersScenario<T2>(dialect);
                var serviceTestContainer = new DmlMutationServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = serviceTestContainer.RunParameterTransactionRollback(
                        tableName,
                        "Alice-v2",
                        "Bob-v2",
                        "alice@example.com",
                        "bob@example.com",
                        createdAt1,
                        createdAt2);
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

    private DateTime NormalizeNpgsqlDateTimeInput(DateTime value)
    {
        if (dialect.Provider == ProviderId.Npgsql && value.Kind == DateTimeKind.Unspecified)
            return DateTime.SpecifyKind(value, DateTimeKind.Utc);

        return value;
    }
}

