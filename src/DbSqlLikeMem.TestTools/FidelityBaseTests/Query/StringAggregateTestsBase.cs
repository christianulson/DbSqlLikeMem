using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared string-aggregation fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de agregacao de strings compartilhados entre execucoes mock e container.
/// </summary>
public abstract class StringAggregateTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies ordered, distinct, custom-separator, and large-group string aggregation for the current provider.
    /// PT: Verifica agregacao de strings ordenada, distinta, com separador customizado e em grupo grande para o provedor atual.
    /// </summary>
    [Fact]
    public void StringAggregationVariantsTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = ResolveUsersTableName(users, uId);

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = RunStringAggregationVariants(serviceTest, tableName);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = RunStringAggregationVariants(serviceTestContainer, tableName);
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
    /// EN: Verifies string aggregation together with total, distinct, and repeated-name counts for the current provider.
    /// PT: Verifica agregacao de strings junto com contagens total, distinta e de nomes repetidos para o provedor atual.
    /// </summary>
    [Fact]
    public void StringAggregationSummaryMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = ResolveUsersTableName(users, uId);

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = RunStringAggregationSummary(serviceTest, tableName);

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
                    var resultContainer = RunStringAggregationSummary(serviceTestContainer, tableName);
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
    /// EN: Verifies grouped string aggregation with CASE, COALESCE, and distinct counts for the current provider.
    /// PT: Verifica agregacao agrupada de strings com CASE, COALESCE e contagens distintas para o provedor atual.
    /// </summary>
    [Fact]
    public void StringAggregationGroupCaseMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();
        var tableName = ResolveUsersTableName(users, uId);

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = RunStringAggregationGroupCase(serviceTest, tableName);

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
                    var resultContainer = RunStringAggregationGroupCase(serviceTestContainer, tableName);
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

    private (string? Plain, string? Ordered, string? Distinct, string? Separator, string? LargeGroup) RunStringAggregationVariants<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users)
        where TConnection : DbConnection
    {
        var tableName = ResolveUsersTableName(users);
        var plain = serviceTest.RunStringAggregate(tableName);
        var ordered = serviceTest.RunStringAggregateOrdered(tableName);
        var distinct = serviceTest.RunStringAggregateDistinct(tableName);
        var separator = serviceTest.RunStringAggregateCustomSeparator(tableName);
        var largeGroup = serviceTest.RunStringAggregateLargeGroup(tableName);

        plain.Should().NotBeNull();
        ordered.Should().NotBeNull();
        distinct.Should().NotBeNull();
        separator.Should().NotBeNull();
        largeGroup.Should().NotBeNull();

        return (plain, ordered, distinct, separator, largeGroup);
    }

    private (string? Ordered, int TotalCount, int DistinctCount, int BobCount) RunStringAggregationSummary<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users)
        where TConnection : DbConnection
    {
        var tableName = ResolveUsersTableName(users);
        var result = serviceTest.RunStringAggregateSummaryMatrix(tableName);

        result.Ordered.Should().NotBeNull();
        result.TotalCount.Should().Be(5);
        result.DistinctCount.Should().Be(4);
        result.BobCount.Should().Be(2);

        return result;
    }

    private int RunStringAggregationGroupCase<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users)
        where TConnection : DbConnection
    {
        var tableName = ResolveUsersTableName(users);
        var result = serviceTest.RunStringAggregateGroupCaseMatrix(tableName);
        result.Should().Be(2);
        return result;
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    private string ResolveUsersTableName(string users, string uId) =>
        dialect.Provider == ProviderId.Oracle
            ? users.ToLowerInvariant()
            : $"{users}_{uId}";

    private string ResolveUsersTableName(string tableName)
    {
        if (dialect.Provider != ProviderId.Oracle)
            return tableName;

        if (TryStripScenarioTokenSuffix(tableName, out var stripped))
            return stripped.ToLowerInvariant();

        return tableName.ToLowerInvariant();
    }

    private static bool TryStripScenarioTokenSuffix(string tableName, out string stripped)
    {
        stripped = tableName;

        var underscoreIndex = tableName.LastIndexOf('_');
        if (underscoreIndex < 0)
            return false;

        var suffixLength = tableName.Length - underscoreIndex - 1;
        if (suffixLength != 8)
            return false;

        for (var i = underscoreIndex + 1; i < tableName.Length; i++)
        {
            var ch = tableName[i];
            var isHexUpper = ch is >= 'A' and <= 'F';
            var isHexDigit = ch is >= '0' and <= '9';
            if (!isHexUpper && !isHexDigit)
                return false;
        }

        stripped = tableName[..underscoreIndex];
        return true;
    }
}
