using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared scalar temporal fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade temporal escalar compartilhados entre execucoes mock e container.
/// </summary>
public abstract class ScalarTemporalTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Returns the allowed drift between mock and container temporal results.
    /// PT: Retorna a variacao permitida entre resultados temporais do mock e do container.
    /// </summary>
    protected virtual TimeSpan TemporalComparisonTolerance => TimeSpan.FromSeconds(10);

    /// <summary>
    /// EN: Verifies scalar temporal functions, current-time predicates, and ordered reads for the current provider.
    /// PT: Verifica funcoes temporais escalares, predicados de tempo atual e leituras ordenadas para o provedor atual.
    /// </summary>
    [Fact]
    public void ScalarTemporalMatrixTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = RunScalarTemporalMatrix(serviceTest, users);
            (resultMock.DateAdd > resultMock.DateScalar).Should().BeTrue();
            (resultMock.DateAdd > resultMock.CurrentTimestamp).Should().BeTrue();
            resultMock.WhereCount.Should().Be(3);
            resultMock.OrderedName.Should().Be("Aaron");

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = RunScalarTemporalMatrix(serviceTestContainer, users);
                    resultMock.DateScalar.Should().BeCloseTo(resultContainer.DateScalar, TemporalComparisonTolerance);
                    resultMock.CurrentTimestamp.Should().BeCloseTo(resultContainer.CurrentTimestamp, TemporalComparisonTolerance);
                    resultMock.DateAdd.Should().BeCloseTo(resultContainer.DateAdd, TemporalComparisonTolerance);
                    resultMock.WhereCount.Should().Be(resultContainer.WhereCount);
                    resultMock.OrderedName.Should().Be(resultContainer.OrderedName);
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
    /// EN: Verifies the scalar date benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark escalar de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public void DateScalarTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new UsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeDateTimeValue(serviceTest.RunDateScalar());
        resultMock.Should().NotBe(default);

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new UsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeDateTimeValue(serviceTestContainer.RunDateScalar());
            resultMock.Should().BeCloseTo(resultContainer, TemporalComparisonTolerance);
        }
    }

    /// <summary>
    /// EN: Verifies the current timestamp benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark de timestamp atual retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalCurrentTimestampTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new UsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeDateTimeValue(serviceTest.RunTemporalCurrentTimestamp());
        resultMock.Should().NotBe(default);

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new UsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeDateTimeValue(serviceTestContainer.RunTemporalCurrentTimestamp());
            resultMock.Should().BeCloseTo(resultContainer, TemporalComparisonTolerance);
        }
    }

    /// <summary>
    /// EN: Verifies the temporal date-add benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark temporal de soma de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalDateAddTest()
    {
        using var connMock = connectionMock();
        connMock.Open();

        var serviceTest = new QueryServiceTest<T>(connMock, new UsersScenario<T>(dialect), dialect);
        var resultMock = NormalizeDateTimeValue(serviceTest.RunTemporalDateAdd());
        resultMock.Should().NotBe(default);

        if (IsSelectContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var serviceTestContainer = new QueryServiceTest<T2>(connContainer, new UsersScenario<T2>(dialect), dialect);
            var resultContainer = NormalizeDateTimeValue(serviceTestContainer.RunTemporalDateAdd());
            resultMock.Should().BeCloseTo(resultContainer, TemporalComparisonTolerance);
        }
    }

    /// <summary>
    /// EN: Verifies the current-time predicate benchmark counts the configured rows for the current provider.
    /// PT: Verifica se o benchmark de predicado de tempo atual conta as linhas configuradas para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalNowWhereTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = Convert.ToInt32(serviceTest.RunTemporalNowWhere(users), CultureInfo.InvariantCulture);
            resultMock.Should().Be(3);

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = Convert.ToInt32(serviceTestContainer.RunTemporalNowWhere(users), CultureInfo.InvariantCulture);
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
    /// EN: Verifies the current-time ordering benchmark returns the expected first row for the current provider.
    /// PT: Verifica se o benchmark de ordenacao por tempo atual retorna a primeira linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void TemporalNowOrderByTest()
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();

        var testScenario = new UsersScenario<T>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
        var serviceTest = new QueryServiceTest<T>(connMock, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var resultMock = Convert.ToString(serviceTest.RunTemporalNowOrderBy(users), CultureInfo.InvariantCulture);
            resultMock.Should().Be("Aaron");

            if (IsSelectContainerComparisonEnabled(dialect.Provider)
                && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
            {
                using var connContainer = connectionContainer(connectionString);
                connContainer.Open();
                var testScenarioContainer = new UsersScenario<T2>(dialect, [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);
                var serviceTestContainer = new QueryServiceTest<T2>(connContainer, testScenarioContainer, dialect);
                serviceTestContainer.CreateScenario(users, uId);
                try
                {
                    var resultContainer = Convert.ToString(serviceTestContainer.RunTemporalNowOrderBy(users), CultureInfo.InvariantCulture);
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

    private static (DateTime DateScalar, DateTime CurrentTimestamp, DateTime DateAdd, int WhereCount, string OrderedName) RunScalarTemporalMatrix<TConnection>(
        QueryServiceTest<TConnection> serviceTest,
        string users)
        where TConnection : DbConnection
    {
        var dateScalar = NormalizeDateTimeValue(serviceTest.RunDateScalar());
        var currentTimestamp = NormalizeDateTimeValue(serviceTest.RunTemporalCurrentTimestamp());
        var dateAdd = NormalizeDateTimeValue(serviceTest.RunTemporalDateAdd());
        var whereCount = Convert.ToInt32(serviceTest.RunTemporalNowWhere(users), CultureInfo.InvariantCulture);
        var orderedName = Convert.ToString(serviceTest.RunTemporalNowOrderBy(users), CultureInfo.InvariantCulture) ?? string.Empty;

        dateScalar.Should().NotBe(default);
        currentTimestamp.Should().NotBe(default);
        dateAdd.Should().NotBe(default);

        return (dateScalar, currentTimestamp, dateAdd, whereCount, orderedName);
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();

    private static DateTime NormalizeDateTimeValue(object? value)
    {
        var normalized = value switch
        {
            DateTime dt => dt,
            DateTimeOffset dto => dto.DateTime,
            string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTime result returned a null value."),
            _ when TryNormalizeDateOnlyValue(value, out var dateOnly) => dateOnly,
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
        };

        return new DateTime(
            normalized.Year,
            normalized.Month,
            normalized.Day,
            normalized.Hour,
            normalized.Minute,
            normalized.Second,
            normalized.Kind);
    }

    private static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }
}
