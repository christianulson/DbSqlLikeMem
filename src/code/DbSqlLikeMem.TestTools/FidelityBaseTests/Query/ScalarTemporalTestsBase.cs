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
    public async Task ScalarTemporalMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => RunScalarTemporalMatrixAsync(s));

        var (dateScalar, currentTimestamp, dateAdd, whereCount, orderedName) =
            ((DateTime dateScalar, DateTime currentTimestamp, DateTime dateAdd, int whereCount, string orderedName))result!;

        dateAdd.Should().BeCloseTo(dateScalar.AddDays(1), TemporalComparisonTolerance);
        dateAdd.Should().BeCloseTo(currentTimestamp.AddDays(1), TemporalComparisonTolerance);
        whereCount.Should().Be(3);
        orderedName.Should().Be("Aaron");
    }

    /// <summary>
    /// EN: Verifies the scalar date benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark escalar de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public async Task DateScalarTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunDateScalarAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies the current timestamp benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark de timestamp atual retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TemporalCurrentTimestampTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunTemporalCurrentTimestampAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies the temporal date-add benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark temporal de soma de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TemporalDateAddTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunTemporalDateAddAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies the current-time predicate benchmark counts the configured rows for the current provider.
    /// PT: Verifica se o benchmark de predicado de tempo atual conta as linhas configuradas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TemporalNowWhereTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunTemporalNowWhereAsync(a));
        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies the current-time ordering benchmark returns the expected first row for the current provider.
    /// PT: Verifica se o benchmark de ordenacao por tempo atual retorna a primeira linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TemporalNowOrderByTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunTemporalNowOrderByAsync(a));
        result.Should().Be("Aaron");
    }

    private static async Task<object?> RunScalarTemporalMatrixAsync(
        QueryServiceTest serviceTest)
    {
        var dateScalar = NormalizeDateTimeValue(await serviceTest.RunDateScalarAsync());
        var currentTimestamp = NormalizeDateTimeValue(await serviceTest.RunTemporalCurrentTimestampAsync());
        var dateAdd = NormalizeDateTimeValue(await serviceTest.RunTemporalDateAddAsync());
        var whereCount = Convert.ToInt32(await serviceTest.RunTemporalNowWhereAsync(), CultureInfo.InvariantCulture);
        var orderedName = Convert.ToString(await serviceTest.RunTemporalNowOrderByAsync(), CultureInfo.InvariantCulture) ?? string.Empty;
        dateScalar.Should().NotBe(default);
        currentTimestamp.Should().NotBe(default);
        dateAdd.Should().NotBe(default);

        return (dateScalar, currentTimestamp, dateAdd, whereCount, orderedName);
    }

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
