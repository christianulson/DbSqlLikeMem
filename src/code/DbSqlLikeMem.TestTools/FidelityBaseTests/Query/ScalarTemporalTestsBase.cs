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
    [FidelityFact]
    public async Task ScalarTemporalMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (DateTime dateScalar, DateTime currentTimestamp, DateTime dateAdd, int whereCount, string orderedName)>(
            (s, a) => RunScalarTemporalMatrixAsync(s));

        var (dateScalar, currentTimestamp, dateAdd, whereCount, orderedName) = result;

        dateAdd.Should().BeCloseTo(dateScalar.AddDays(1), TemporalComparisonTolerance);
        dateAdd.Should().BeCloseTo(currentTimestamp.AddDays(1), TemporalComparisonTolerance);
        whereCount.Should().Be(3);
        orderedName.Should().Be("Aaron");
    }

    /// <summary>
    /// EN: Verifies the scalar date benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark escalar de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task DateScalarTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, DateTime>(
            (s, a) => s.RunDateScalarAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies the current timestamp benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark de timestamp atual retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalCurrentTimestampTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, DateTime>(
            (s, a) => s.RunTemporalCurrentTimestampAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies the temporal date-add benchmark returns a non-default temporal value for the current provider.
    /// PT: Verifica se o benchmark temporal de soma de data retorna um valor temporal nao padrao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalDateAddTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, DateTime>(
            (s, a) => s.RunTemporalDateAddAsync());
        result.Should().NotBe(default);
    }

    /// <summary>
    /// EN: Verifies SQL Server DATETRUNC keeps the expected truncation values for supported providers.
    /// PT: Verifica se o SQL Server DATETRUNC mantem os valores de truncamento esperados para os provedores suportados.
    /// </summary>
    [FidelityFact]
    public async Task TemporalDateTruncTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        if (!dialect.SupportsSqlServerDateFunction("DATETRUNC"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (DateTime monthValue, DateTime weekValue, DateTime dayOfYearValue, DateTime isoWeekValue, DateTime millisecondValue, DateTime microsecondValue)>(
                (s, a) => s.RunTemporalDateTruncAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (DateTime monthValue, DateTime weekValue, DateTime dayOfYearValue, DateTime isoWeekValue, DateTime millisecondValue, DateTime microsecondValue)>(
            (s, a) => s.RunTemporalDateTruncAsync());

        var (
            monthValue,
            weekValue,
            dayOfYearValue,
            isoWeekValue,
            millisecondValue,
            microsecondValue) = result;

        monthValue.Should().Be(new DateTime(2020, 2, 1));
        weekValue.Should().Be(new DateTime(2020, 2, 16));
        dayOfYearValue.Should().Be(new DateTime(2020, 2, 14));
        isoWeekValue.Should().Be(new DateTime(2020, 12, 28));
        millisecondValue.Should().Be(new DateTime(2020, 2, 10, 10, 11, 12, 124));
        microsecondValue.Should().Be(new DateTime(2020, 2, 10, 10, 11, 12).AddTicks(1245670));
    }

    /// <summary>
    /// EN: Verifies SQL Server time zone offset functions and TODATETIMEOFFSET keep the expected offset values for supported providers.
    /// PT: Verifica se as funcoes de offset de fuso horario do SQL Server e TODATETIMEOFFSET mantem os valores de offset esperados para os provedores suportados.
    /// </summary>
    [FidelityFact]
    public async Task TemporalTimeZoneOffsetTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        if (!dialect.SupportsSqlServerScalarFunction("TODATETIMEOFFSET"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (int literalOffsetMinutes, string literalOffsetText, int utcOffsetMinutes, string utcOffsetText, DateTimeOffset offsetValue, DateTimeOffset switchedValue, int offsetMinutes, string offsetText, int negativeOffsetMinutes, string negativeOffsetText)>(
                (s, a) => s.RunTemporalTimeZoneOffsetAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (int literalOffsetMinutes, string literalOffsetText, int utcOffsetMinutes, string utcOffsetText, DateTimeOffset offsetValue, DateTimeOffset switchedValue, int offsetMinutes, string offsetText, int negativeOffsetMinutes, string negativeOffsetText)>(
            (s, a) => s.RunTemporalTimeZoneOffsetAsync());

        var (
            literalOffsetMinutes,
            literalOffsetText,
            utcOffsetMinutes,
            utcOffsetText,
            offsetValue,
            switchedValue,
            offsetMinutes,
            offsetText,
            negativeOffsetMinutes,
            negativeOffsetText) = result;

        literalOffsetMinutes.Should().Be(310);
        literalOffsetText.Should().Be("310");
        utcOffsetMinutes.Should().Be(0);
        utcOffsetText.Should().Be("0");
        offsetValue.Should().Be(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12), TimeSpan.FromHours(2)));
        switchedValue.Should().Be(new DateTimeOffset(new DateTime(2020, 2, 29, 9, 11, 12), TimeSpan.Zero));
        offsetMinutes.Should().Be(120);
        offsetText.Should().Be("120");
        negativeOffsetMinutes.Should().Be(-210);
        negativeOffsetText.Should().Be("-210");
    }

    /// <summary>
    /// EN: Verifies SQL Server FROMPARTS temporal constructors keep the expected values for supported providers.
    /// PT: Verifica se os construtores temporais FROMPARTS do SQL Server mantem os valores esperados para os provedores suportados.
    /// </summary>
    [FidelityFact]
    public async Task TemporalFromPartsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        if (!dialect.SupportsSqlServerScalarFunction("DATEFROMPARTS"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (DateTime dateValue, DateTime dateTimeValue, DateTime dateTime2Value, DateTimeOffset dateTimeOffsetValue, TimeSpan timeValue, DateTime smallDateTimeValue)>(
                (s, a) => s.RunTemporalFromPartsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (DateTime dateValue, DateTime dateTimeValue, DateTime dateTime2Value, DateTimeOffset dateTimeOffsetValue, TimeSpan timeValue, DateTime smallDateTimeValue)>(
            (s, a) => s.RunTemporalFromPartsAsync());

        var (
            dateValue,
            dateTimeValue,
            dateTime2Value,
            dateTimeOffsetValue,
            timeValue,
            smallDateTimeValue) = result;

        dateValue.Should().Be(new DateTime(2020, 2, 29));
        dateTimeValue.Should().Be(new DateTime(2020, 2, 29, 10, 11, 12));
        dateTime2Value.Should().Be(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L));
        dateTimeOffsetValue.Should().Be(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), TimeSpan.FromMinutes(60)));
        timeValue.Should().Be(new TimeSpan(10, 11, 12).Add(TimeSpan.FromTicks(1234567 * 10L)));
        smallDateTimeValue.Should().Be(new DateTime(2020, 2, 29, 10, 11, 0));
    }

    /// <summary>
    /// EN: Verifies SQL Server EOMONTH returns the expected month-end date for supported providers.
    /// PT: Verifica se o SQL Server EOMONTH retorna a data final do mes esperada para os provedores suportados.
    /// </summary>
    [FidelityFact]
    public async Task TemporalEndOfMonthTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        if (!dialect.SupportsSqlServerDateFunction("EOMONTH"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, DateTime>(
                (s, a) => s.RunTemporalEndOfMonthAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, DateTime>(
            (s, a) => s.RunTemporalEndOfMonthAsync());

        result.Should().Be(new DateTime(2020, 2, 29));
    }

    /// <summary>
    /// EN: Verifies SQL Server DATEDIFF_BIG keeps the expected large-span differences for supported providers.
    /// PT: Verifica se o SQL Server DATEDIFF_BIG mantem as diferencas de intervalo grande esperadas para os provedores suportados.
    /// </summary>
    [FidelityFact]
    public async Task TemporalDateDiffBigTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, TemporalComparisonTolerance);

        if (!dialect.SupportsSqlServerDateFunction("DATEDIFF_BIG"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (long dayDiff, long weekDiff, long millisecondDiff, long microsecondDiff, long nanosecondDiff)>(
                (s, a) => s.RunTemporalDateDiffBigAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (long dayDiff, long weekDiff, long millisecondDiff, long microsecondDiff, long nanosecondDiff)>(
            (s, a) => s.RunTemporalDateDiffBigAsync());

        var (
            dayDiff,
            weekDiff,
            millisecondDiff,
            microsecondDiff,
            nanosecondDiff) = result;

        dayDiff.Should().Be(2L);
        weekDiff.Should().Be(1L);
        millisecondDiff.Should().Be(1L);
        microsecondDiff.Should().Be(1L);
        nanosecondDiff.Should().Be(100L);
    }

    /// <summary>
    /// EN: Verifies the current-time predicate benchmark counts the configured rows for the current provider.
    /// PT: Verifica se o benchmark de predicado de tempo atual conta as linhas configuradas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalNowWhereTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, int>(
            (s, a) => s.RunTemporalNowWhereAsync(a));
        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies the current-time ordering benchmark returns the expected first row for the current provider.
    /// PT: Verifica se o benchmark de ordenacao por tempo atual retorna a primeira linha esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalNowOrderByTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            TemporalComparisonTolerance,
            [(1, "Charlie"), (2, "Bravo"), (3, "Aaron")]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, string?>(
            (s, a) => s.RunTemporalNowOrderByAsync(a));
        result.Should().Be("Aaron");
    }

    private static async Task<(DateTime dateScalar, DateTime currentTimestamp, DateTime dateAdd, int whereCount, string orderedName)> RunScalarTemporalMatrixAsync(
        QueryServiceTest serviceTest)
    {
        var dateScalar = NormalizeDateTimeValue(await serviceTest.RunDateScalarAsync());
        var currentTimestamp = NormalizeDateTimeValue(await serviceTest.RunTemporalCurrentTimestampAsync());
        var dateAdd = NormalizeDateTimeValue(await serviceTest.RunTemporalDateAddAsync());
        var whereCount = await serviceTest.RunTemporalNowWhereAsync();
        var orderedName = await serviceTest.RunTemporalNowOrderByAsync() ?? string.Empty;
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
