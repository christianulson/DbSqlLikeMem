using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared fidelity tests for typed columns and SQL function calculations across mock and container runs.
/// PT: Fornece testes de fidelidade compartilhados para colunas tipadas e calculos de funcoes entre execucoes mock e container.
/// </summary>
public abstract class FieldTypeFunctionTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that typed columns and common SQL functions keep consistent results for the current provider.
    /// PT: Verifica se colunas tipadas e funcoes SQL comuns mantem resultados consistentes para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldAndFunctionBlendTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, a) => s.RunTypedFieldAndFunctionBlendAsync(a));
    }

    /// <summary>
    /// EN: Verifies a single large projection query over typed columns and common SQL functions for the current provider.
    /// PT: Verifica uma unica consulta grande sobre colunas tipadas e funcoes SQL comuns para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldFunctionMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, a) => s.RunTypedFieldFunctionMatrixAsync(a));
    }

    /// <summary>
    /// EN: Verifies a second large projection query over typed columns with casts, arithmetic, and predicates for the current provider.
    /// PT: Verifica uma segunda consulta grande sobre colunas tipadas com casts, aritmetica e predicados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldCalculationMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, a) => s.RunTypedFieldCalculationMatrixAsync(a));
    }

    /// <summary>
    /// EN: Verifies text, boolean, integer, exact and approximate numeric, fixed-length text, bigint, binary, time, DateTimeOffset, and temporal columns round-trip consistently, and that Firebird rejects GUID binding in this matrix.
    /// PT: Verifica se colunas de texto, booleano, inteiro, numerico exato e aproximado, texto de tamanho fixo, bigint, binario, time, DateTimeOffset e colunas temporais retornam valores consistentes, e que o Firebird rejeita o bind de GUID nesta matriz.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldStorageMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (dialect.Provider == ProviderId.Firebird)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, a) => s.RunTypedFieldStorageMatrixAsync(a))).Should().ThrowAsync<ArgumentOutOfRangeException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, a) => s.RunTypedFieldStorageMatrixAsync(a));
    }

    /// <summary>
    /// EN: Verifies a large JSON projection query over typed columns for the current provider.
    /// PT: Verifica uma consulta grande de JSON sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonTypedFieldMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, a) => s.RunJsonTypedFieldMatrixAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, a) => s.RunJsonTypedFieldMatrixAsync(a));
    }

    /// <summary>
    /// EN: Verifies that JSON scalar reads return the expected text for the current provider.
    /// PT: Verifica se leituras escalares de JSON retornam o texto esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonScalarReadTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonScalarReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunJsonScalarReadAsync());

        result.Should().Be("Alice");
    }

    /// <summary>
    /// EN: Verifies that nested JSON path reads return the expected text for the current provider.
    /// PT: Verifica se leituras de caminho JSON aninhado retornam o texto esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonPathReadTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonPathReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunJsonPathReadAsync());

        result.Should().Be("Alice");
    }

    /// <summary>
    /// EN: Verifies that a missing JSON path returns a null value for the current provider.
    /// PT: Verifica se um caminho JSON ausente retorna um valor nulo para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonMissingPathReturnsNullTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonMissingPathReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunJsonMissingPathReadAsync());

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that the JSON insert and cast benchmark returns the expected null value for the current provider.
    /// PT: Verifica se o benchmark de insert e cast de JSON retorna o valor nulo esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonInsertCastReturnsNullTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonInsertCastAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunJsonInsertCastAsync());

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query over typed columns for the current provider.
    /// PT: Verifica uma consulta temporal grande sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalFieldMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTemporalFieldMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query that blends timestamp comparisons and fallback logic for the current provider.
    /// PT: Verifica uma consulta temporal grande que mistura comparacoes de timestamp e logica de fallback para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalComparisonMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTemporalComparisonMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a larger temporal arithmetic matrix over typed columns for the current provider.
    /// PT: Verifica uma matriz maior de aritmetica temporal sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalArithmeticMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTemporalArithmeticMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends casts, arithmetic, and text formatting over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura casts, aritmetica e formatacao textual sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CastCalculationMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunCastCalculationMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends null handling and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura tratamento de null e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task NullComparisonMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunNullComparisonMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large typed-field predicate query that blends LIKE, NOT LIKE, BETWEEN, and null checks for the current provider.
    /// PT: Verifica uma consulta grande de predicados em campos tipados que mistura LIKE, NOT LIKE, BETWEEN e verificacoes de null para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldPredicateMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTypedFieldPredicateMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a compound typed-field predicate query that blends OR, AND, LIKE, and null checks for the current provider.
    /// PT: Verifica uma consulta de predicado composto em campos tipados que mistura OR, AND, LIKE e verificacoes de null para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldCompoundPredicateMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTypedFieldCompoundPredicateMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends string lengths, trimming, and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura comprimentos de texto, trim e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TextLengthMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTextLengthMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends case conversion, trimming, prefix extraction, and text predicates over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura conversao de caixa, trim, extracao de prefixo e predicados de texto sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TextCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunTextCaseMatrixAsync());
    }
}

