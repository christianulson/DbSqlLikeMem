using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
using System.Text;

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

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
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

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
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

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunTypedFieldCalculationMatrixAsync(a));
    }

    /// <summary>
    /// EN: Verifies text, boolean, integer, exact and approximate numeric, fixed-length text, bigint, binary, time, DateTimeOffset, temporal, and GUID columns round-trip consistently.
    /// PT: Verifica se colunas de texto, booleano, inteiro, numerico exato e aproximado, texto de tamanho fixo, bigint, binario, time, DateTimeOffset, temporais e GUID retornam valores consistentes.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldStorageMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
                (s, a) => s.RunJsonTypedFieldMatrixAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonScalarReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonPathReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonMissingPathReadAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonInsertCastAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunJsonInsertCastAsync());

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that JSON_MODIFY replaces a nested JSON property for the current provider.
    /// PT: Verifica se JSON_MODIFY substitui uma propriedade JSON aninhada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonModifyReplaceTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("JSON_MODIFY"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonModifyReplaceAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunJsonModifyReplaceAsync());

        result.Should().Be("{\"profile\":{\"active\":true,\"name\":\"Bia\"}}");
    }

    /// <summary>
    /// EN: Verifies that JSON_QUERY without a path returns a raw root fragment for the current provider.
    /// PT: Verifica se JSON_QUERY sem path retorna um fragmento bruto de raiz para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonQueryRootFragmentTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonQueryFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunJsonQueryRootFragmentAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunJsonQueryRootFragmentAsync());

        result.Should().Be("{\"profile\":{\"active\":true,\"name\":\"Ana\"}}");
    }

    /// <summary>
    /// EN: Verifies that STRING_ESCAPE returns the expected escaped JSON text for the current provider.
    /// PT: Verifica se STRING_ESCAPE retorna o texto JSON escapado esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task StringEscapeTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("STRING_ESCAPE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunStringEscapeAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunStringEscapeAsync());

        result.Should().Be("\\\"Ana\\nBob\\\"");
    }

    /// <summary>
    /// EN: Verifies that TRANSLATE returns the expected translated text for the current provider.
    /// PT: Verifica se TRANSLATE retorna o texto traduzido esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TranslateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("TRANSLATE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunTranslateAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunTranslateAsync());

        result.Should().Be("xyc");
    }

    /// <summary>
    /// EN: Verifies that FORMATMESSAGE returns the expected formatted text for the current provider.
    /// PT: Verifica se FORMATMESSAGE retorna o texto formatado esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task FormatMessageTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("FORMATMESSAGE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunFormatMessageAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunFormatMessageAsync());

        result.Should().Be("Hello Bob #7");
    }

    /// <summary>
    /// EN: Verifies that ISJSON returns the expected validity flag for the current provider.
    /// PT: Verifica se ISJSON retorna a flag de validade esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task IsJsonTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("ISJSON"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                (s, _) => s.RunIsJsonAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            (s, _) => s.RunIsJsonAsync());

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that FORMAT returns the expected formatted text for the current provider.
    /// PT: Verifica se FORMAT retorna o texto formatado esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task FormatTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("FORMAT"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
                (s, _) => s.RunFormatAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, string?>(
            (s, _) => s.RunFormatAsync());

        result.Should().Be("0042");
    }

    /// <summary>
    /// EN: Verifies that ABS, CEIL/CEILING, DEGREES, FLOOR, LN/LOG10, POWER, RADIANS, ROUND, SIGN, SQRT, and SQUARE keep the expected math results for the current provider.
    /// PT: Verifica se ABS, CEIL/CEILING, DEGREES, FLOOR, LN/LOG10, POWER, RADIANS, ROUND, SIGN, SQRT e SQUARE mantem os resultados matematicos esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task MathFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int abs, int ceiling, double degrees, int floor, double naturalLog, double log10, double power, double radians, decimal round, int sign, double sqrt, double square)>(
                async (QueryServiceTest s, object[] _) => await s.RunMathFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int abs, int ceiling, double degrees, int floor, double naturalLog, double log10, double power, double radians, decimal round, int sign, double sqrt, double square)>(
            async (QueryServiceTest s, object[] _) => await s.RunMathFunctionsAsync());

        result.abs.Should().Be(10);
        result.ceiling.Should().Be(2);
        result.degrees.Should().BeApproximately(180d, 12);
        result.floor.Should().Be(1);
        result.naturalLog.Should().BeApproximately(1d, 12);
        result.log10.Should().BeApproximately(3d, 12);
        result.power.Should().BeApproximately(8d, 12);
        result.radians.Should().BeApproximately(Math.PI, 12);
        result.round.Should().Be(1.24m);
        result.sign.Should().Be(-1);
        result.sqrt.Should().BeApproximately(3d, 12);
        result.square.Should().BeApproximately(9d, 12);
    }

    /// <summary>
    /// EN: Verifies that LOG with an explicit base keeps the expected math result for providers that expose the two-argument form.
    /// PT: Verifica se LOG com base explicita mantem o resultado matematico esperado para provedores que expoem a forma com dois argumentos.
    /// </summary>
    [FidelityFact]
    public async Task MathLogBaseFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathLogBaseFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathLogBaseFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathLogBaseFunctionAsync());

        result.Should().BeApproximately(2d, 12);
    }

    /// <summary>
    /// EN: Verifies that LOG2 keeps the expected base-2 logarithm result for providers that expose the function.
    /// PT: Verifica se LOG2 mantem o resultado esperado do logaritmo de base 2 para provedores que expoem a funcao.
    /// </summary>
    [FidelityFact]
    public async Task MathLog2FunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathLog2Function)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathLog2FunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathLog2FunctionAsync());

        result.Should().BeApproximately(3d, 12);
    }

    /// <summary>
    /// EN: Verifies that PI keeps the expected math result for providers that expose the function.
    /// PT: Verifica se PI mantem o resultado matematico esperado para provedores que expoem a funcao.
    /// </summary>
    [FidelityFact]
    public async Task MathPiFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathPiFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathPiFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathPiFunctionAsync());

        result.Should().BeApproximately(Math.PI, 12);
    }

    /// <summary>
    /// EN: Verifies that RAND keeps the expected math result for providers that expose the function.
    /// PT: Verifica se RAND mantem o resultado matematico esperado para provedores que expoem a funcao.
    /// </summary>
    [FidelityFact]
    public async Task MathRandFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathRandFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathRandFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathRandFunctionAsync());

        result.Should().BeInRange(0d, 1d);
    }

    /// <summary>
    /// EN: Verifies that REMAINDER keeps the expected math result for providers that expose the function.
    /// PT: Verifica se REMAINDER mantem o resultado matematico esperado para provedores que expoem a funcao.
    /// </summary>
    [FidelityFact]
    public async Task MathRemainderFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathRemainderFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathRemainderFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathRemainderFunctionAsync());

        result.Should().BeApproximately(Math.IEEERemainder(7d, 3d), 12);
    }

    /// <summary>
    /// EN: Verifies that numeric TRUNC keeps the expected provider result, including scale when supported.
    /// PT: Verifica se TRUNC numerico mantem o resultado esperado do provedor, incluindo escala quando suportada.
    /// </summary>
    [FidelityFact]
    public async Task MathTruncFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathTruncFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (decimal trunc, decimal? truncScale)>(
                async (QueryServiceTest s, object[] _) => await s.RunMathTruncFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (decimal trunc, decimal? truncScale)>(
            async (QueryServiceTest s, object[] _) => await s.RunMathTruncFunctionAsync());

        result.trunc.Should().Be(1m);
        if (dialect.SupportsMathTruncScaleFunction)
        {
            result.truncScale.Should().Be(1.98m);
        }
        else
        {
            result.truncScale.Should().BeNull();
        }
    }

    /// <summary>
    /// EN: Verifies that COT keeps the expected math result for providers that expose the function.
    /// PT: Verifica se COT mantem o resultado matematico esperado para provedores que expoem a funcao.
    /// </summary>
    [FidelityFact]
    public async Task MathCotFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathCotFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
                async (QueryServiceTest s, object[] _) => await s.RunMathCotFunctionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, double>(
            async (QueryServiceTest s, object[] _) => await s.RunMathCotFunctionAsync());

        result.Should().BeApproximately(1d / Math.Tan(1d), 12);
    }

    /// <summary>
    /// EN: Verifies that BIN, GREATEST, LEAST, LOG2, MOD, POW, and TRUNCATE keep the expected MySQL-family results for providers that expose the functions.
    /// PT: Verifica se BIN, GREATEST, LEAST, LOG2, MOD, POW e TRUNCATE mantem os resultados esperados da familia MySQL para provedores que expoem as funcoes.
    /// </summary>
    [FidelityFact]
    public async Task MySqlUtilityMathFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMySqlUtilityMathFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string bin, int greatest, int least, double log2, decimal mod, double pow, decimal truncate)>(
                async (QueryServiceTest s, object[] _) => await s.RunMySqlUtilityMathFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string bin, int greatest, int least, double log2, decimal mod, double pow, decimal truncate)>(
            async (QueryServiceTest s, object[] _) => await s.RunMySqlUtilityMathFunctionsAsync());

        result.bin.Should().Be("110");
        result.greatest.Should().Be(5);
        result.least.Should().Be(1);
        result.log2.Should().BeApproximately(3d, 12);
        result.mod.Should().Be(1m);
        result.pow.Should().BeApproximately(8d, 12);
        result.truncate.Should().Be(12.34m);
    }

    /// <summary>
    /// EN: Verifies that GREATEST, LEAST, and MOD keep the expected shared results for providers that expose the functions.
    /// PT: Verifica se GREATEST, LEAST e MOD mantem os resultados compartilhados esperados para provedores que expoem as funcoes.
    /// </summary>
    [FidelityFact]
    public async Task GreatestLeastModFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsGreatestLeastModFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int greatest, int least, decimal mod)>(
                async (QueryServiceTest s, object[] _) => await s.RunGreatestLeastModFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int greatest, int least, decimal mod)>(
            async (QueryServiceTest s, object[] _) => await s.RunGreatestLeastModFunctionsAsync());

        result.greatest.Should().Be(5);
        result.least.Should().Be(1);
        result.mod.Should().Be(1m);
    }

    /// <summary>
    /// EN: Verifies that ABSVAL, MOD, TRUNC, and TRUNCATE keep the expected DB2 alias results for providers that expose the functions.
    /// PT: Verifica se ABSVAL, MOD, TRUNC e TRUNCATE mantem os resultados esperados dos aliases do DB2 para provedores que expoem as funcoes.
    /// </summary>
    [FidelityFact]
    public async Task Db2AliasMathFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsDb2AliasMathFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int absVal, decimal mod, decimal trunc, decimal truncate)>(
                async (QueryServiceTest s, object[] _) => await s.RunDb2AliasMathFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int absVal, decimal mod, decimal trunc, decimal truncate)>(
            async (QueryServiceTest s, object[] _) => await s.RunDb2AliasMathFunctionsAsync());

        result.absVal.Should().Be(10);
        result.mod.Should().Be(1m);
        result.trunc.Should().Be(1m);
        result.truncate.Should().Be(1.98m);
    }

    /// <summary>
    /// EN: Verifies that ABSVAL, BIN, COSH, SINH, TANH, and TRUNC keep the expected Firebird alias results for providers that expose the functions.
    /// PT: Verifica se ABSVAL, BIN, COSH, SINH, TANH e TRUNC mantem os resultados esperados dos aliases do Firebird para provedores que expoem as funcoes.
    /// </summary>
    [FidelityFact]
    public async Task FirebirdAliasMathFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsFirebirdAliasMathFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int absVal, string bin, double cosh, double sinh, double tanh, decimal trunc, decimal truncScale)>(
                async (QueryServiceTest s, object[] _) => await s.RunFirebirdAliasMathFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int absVal, string bin, double cosh, double sinh, double tanh, decimal trunc, decimal truncScale)>(
            async (QueryServiceTest s, object[] _) => await s.RunFirebirdAliasMathFunctionsAsync());

        result.absVal.Should().Be(5);
        result.bin.Should().Be("110");
        result.cosh.Should().BeApproximately(1d, 12);
        result.sinh.Should().BeApproximately(0d, 12);
        result.tanh.Should().BeApproximately(0d, 12);
        result.trunc.Should().Be(123m);
        result.truncScale.Should().Be(123.45m);
    }

    /// <summary>
    /// EN: Verifies that ACOS, ASIN, ATAN, ATAN2, COS, EXP, SIN, and TAN keep the expected transcendental results for the current provider.
    /// PT: Verifica se ACOS, ASIN, ATAN, ATAN2, COS, EXP, SIN e TAN mantem os resultados transcendentais esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task MathTranscendentalFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsMathTranscendentalFunctions)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (double acos, double asin, double atan, double atan2, double cos, double exp, double sin, double tan)>(
                async (QueryServiceTest s, object[] _) => await s.RunMathTranscendentalFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (double acos, double asin, double atan, double atan2, double cos, double exp, double sin, double tan)>(
            async (QueryServiceTest s, object[] _) => await s.RunMathTranscendentalFunctionsAsync());

        result.acos.Should().BeApproximately(0d, 12);
        result.asin.Should().BeApproximately(0d, 12);
        result.atan.Should().BeApproximately(0d, 12);
        result.atan2.Should().BeApproximately(0d, 12);
        result.cos.Should().BeApproximately(1d, 12);
        result.exp.Should().BeApproximately(Math.E, 12);
        result.sin.Should().BeApproximately(0d, 12);
        result.tan.Should().BeApproximately(0d, 12);
    }

    /// <summary>
    /// EN: Verifies that ASCII, CHARINDEX, BINARY_CHECKSUM, CHECKSUM, REPLICATE, REVERSE, SPACE, and STUFF keep the expected string results for the current provider.
    /// PT: Verifica se ASCII, CHARINDEX, BINARY_CHECKSUM, CHECKSUM, REPLICATE, REVERSE, SPACE e STUFF mantem os resultados de string esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task StringUtilityFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("ASCII")
            || !dialect.SupportsSqlServerScalarFunction("CHARINDEX")
            || !dialect.SupportsSqlServerScalarFunction("BINARY_CHECKSUM")
            || !dialect.SupportsSqlServerScalarFunction("CHECKSUM")
            || !dialect.SupportsSqlServerScalarFunction("REPLICATE")
            || !dialect.SupportsSqlServerScalarFunction("REVERSE")
            || !dialect.SupportsSqlServerScalarFunction("SPACE")
            || !dialect.SupportsSqlServerScalarFunction("STUFF"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int ascii, int charIndex, int binaryChecksumLower, int binaryChecksumUpper, int checksumLower, int checksumUpper, string replicate, string reverse, string space, string stuff)>(
                async (QueryServiceTest s, object[] _) => await s.RunStringUtilityFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int ascii, int charIndex, int binaryChecksumLower, int binaryChecksumUpper, int checksumLower, int checksumUpper, string replicate, string reverse, string space, string stuff)>(
            async (QueryServiceTest s, object[] _) => await s.RunStringUtilityFunctionsAsync());

        result.ascii.Should().Be(65);
        result.charIndex.Should().Be(4);
        result.binaryChecksumLower.Should().NotBe(result.binaryChecksumUpper);
        result.checksumLower.Should().Be(result.checksumUpper);
        result.replicate.Should().Be("NaNa");
        result.reverse.Should().Be("anA");
        result.space.Should().Be("   ");
        result.stuff.Should().Be("Axxa");
    }

    /// <summary>
    /// EN: Verifies that PARSENAME, QUOTENAME, and STR keep the expected metadata and formatting results for the current provider.
    /// PT: Verifica se PARSENAME, QUOTENAME e STR mantem os resultados esperados de metadados e formatacao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task StringMetadataFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("PARSENAME")
            || !dialect.SupportsSqlServerScalarFunction("QUOTENAME")
            || !dialect.SupportsSqlServerScalarFunction("STR"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string parsename, string quotename, string str)>(
                async (QueryServiceTest s, object[] _) => await s.RunStringMetadataFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string parsename, string quotename, string str)>(
            async (QueryServiceTest s, object[] _) => await s.RunStringMetadataFunctionsAsync());

        result.parsename.Should().Be("dbo");
        result.quotename.Should().Be("[Ana]");
        result.str.Should().Be(" 123.5");
    }

    /// <summary>
    /// EN: Verifies that SQL Server metadata, identifier, and system time helpers keep the expected values for the current provider.
    /// PT: Verifica se helpers de metadados, identificadores e tempo de sistema do SQL Server mantem os valores esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerMetadataFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataFunction("APP_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("CONNECTIONPROPERTY")
            || !dialect.SupportsSqlServerMetadataFunction("DATABASE_PRINCIPAL_ID")
            || !dialect.SupportsSqlServerMetadataFunction("DATABASEPROPERTYEX")
            || !dialect.SupportsSqlServerMetadataFunction("COLUMNPROPERTY")
            || !dialect.SupportsSqlServerMetadataFunction("COL_LENGTH")
            || !dialect.SupportsSqlServerMetadataFunction("COL_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("DB_ID")
            || !dialect.SupportsSqlServerMetadataFunction("DB_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("OBJECT_ID")
            || !dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTY")
            || !dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTYEX")
            || !dialect.SupportsSqlServerMetadataFunction("OBJECT_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("OBJECT_SCHEMA_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("ORIGINAL_DB_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("SCHEMA_ID")
            || !dialect.SupportsSqlServerMetadataFunction("SCHEMA_NAME")
            || !dialect.SupportsSqlServerMetadataIdentifier("CURRENT_USER")
            || !dialect.SupportsSqlServerDateFunction("GETUTCDATE")
            || !dialect.SupportsSqlServerDateFunction("SYSDATETIMEOFFSET")
            || !dialect.SupportsSqlServerDateFunction("SYSUTCDATETIME"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string appName, string localNetAddress, string netTransport, string databaseStatus, string databaseUpdateability, int databasePrincipalId, string currentUser, int nameColumnId, int identityColumnIsIdentity, int emailColumnAllowsNull, int colLength, string colName, int dbId, string dbName, int objectId, int objectPropertyIsTable, int objectPropertyExIsProcedure, string objectName, string objectSchemaName, string originalDbName, int schemaId, string schemaName, DateTime getUtcDate, DateTimeOffset sysDateTimeOffset, DateTime sysUtcDateTime)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerMetadataFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string appName, string localNetAddress, string netTransport, string databaseStatus, string databaseUpdateability, int databasePrincipalId, string currentUser, int nameColumnId, int identityColumnIsIdentity, int emailColumnAllowsNull, int colLength, string colName, int dbId, string dbName, int objectId, int objectPropertyIsTable, int objectPropertyExIsProcedure, string objectName, string objectSchemaName, string originalDbName, int schemaId, string schemaName, DateTime getUtcDate, DateTimeOffset sysDateTimeOffset, DateTime sysUtcDateTime)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerMetadataFunctionsAsync());

        result.appName.Should().Be("DbSqlLikeMem");
        result.localNetAddress.Should().Be("127.0.0.1");
        result.netTransport.Should().Be("TCP");
        result.databaseStatus.Should().Be("ONLINE");
        result.databaseUpdateability.Should().Be("READ_WRITE");
        result.databasePrincipalId.Should().Be(1);
        result.currentUser.Should().Be("dbo");
        result.nameColumnId.Should().Be(2);
        result.identityColumnIsIdentity.Should().Be(1);
        result.emailColumnAllowsNull.Should().Be(1);
        result.colLength.Should().Be(4);
        result.colName.Should().Be("Name");
        result.dbId.Should().Be(1);
        result.dbName.Should().Be("DefaultSchema");
        result.objectId.Should().Be(2);
        result.objectPropertyIsTable.Should().Be(1);
        result.objectPropertyExIsProcedure.Should().Be(0);
        result.objectName.Should().StartWith("users_");
        result.objectSchemaName.Should().Be("DefaultSchema");
        result.originalDbName.Should().Be("DefaultSchema");
        result.schemaId.Should().Be(1);
        result.schemaName.Should().Be("dbo");
        result.getUtcDate.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
        result.sysDateTimeOffset.Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromSeconds(10));
        result.sysUtcDateTime.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// EN: Verifies that SCOPE_IDENTITY returns the last generated identity value for the current provider.
    /// PT: Verifica se SCOPE_IDENTITY retorna o ultimo valor identity gerado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ScopeIdentityTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataFunction("SCOPE_IDENTITY"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
                async (s, _) => await s.RunScopeIdentityAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            async (s, _) => await s.RunScopeIdentityAsync());

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that SQL Server system and identity helpers keep the expected values for the current provider.
    /// PT: Verifica se helpers de sistema e identidade do SQL Server mantem os valores esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerSystemFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataIdentifier("@@DATEFIRST")
            || !dialect.SupportsSqlServerMetadataIdentifier("@@IDENTITY")
            || !dialect.SupportsSqlServerMetadataIdentifier("@@MAX_PRECISION")
            || !dialect.SupportsSqlServerMetadataFunction("SERVERPROPERTY")
            || !dialect.SupportsSqlServerMetadataFunction("ORIGINAL_LOGIN")
            || !dialect.SupportsSqlServerMetadataFunction("CURRENT_REQUEST_ID")
            || !dialect.SupportsSqlServerMetadataFunction("SESSION_ID")
            || !dialect.SupportsSqlServerMetadataFunction("TYPE_ID")
            || !dialect.SupportsSqlServerMetadataFunction("TYPE_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("TYPEPROPERTY")
            || !dialect.SupportsSqlServerMetadataIdentifier("SESSION_USER")
            || !dialect.SupportsSqlServerMetadataFunction("SUSER_ID")
            || !dialect.SupportsSqlServerMetadataFunction("SUSER_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("SUSER_SID")
            || !dialect.SupportsSqlServerMetadataFunction("SUSER_SNAME")
            || !dialect.SupportsSqlServerMetadataIdentifier("SYSTEM_USER")
            || !dialect.SupportsSqlServerMetadataFunction("USER_ID")
            || !dialect.SupportsSqlServerMetadataFunction("USER_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("XACT_STATE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int dateFirst, int identity, int maxPrecision, string serverProperty, string originalLogin, int currentRequestId, int sessionId, int typeId, string typeName, int typeProperty, string sessionUser, int suserId, string suserName, byte[] suserSid, string suserSname, string systemUser, int userId, string userName, int xactState, DateTime currentTimestamp, DateTime getDate, DateTime sysDateTime)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerSystemFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int dateFirst, int identity, int maxPrecision, string serverProperty, string originalLogin, int currentRequestId, int sessionId, int typeId, string typeName, int typeProperty, string sessionUser, int suserId, string suserName, byte[] suserSid, string suserSname, string systemUser, int userId, string userName, int xactState, DateTime currentTimestamp, DateTime getDate, DateTime sysDateTime)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerSystemFunctionsAsync());

        result.dateFirst.Should().Be(7);
        result.identity.Should().Be(1);
        result.maxPrecision.Should().Be(38);
        result.serverProperty.Should().Be("2022");
        result.originalLogin.Should().Be("sa");
        result.currentRequestId.Should().Be(1);
        result.sessionId.Should().Be(1);
        result.typeId.Should().Be(56);
        result.typeName.Should().Be("int");
        result.typeProperty.Should().Be(1);
        result.sessionUser.Should().Be("dbo");
        result.suserId.Should().Be(1);
        result.suserName.Should().Be("sa");
        result.suserSid.Should().Equal(new byte[] { 0x01 });
        result.suserSname.Should().Be("sa");
        result.systemUser.Should().Be("sa");
        result.userId.Should().Be(1);
        result.userName.Should().Be("dbo");
        result.xactState.Should().Be(0);
        result.currentTimestamp.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
        result.getDate.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
        result.sysDateTime.Should().BeCloseTo(DateTime.UtcNow, TimeSpan.FromSeconds(10));
    }

    /// <summary>
    /// EN: Verifies @@TEXTSIZE and NEWSEQUENTIALID keep the expected values for the current provider.
    /// PT: Verifica se @@TEXTSIZE e NEWSEQUENTIALID mantem os valores esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerSpecialFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataIdentifier("@@TEXTSIZE")
            || !dialect.SupportsSqlServerScalarFunction("NEWSEQUENTIALID"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int textSize, string newSequentialId)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerSpecialFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int textSize, string newSequentialId)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerSpecialFunctionsAsync());

        result.textSize.Should().Be(4096);
        Guid.TryParse(result.newSequentialId, out _).Should().BeTrue();
    }

    /// <summary>
    /// EN: Verifies that SQL Server context-info and session-context helpers keep the expected values for the current provider.
    /// PT: Verifica se helpers de context-info e session-context do SQL Server mantem os valores esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerContextFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataFunction("SESSION_CONTEXT"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (byte[] contextInfo, int sessionContextTenant, string? sessionContextMissing)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerContextFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (byte[] contextInfo, int sessionContextTenant, string? sessionContextMissing)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerContextFunctionsAsync());

        result.contextInfo.Should().Equal(new byte[] { 0x0A, 0x0B });
        result.sessionContextTenant.Should().Be(42);
        result.sessionContextMissing.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that SQL Server transaction-state helpers reflect an active transaction for the current provider.
    /// PT: Verifica se helpers de estado de transacao do SQL Server refletem uma transacao ativa para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerTransactionStateFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataFunction("CURRENT_TRANSACTION_ID")
            || !dialect.SupportsSqlServerMetadataFunction("XACT_STATE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int xactState, long currentTransactionId)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerTransactionStateFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int xactState, long currentTransactionId)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerTransactionStateFunctionsAsync());

        result.xactState.Should().Be(1);
        result.currentTransactionId.Should().Be(1L);
    }

    /// <summary>
    /// EN: Verifies that SQL Server metadata and session-style helpers keep the expected values for the current provider.
    /// PT: Verifica se helpers de metadados e sessao do SQL Server mantem os valores esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerSessionFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerMetadataFunction("GETANSINULL")
            || !dialect.SupportsSqlServerMetadataFunction("HOST_ID")
            || !dialect.SupportsSqlServerMetadataFunction("HOST_NAME")
            || !dialect.SupportsSqlServerMetadataFunction("IS_MEMBER")
            || !dialect.SupportsSqlServerMetadataFunction("IS_ROLEMEMBER")
            || !dialect.SupportsSqlServerMetadataFunction("IS_SRVROLEMEMBER")
            || !dialect.SupportsSqlServerScalarFunction("DATALENGTH")
            || !dialect.SupportsSqlServerScalarFunction("GROUPING")
            || !dialect.SupportsSqlServerScalarFunction("GROUPING_ID")
            || !dialect.SupportsSqlServerScalarFunction("ISDATE")
            || !dialect.SupportsSqlServerScalarFunction("PATINDEX"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int getAnsiNull, int dataLength, int grouping, int groupingId, int hostId, string hostName, int isMember, int isRoleMember, int isSrvRoleMember, int isDateValid, int isDateInvalid, int patIndex, int patIndexMissing)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerSessionFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int getAnsiNull, int dataLength, int grouping, int groupingId, int hostId, string hostName, int isMember, int isRoleMember, int isSrvRoleMember, int isDateValid, int isDateInvalid, int patIndex, int patIndexMissing)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerSessionFunctionsAsync());

        result.getAnsiNull.Should().Be(1);
        result.dataLength.Should().Be(4);
        result.grouping.Should().Be(0);
        result.groupingId.Should().Be(0);
        result.hostId.Should().Be(1);
        result.hostName.Should().Be("localhost");
        result.isMember.Should().Be(0);
        result.isRoleMember.Should().Be(0);
        result.isSrvRoleMember.Should().Be(1);
        result.isDateValid.Should().Be(1);
        result.isDateInvalid.Should().Be(0);
        result.patIndex.Should().Be(5);
        result.patIndexMissing.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that LEN, LTRIM, RTRIM, and UNICODE keep the expected text and code-point results for the current provider.
    /// PT: Verifica se LEN, LTRIM, RTRIM e UNICODE mantem os resultados de texto e codigo esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task StringBasicFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("LEN")
            || !dialect.SupportsSqlServerScalarFunction("LTRIM")
            || !dialect.SupportsSqlServerScalarFunction("RTRIM")
            || !dialect.SupportsSqlServerScalarFunction("UNICODE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (long length, string ltrim, string rtrim, int unicode)>(
                async (QueryServiceTest s, object[] _) => await s.RunStringBasicFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (long length, string ltrim, string rtrim, int unicode)>(
            async (QueryServiceTest s, object[] _) => await s.RunStringBasicFunctionsAsync());

        result.length.Should().Be(3);
        result.ltrim.Should().Be("Ana");
        result.rtrim.Should().Be("Ana");
        result.unicode.Should().Be(65);
    }

    /// <summary>
    /// EN: Verifies that PARSE, TRY_CONVERT, and TRY_PARSE keep the expected conversion results for the current provider.
    /// PT: Verifica se PARSE, TRY_CONVERT e TRY_PARSE mantem os resultados de conversao esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParseFamilyTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("PARSE")
            || !dialect.SupportsSqlServerScalarFunction("TRY_CONVERT")
            || !dialect.SupportsSqlServerScalarFunction("TRY_PARSE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int parse, object? tryConvertNull, int tryConvertValue, object? tryParseNull, int tryParseValue)>(
                async (QueryServiceTest s, object[] _) => await s.RunParseFamilyAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (int parse, object? tryConvertNull, int tryConvertValue, object? tryParseNull, int tryParseValue)>(
            async (QueryServiceTest s, object[] _) => await s.RunParseFamilyAsync());

        result.parse.Should().Be(42);
        result.tryConvertNull.Should().Be(DBNull.Value);
        result.tryConvertValue.Should().Be(42);
        result.tryParseNull.Should().Be(DBNull.Value);
        result.tryParseValue.Should().Be(42);
    }

    /// <summary>
    /// EN: Verifies that SOUNDEX and DIFFERENCE return the expected phonetic match values for the current provider.
    /// PT: Verifica se SOUNDEX e DIFFERENCE retornam os valores foneticos esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SoundexTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("SOUNDEX")
            || !dialect.SupportsSqlServerScalarFunction("DIFFERENCE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string soundex, int difference)>(
                async (QueryServiceTest s, object[] _) => await s.RunSoundexAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (string soundex, int difference)>(
            async (QueryServiceTest s, object[] _) => await s.RunSoundexAsync());

        result.soundex.Should().Be("R163");
        result.difference.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies that COMPRESS and DECOMPRESS keep the expected binary payload for the current provider.
    /// PT: Verifica se COMPRESS e DECOMPRESS mantem o payload binario esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CompressionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("COMPRESS")
            || !dialect.SupportsSqlServerScalarFunction("DECOMPRESS"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (byte[] compressed, byte[] decompressed)>(
                async (QueryServiceTest s, object[] _) => await s.RunCompressionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, (byte[] compressed, byte[] decompressed)>(
            async (QueryServiceTest s, object[] _) => await s.RunCompressionAsync());

        result.compressed.Should().NotBeEmpty();
        result.decompressed.Should().Equal(Encoding.Unicode.GetBytes("Ana"));
    }

    /// <summary>
    /// EN: Verifies APPROX_COUNT_DISTINCT returns the expected approximate count for the current provider.
    /// PT: Verifica se APPROX_COUNT_DISTINCT retorna a contagem aproximada esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ApproxCountDistinctTest()
    {
        object?[][] initialData = [[(1, "Ana"), (2, "Bob")]];
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        if (!dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, int>(
                async (s, _) => await s.RunApproxCountDistinctAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, int>(
            async (s, _) => await s.RunApproxCountDistinctAsync());

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies PERCENTILE_CONT and PERCENTILE_DISC return the expected percentile values for the current provider.
    /// PT: Verifica se PERCENTILE_CONT e PERCENTILE_DISC retornam os valores percentis esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task PercentileAggregateFunctionsTest()
    {
        object?[][] initialData = [[(1, "Ana"), (2, "Bob")]];
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        if (!dialect.SupportsSqlServerAggregateFunction("PERCENTILE_CONT")
            || !dialect.SupportsSqlServerAggregateFunction("PERCENTILE_DISC"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (double continuous, double discrete)>(
                async (QueryServiceTest s, object[] _) => await s.RunPercentileAggregatesAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (double continuous, double discrete)>(
            async (QueryServiceTest s, object[] _) => await s.RunPercentileAggregatesAsync());

        result.continuous.Should().Be(1.5d);
        result.discrete.Should().Be(1d);
    }

    /// <summary>
    /// EN: Verifies SQL Server aggregate helpers keep the expected count, checksum, string, and statistical results for the current provider.
    /// PT: Verifica se agregadores do SQL Server mantem os resultados esperados de contagem, checksum, string e estatistica para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SqlServerAggregateFunctionsTest()
    {
        object?[][] initialData = [[(1, "Ana"), (2, "Bob")]];
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        if (!dialect.SupportsSqlServerAggregateFunction("CHECKSUM_AGG")
            || !dialect.SupportsSqlServerAggregateFunction("STRING_AGG"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<UsersScenario, QueryServiceTest, (long countBig, int checksumAgg, string stringAggOrdered, double stdev, double stdevp, double variance, double varp)>(
                async (QueryServiceTest s, object[] _) => await s.RunSqlServerAggregateFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, (long countBig, int checksumAgg, string stringAggOrdered, double stdev, double stdevp, double variance, double varp)>(
            async (QueryServiceTest s, object[] _) => await s.RunSqlServerAggregateFunctionsAsync());

        result.countBig.Should().Be(2L);
        result.checksumAgg.Should().NotBe(0);
        result.stringAggOrdered.Should().Be("Bob,Ana");
        result.stdev.Should().BeApproximately(Math.Sqrt(0.5d), 12);
        result.stdevp.Should().BeApproximately(0.5d, 12);
        result.variance.Should().BeApproximately(0.5d, 12);
        result.varp.Should().BeApproximately(0.25d, 12);
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query over typed columns for the current provider.
    /// PT: Verifica uma consulta temporal grande sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalFieldMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTemporalFieldMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large temporal projection query that blends timestamp comparisons and fallback logic for the current provider.
    /// PT: Verifica uma consulta temporal grande que mistura comparacoes de timestamp e logica de fallback para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalComparisonMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTemporalComparisonMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a larger temporal arithmetic matrix over typed columns for the current provider.
    /// PT: Verifica uma matriz maior de aritmetica temporal sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TemporalArithmeticMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTemporalArithmeticMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends casts, arithmetic, and text formatting over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura casts, aritmetica e formatacao textual sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task CastCalculationMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunCastCalculationMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends null handling and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura tratamento de null e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task NullComparisonMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunNullComparisonMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large typed-field predicate query that blends LIKE, NOT LIKE, BETWEEN, and null checks for the current provider.
    /// PT: Verifica uma consulta grande de predicados em campos tipados que mistura LIKE, NOT LIKE, BETWEEN e verificacoes de null para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldPredicateMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTypedFieldPredicateMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a compound typed-field predicate query that blends OR, AND, LIKE, and null checks for the current provider.
    /// PT: Verifica uma consulta de predicado composto em campos tipados que mistura OR, AND, LIKE e verificacoes de null para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldCompoundPredicateMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            async (s, _) => await s.RunTypedFieldCompoundPredicateMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends string lengths, trimming, and comparisons over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura comprimentos de texto, trim e comparacoes sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TextLengthMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTextLengthMatrixAsync());
    }

    /// <summary>
    /// EN: Verifies a large projection query that blends case conversion, trimming, prefix extraction, and text predicates over typed columns for the current provider.
    /// PT: Verifica uma consulta grande que mistura conversao de caixa, trim, extracao de prefixo e predicados de texto sobre colunas tipadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task TextCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) => await s.RunTextCaseMatrixAsync());
    }
}
