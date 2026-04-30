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
    /// EN: Verifies text, boolean, integer, exact and approximate numeric, fixed-length text, bigint, binary, time, DateTimeOffset, temporal, and GUID columns round-trip consistently.
    /// PT: Verifica se colunas de texto, booleano, inteiro, numerico exato e aproximado, texto de tamanho fixo, bigint, binario, time, DateTimeOffset, temporais e GUID retornam valores consistentes.
    /// </summary>
    [FidelityFact]
    public async Task TypedFieldStorageMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

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
    /// EN: Verifies that JSON_MODIFY replaces a nested JSON property for the current provider.
    /// PT: Verifica se JSON_MODIFY substitui uma propriedade JSON aninhada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task JsonModifyReplaceTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonModifyReplaceAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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

        if (!dialect.SupportsJsonScalarRead)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunJsonQueryRootFragmentAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunStringEscapeAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunTranslateAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunFormatMessageAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunIsJsonAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunFormatAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunFormatAsync());

        result.Should().Be("0042");
    }

    /// <summary>
    /// EN: Verifies that ABS, CEILING, DEGREES, FLOOR, POWER, RADIANS, ROUND, SQRT, and SQUARE keep the expected math results for the current provider.
    /// PT: Verifica se ABS, CEILING, DEGREES, FLOOR, POWER, RADIANS, ROUND, SQRT e SQUARE mantem os resultados matematicos esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task MathFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerScalarFunction("ABS")
            || !dialect.SupportsSqlServerScalarFunction("CEILING")
            || !dialect.SupportsSqlServerScalarFunction("DEGREES")
            || !dialect.SupportsSqlServerScalarFunction("FLOOR")
            || !dialect.SupportsSqlServerScalarFunction("POWER")
            || !dialect.SupportsSqlServerScalarFunction("RADIANS")
            || !dialect.SupportsSqlServerScalarFunction("ROUND")
            || !dialect.SupportsSqlServerScalarFunction("SQRT")
            || !dialect.SupportsSqlServerScalarFunction("SQUARE"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunMathFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunMathFunctionsAsync());

        result.abs.Should().Be(10);
        result.ceiling.Should().Be(2);
        result.degrees.Should().BeApproximately(180d, 12);
        result.floor.Should().Be(1);
        result.power.Should().BeApproximately(8d, 12);
        result.radians.Should().BeApproximately(Math.PI, 12);
        result.round.Should().Be(1.24m);
        result.sqrt.Should().BeApproximately(3d, 12);
        result.square.Should().BeApproximately(9d, 12);
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunStringUtilityFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunStringUtilityFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunStringMetadataFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunStringMetadataFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerMetadataFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerMetadataFunctionsAsync());

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
        result.objectPropertyExIsProcedure.Should().Be(1);
        result.objectName.Should().Be("Users");
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunScopeIdentityAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunScopeIdentityAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerSystemFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerSystemFunctionsAsync());

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
        result.currentTimestamp.Should().BeCloseTo(DateTime.Now, TimeSpan.FromSeconds(10));
        result.getDate.Should().BeCloseTo(DateTime.Now, TimeSpan.FromSeconds(10));
        result.sysDateTime.Should().BeCloseTo(DateTime.Now, TimeSpan.FromSeconds(10));
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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerSpecialFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerSpecialFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerContextFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerContextFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerTransactionStateFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerTransactionStateFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerSessionFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerSessionFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunStringBasicFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunStringBasicFunctionsAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunParseFamilyAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunParseFamilyAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSoundexAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSoundexAsync());

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
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunCompressionAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunCompressionAsync());

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
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunApproxCountDistinctAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunApproxCountDistinctAsync());

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies PERCENTILE_CONT and PERCENTILE_DISC return the expected percentile values for the current provider.
    /// PT: Verifica se PERCENTILE_CONT e PERCENTILE_DISC retornam os valores percentis esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task PercentileAggregateFunctionsTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerAggregateFunction("PERCENTILE_CONT")
            || !dialect.SupportsSqlServerAggregateFunction("PERCENTILE_DISC"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunPercentileAggregatesAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunPercentileAggregatesAsync());

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
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsSqlServerAggregateFunction("CHECKSUM_AGG")
            || !dialect.SupportsSqlServerAggregateFunction("STRING_AGG"))
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
                (s, _) => s.RunSqlServerAggregateFunctionsAsync())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
            (s, _) => s.RunSqlServerAggregateFunctionsAsync());

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
