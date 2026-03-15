namespace DbSqlLikeMem.Npgsql.Test;

/// <summary>
/// EN: Validates PostgreSQL function execution for common scalar and aggregate utilities.
/// PT: Valida a execucao de funcoes PostgreSQL para utilitarios escalares e agregados comuns.
/// </summary>
public sealed class NpgsqlFunctionTests
    : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _connection;

    /// <summary>
    /// EN: Initializes PostgreSQL function test fixtures with sample tables.
    /// PT: Inicializa os fixtures de funcoes PostgreSQL com tabelas de exemplo.
    /// </summary>
    public NpgsqlFunctionTests(ITestOutputHelper helper)
        : base(helper)
    {
        _connection = CreateOpenConnection();
    }

    /// <summary>
    /// EN: Ensures PostgreSQL system functions return expected values.
    /// PT: Garante que funcoes de sistema do PostgreSQL retornem valores esperados.
    /// </summary>
    [Theory]
    [MemberDataNpgsqlVersion]
    [Trait("Category", "PostgreSqlMock")]
    public void SystemFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal("postgres", ExecuteScalar(connection, "SELECT CURRENT_USER FROM Users WHERE Id = 1"));
        Assert.Equal("postgres", ExecuteScalar(connection, "SELECT CURRENT_ROLE() FROM Users WHERE Id = 1"));
        Assert.Equal("postgres", ExecuteScalar(connection, "SELECT CURRENT_DATABASE() FROM Users WHERE Id = 1"));
        Assert.Equal("postgres", ExecuteScalar(connection, "SELECT CURRENT_CATALOG() FROM Users WHERE Id = 1"));
        Assert.Equal("public", ExecuteScalar(connection, "SELECT CURRENT_SCHEMA() FROM Users WHERE Id = 1"));
        Assert.Equal("\"$user\", public", ExecuteScalar(connection, "SELECT CURRENT_SETTING('search_path') FROM Users WHERE Id = 1"));
        Assert.Equal("DbSqlLikeMem", ExecuteScalar(connection, "SELECT CURRENT_SETTING('application_name') FROM Users WHERE Id = 1"));
        Assert.Equal(version.ToString(CultureInfo.InvariantCulture), ExecuteScalar(connection, "SELECT CURRENT_SETTING('server_version') FROM Users WHERE Id = 1"));
        Assert.Equal($"{version}0000", ExecuteScalar(connection, "SELECT CURRENT_SETTING('server_version_num') FROM Users WHERE Id = 1"));
        Assert.Equal($"PostgreSQL {version}", ExecuteScalar(connection, "SELECT VERSION() FROM Users WHERE Id = 1"));
        Assert.Equal("SELECT CURRENT_QUERY() FROM Users WHERE Id = 1", ExecuteScalar(connection, "SELECT CURRENT_QUERY() FROM Users WHERE Id = 1"));

        var schemas = ExecuteScalar(connection, "SELECT CURRENT_SCHEMAS() FROM Users WHERE Id = 1");
        var schemaList = Assert.IsType<string[]>(schemas);
        Assert.Contains("public", schemaList);

        Assert.Equal(new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day), ((DateTime)ExecuteScalar(connection, "SELECT CURRENT_DATE FROM Users WHERE Id = 1")!).Date);
        Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT CURRENT_TIME FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT_TIMESTAMP FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT NOW() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CLOCK_TIMESTAMP() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT STATEMENT_TIMESTAMP() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT TRANSACTION_TIMESTAMP() FROM Users WHERE Id = 1"));
        Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT LOCALTIME() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT LOCALTIMESTAMP() FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL date helpers return expected values.
    /// PT: Garante que helpers de data do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void DateFunctions_ShouldReturnExpectedValues()
    {
        var trunc = ExecuteScalar("SELECT DATE_TRUNC('day', '2024-01-02 03:04:05') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 2), (DateTime)trunc!);

        var part = Convert.ToDouble(ExecuteScalar("SELECT DATE_PART('month', '2024-02-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(2d, part);

        var extract = Convert.ToInt32(ExecuteScalar("SELECT EXTRACT(MONTH FROM '2024-02-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(2, extract);

        var age = ExecuteScalar("SELECT AGE('2024-01-02', '2024-01-01') FROM Users WHERE Id = 1");
        var span = Assert.IsType<TimeSpan>(age);
        Assert.Equal(1d, span.TotalDays, 5);

        var makeDate = ExecuteScalar("SELECT MAKE_DATE(2024, 1, 2) FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 2), (DateTime)makeDate!);

        var makeTime = ExecuteScalar("SELECT MAKE_TIME(3, 4, 5) FROM Users WHERE Id = 1");
        Assert.Equal(new TimeSpan(3, 4, 5), (TimeSpan)makeTime!);

        var makeTimestamp = ExecuteScalar("SELECT MAKE_TIMESTAMP(2024, 1, 2, 3, 4, 5) FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5), (DateTime)makeTimestamp!);

        var makeTimestampTz = ExecuteScalar("SELECT MAKE_TIMESTAMPTZ(2024, 1, 2, 3, 4, 5) FROM Users WHERE Id = 1");
        var dateTimeOffset = Assert.IsType<DateTimeOffset>(makeTimestampTz);
        Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5), dateTimeOffset.DateTime);

        var makeInterval = ExecuteScalar("SELECT MAKE_INTERVAL(0, 0, 1, 2, 3, 4, 5) FROM Users WHERE Id = 1");
        Assert.Equal(new TimeSpan(9, 3, 4, 5), Assert.IsType<TimeSpan>(makeInterval));

        var toDate = ExecuteScalar("SELECT TO_DATE('2024-01-02','YYYY-MM-DD') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 2), ((DateTime)toDate!).Date);

        var toCharDate = ExecuteScalar("SELECT TO_CHAR(TO_DATE('2024-01-02','YYYY-MM-DD'),'YYYY-MM-DD') FROM Users WHERE Id = 1");
        Assert.Equal("2024-01-02", toCharDate);
    }

    /// <summary>
    /// EN: Ensures common PostgreSQL numeric helpers return expected values.
    /// PT: Garante que helpers numericos comuns do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void NumericFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(10, Convert.ToInt32(ExecuteScalar("SELECT ABS(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT CEIL(1.2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT CEILING(1.1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT FLOOR(1.9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar("SELECT ASCII('A') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var ln = Convert.ToDouble(ExecuteScalar("SELECT LN(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(0d, ln);

        var exp = Convert.ToDouble(ExecuteScalar("SELECT EXP(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.True(exp > 2.7d && exp < 2.8d);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL trigonometric and power helpers return expected values.
    /// PT: Garante que helpers trigonometricos e de potencia do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TrigonometricFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ACOS(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ASIN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ATAN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ATAN2(0, 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar("SELECT CBRT(8) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT COS(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);

        var cot = Convert.ToDouble(ExecuteScalar("SELECT COT(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.True(cot > 0d);

        Assert.Equal(180d, Convert.ToDouble(ExecuteScalar("SELECT DEGREES(PI()) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar("SELECT PI() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(8d, Convert.ToDouble(ExecuteScalar("SELECT POWER(2, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar("SELECT RADIANS(180) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT SIN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(3d, Convert.ToDouble(ExecuteScalar("SELECT SQRT(9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT TAN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 10);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL text helpers return expected values.
    /// PT: Garante que helpers de texto do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void TextFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("Ana", ExecuteScalar("SELECT BTRIM('  Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal(24, Convert.ToInt32(ExecuteScalar("SELECT BIT_LENGTH('abc') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Ana", ExecuteScalar("SELECT LTRIM('  Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT RTRIM('Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana Maria", ExecuteScalar("SELECT INITCAP('ana maria') FROM Users WHERE Id = 1"));
        Assert.Equal("A", ExecuteScalar("SELECT CHR(65) FROM Users WHERE Id = 1"));
        Assert.Equal("b", ExecuteScalar("SELECT SPLIT_PART('a,b,c', ',', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("fallback", ExecuteScalar("SELECT COALESCE(NULL, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal("AnaMaria", ExecuteScalar("SELECT CONCAT('Ana', 'Maria') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana-Maria", ExecuteScalar("SELECT CONCAT_WS('-', 'Ana', NULL, 'Maria') FROM Users WHERE Id = 1"));
        Assert.Equal(3L, ExecuteScalar("SELECT CHAR_LENGTH('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("An", ExecuteScalar("SELECT LEFT('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar("SELECT LENGTH('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("ana", ExecuteScalar("SELECT LOWER('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal(DBNull.Value, ExecuteScalar("SELECT NULLIF('Ana', 'Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("na", ExecuteScalar("SELECT RIGHT('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("na", ExecuteScalar("SELECT SUBSTR('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("An", ExecuteScalar("SELECT SUBSTRING('Ana', 1, 2) FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT TRIM('  Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal("ANA", ExecuteScalar("SELECT UPPER('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal(4L, ExecuteScalar("SELECT CHARACTER_LENGTH('Ana ') FROM Users WHERE Id = 1"));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar("SELECT OCTET_LENGTH('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("ababab", ExecuteScalar("SELECT REPEAT('ab', 3) FROM Users WHERE Id = 1"));
        Assert.Equal("00abc", ExecuteScalar("SELECT LPAD('abc', 5, '0') FROM Users WHERE Id = 1"));
        Assert.Equal("abc00", ExecuteScalar("SELECT RPAD('abc', 5, '0') FROM Users WHERE Id = 1"));
        Assert.Equal("cba", ExecuteScalar("SELECT REVERSE('abc') FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT STARTS_WITH('prefix-value', 'pre') FROM Users WHERE Id = 1"));
        Assert.Equal("xyc", ExecuteScalar("SELECT TRANSLATE('abc','ab','xy') FROM Users WHERE Id = 1"));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT POSITION('bar', 'foobar') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MIN_SCALE(8.4100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var parts = ExecuteScalar("SELECT STRING_TO_ARRAY('a,b,c', ',') FROM Users WHERE Id = 1");
        Assert.Equal(new[] { "a", "b", "c" }, Assert.IsType<string[]>(parts));

        var identifierParts = ExecuteScalar("SELECT PARSE_IDENT('public.\"User Table\"') FROM Users WHERE Id = 1");
        Assert.Equal(new[] { "public", "User Table" }, Assert.IsType<string[]>(identifierParts));

        Assert.Equal("'O''Reilly'", ExecuteScalar("SELECT QUOTE_LITERAL('O''Reilly') FROM Users WHERE Id = 1"));
        Assert.Equal("\"User Name\"", ExecuteScalar("SELECT QUOTE_IDENT('User Name') FROM Users WHERE Id = 1"));
        Assert.Equal("ff", ExecuteScalar("SELECT TO_HEX(255) FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL formatting helpers return expected values for text and numeric formatting.
    /// PT: Garante que helpers de formatacao do PostgreSQL retornem valores esperados para formatacao textual e numerica.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void FormattingFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(" 1234.50", ExecuteScalar("SELECT TO_CHAR(1234.5, '9999D00') FROM Users WHERE Id = 1"));
        Assert.Equal("hello Ana, \"User Name\", 'O''Reilly', %", ExecuteScalar("SELECT FORMAT('hello %s, %I, %L, %%', 'Ana', 'User Name', 'O''Reilly') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL unicode helpers return expected normalized and ASCII-only text.
    /// PT: Garante que helpers unicode do PostgreSQL retornem texto normalizado e somente ASCII.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void UnicodeFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("Jose", ExecuteScalar("SELECT TO_ASCII('Jos\u00E9') FROM Users WHERE Id = 1"));
        Assert.Equal("\u00E9", ExecuteScalar("SELECT NORMALIZE('e\u0301', 'NFC') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL network helpers return expected host and mask information.
    /// PT: Garante que helpers de rede do PostgreSQL retornem as informacoes esperadas de host e mascara.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void NetworkFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("192.168.10.5", ExecuteScalar("SELECT HOST('192.168.10.5/24') FROM Users WHERE Id = 1"));
        Assert.Equal(24, Convert.ToInt32(ExecuteScalar("SELECT MASKLEN('192.168.10.5/24') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("255.255.255.0", ExecuteScalar("SELECT NETMASK('192.168.10.5/24') FROM Users WHERE Id = 1"));
        Assert.Equal("0.0.0.255", ExecuteScalar("SELECT HOSTMASK('192.168.10.5/24') FROM Users WHERE Id = 1"));
        Assert.Equal("192.168.10.0/24", ExecuteScalar("SELECT NETWORK('192.168.10.5/24') FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT INET_SAME_FAMILY('192.168.10.5/24', '10.0.0.1/8') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL comparison helpers and digest functions return expected values.
    /// PT: Garante que helpers de comparacao e funcoes de digest do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ComparisonAndDigestFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(9, Convert.ToInt32(ExecuteScalar("SELECT GREATEST(5, 2, 9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT LEAST(5, 2, 9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(true, ExecuteScalar("SELECT 'Ana' LIKE 'A%' FROM Users WHERE Id = 1"));
        Assert.Equal("900150983cd24fb0d6963f7d28e17f72", ExecuteScalar("SELECT MD5('abc') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL random helpers follow provider-specific return types and ranges.
    /// PT: Garante que helpers aleatorios do PostgreSQL sigam os tipos de retorno e intervalos especificos do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void RandomFunctions_ShouldReturnExpectedValues()
    {
        var random = Assert.IsType<double>(ExecuteScalar("SELECT RANDOM() FROM Users WHERE Id = 1"));
        Assert.InRange(random, 0d, 1d);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL regex helpers return expected values for search, replace and split operations.
    /// PT: Garante que helpers de regex do PostgreSQL retornem valores esperados para operacoes de busca, substituicao e split.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void RegexFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT REGEXP_COUNT('abc123abc', 'abc') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT REGEXP_INSTR('abc123abc', '123') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(true, ExecuteScalar("SELECT REGEXP_LIKE('abc123', '[0-9]+') FROM Users WHERE Id = 1"));

        var match = Assert.IsType<string[]>(ExecuteScalar("SELECT REGEXP_MATCH('abc123', '([a-z]+)([0-9]+)') FROM Users WHERE Id = 1"));
        Assert.Equal(["abc", "123"], match);

        Assert.Equal("abc-abc", ExecuteScalar("SELECT REGEXP_REPLACE('abc123abc', '[0-9]+', '-') FROM Users WHERE Id = 1"));

        var split = Assert.IsType<string[]>(ExecuteScalar("SELECT REGEXP_SPLIT_TO_ARRAY('a,b;c', '[,;]') FROM Users WHERE Id = 1"));
        Assert.Equal(["a", "b", "c"], split);

        Assert.Equal("123", ExecuteScalar("SELECT REGEXP_SUBSTR('abc123abc', '[0-9]+') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL DECODE uses byte decoding semantics instead of Oracle-style conditional decoding.
    /// PT: Garante que DECODE no PostgreSQL use semantica de decodificacao binaria em vez do decode condicional estilo Oracle.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void DecodeFunctions_ShouldFollowPostgreSqlSemantics()
    {
        var hexBytes = Assert.IsType<byte[]>(ExecuteScalar("SELECT DECODE('4142', 'hex') FROM Users WHERE Id = 1"));
        Assert.Equal(new byte[] { 0x41, 0x42 }, hexBytes);

        var base64Bytes = Assert.IsType<byte[]>(ExecuteScalar("SELECT DECODE('QUI=', 'base64') FROM Users WHERE Id = 1"));
        Assert.Equal(new byte[] { 0x41, 0x42 }, base64Bytes);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL LOG follows provider-specific semantics for one and two arguments.
    /// PT: Garante que LOG no PostgreSQL siga a semantica especifica do provedor para um e dois argumentos.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void LogFunctions_ShouldFollowPostgreSqlSemantics()
    {
        var log = Convert.ToDouble(ExecuteScalar("SELECT LOG(100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(2d, log);

        var logBase = Convert.ToDouble(ExecuteScalar("SELECT LOG(10, 100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(2d, logBase);

        var log10 = Convert.ToDouble(ExecuteScalar("SELECT LOG10(1000) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(3d, log10);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL arithmetic helpers return expected scalar values.
    /// PT: Garante que helpers aritmeticos do PostgreSQL retornem valores escalares esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ArithmeticFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(1m, Convert.ToDecimal(ExecuteScalar("SELECT MOD(10, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2m, Convert.ToDecimal(ExecuteScalar("SELECT ROUND(1.6) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1.23m, Convert.ToDecimal(ExecuteScalar("SELECT ROUND(1.234, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(36L, Convert.ToInt64(ExecuteScalar("SELECT LCM(12, 18) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT NUM_NULLS(1, NULL, 'x', NULL) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT NUM_NONNULLS(1, NULL, 'x', NULL) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL array helpers return expected values.
    /// PT: Garante que helpers de array do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ArrayFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("a;b", ExecuteScalar("SELECT ARRAY_TO_STRING(STRING_TO_ARRAY('a,b', ','), ';') FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT ARRAY_LENGTH(STRING_TO_ARRAY('a,b', ','), 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT ARRAY_LOWER(STRING_TO_ARRAY('a,b', ','), 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT ARRAY_UPPER(STRING_TO_ARRAY('a,b', ','), 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT ARRAY_POSITION(STRING_TO_ARRAY('a,b', ','), 'b') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("[1:2]", ExecuteScalar("SELECT ARRAY_DIMS(STRING_TO_ARRAY('a,b', ',')) FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT ARRAY_NDIMS(STRING_TO_ARRAY('a,b', ',')) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var appended = ExecuteScalar("SELECT ARRAY_APPEND(STRING_TO_ARRAY('a,b', ','), 'c') FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { "a", "b", "c" }, Assert.IsType<object?[]>(appended));

        var prepended = ExecuteScalar("SELECT ARRAY_PREPEND('z', STRING_TO_ARRAY('a,b', ',')) FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { "z", "a", "b" }, Assert.IsType<object?[]>(prepended));

        var concatenated = ExecuteScalar("SELECT ARRAY_CAT(STRING_TO_ARRAY('a,b', ','), STRING_TO_ARRAY('c,d', ',')) FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { "a", "b", "c", "d" }, Assert.IsType<object?[]>(concatenated));

        var removed = ExecuteScalar("SELECT ARRAY_REMOVE(STRING_TO_ARRAY('a,b,a', ','), 'a') FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { "b" }, Assert.IsType<object?[]>(removed));

        var replaced = ExecuteScalar("SELECT ARRAY_REPLACE(STRING_TO_ARRAY('a,b,a', ','), 'a', 'x') FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { "x", "b", "x" }, Assert.IsType<object?[]>(replaced));

        var positions = ExecuteScalar("SELECT ARRAY_POSITIONS(STRING_TO_ARRAY('a,b,a', ','), 'a') FROM Users WHERE Id = 1");
        Assert.Equal(new object?[] { 1, 3 }, Assert.IsType<object?[]>(positions));

        Assert.Equal("[\"a\",\"b\"]", ExecuteScalar("SELECT ARRAY_TO_JSON(STRING_TO_ARRAY('a,b', ',')) FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL JSON helpers return expected values.
    /// PT: Garante que helpers JSON do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void JsonFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("1", ExecuteScalar("SELECT TO_JSON(1) FROM Users WHERE Id = 1"));
        Assert.Equal("\"Ana\"", ExecuteScalar("SELECT TO_JSONB('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("\"Ana\"", ExecuteScalar("SELECT ROW_TO_JSON('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("123", ExecuteScalar("SELECT JSON_SCALAR(123) FROM Users WHERE Id = 1"));
        Assert.Equal("\"Ana\"", ExecuteScalar("SELECT JSON_SERIALIZE('Ana') FROM Users WHERE Id = 1"));
        Assert.Throws<NotSupportedException>(() => ExecuteScalar("SELECT JSON_QUERY('{\"user\":{\"name\":\"Ana\"}}', '$.user') FROM Users WHERE Id = 1"));
        Assert.Throws<NotSupportedException>(() => ExecuteScalar("SELECT JSON_VALUE('{\"user\":{\"name\":\"Ana\"}}', '$.user.name') FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT JSONB_PATH_EXISTS('{\"user\":{\"name\":\"Ana\"}}', '$.user.name') FROM Users WHERE Id = 1"));
        Assert.Equal("[\"Ana\"]", ExecuteScalar("SELECT JSONB_PATH_QUERY_ARRAY('{\"user\":{\"name\":\"Ana\"}}', '$.user.name') FROM Users WHERE Id = 1"));
        Assert.Equal("[1,\"Ana\",null]", ExecuteScalar("SELECT JSON_ARRAY(1, 'Ana', NULL) FROM Users WHERE Id = 1"));
        Assert.Equal("{\"id\":1,\"name\":\"Ana\"}", ExecuteScalar("SELECT JSON_OBJECT('id', 1, 'name', 'Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("[1,\"Ana\",true]", ExecuteScalar("SELECT JSON_BUILD_ARRAY(1, 'Ana', true) FROM Users WHERE Id = 1"));
        Assert.Equal("[1,\"Ana\",true]", ExecuteScalar("SELECT JSONB_BUILD_ARRAY(1, 'Ana', true) FROM Users WHERE Id = 1"));
        Assert.Equal("{\"id\":1,\"name\":\"Ana\"}", ExecuteScalar("SELECT JSON_BUILD_OBJECT('id', 1, 'name', 'Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"id\":1,\"name\":\"Ana\"}", ExecuteScalar("SELECT JSONB_BUILD_OBJECT('id', 1, 'name', 'Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"id\":\"1\",\"name\":\"Ana\"}", ExecuteScalar("SELECT JSONB_OBJECT(STRING_TO_ARRAY('id,name', ','), STRING_TO_ARRAY('1,Ana', ',')) FROM Users WHERE Id = 1"));

        Assert.Equal("object", ExecuteScalar("SELECT JSON_TYPEOF('{\"a\":1}') FROM Users WHERE Id = 1"));
        Assert.Equal("array", ExecuteScalar("SELECT JSONB_TYPEOF('[1,2]') FROM Users WHERE Id = 1"));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar("SELECT JSON_ARRAY_LENGTH('[1,2,3]') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT JSONB_ARRAY_LENGTH('[1,2]') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("1", ExecuteScalar("SELECT JSON_EXTRACT_PATH('{\"a\":{\"b\":[1,2,3]}}', 'a', 'b', '0') FROM Users WHERE Id = 1"));
        Assert.Equal("2", ExecuteScalar("SELECT JSONB_EXTRACT_PATH('{\"a\":{\"b\":[1,2,3]}}', 'a', 'b', '1') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT JSON_EXTRACT_PATH_TEXT('{\"user\":{\"name\":\"Ana\"}}', 'user', 'name') FROM Users WHERE Id = 1"));
        Assert.Equal("Bob", ExecuteScalar("SELECT JSONB_EXTRACT_PATH_TEXT('{\"user\":{\"name\":\"Bob\"}}', 'user', 'name') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"user\":{\"name\":\"Ana\"}}", ExecuteScalar("SELECT JSON_STRIP_NULLS('{\"user\":{\"name\":\"Ana\",\"nickname\":null},\"status\":null}') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"user\":{\"name\":\"Bob\"}}", ExecuteScalar("SELECT JSONB_STRIP_NULLS('{\"user\":{\"name\":\"Bob\",\"nickname\":null},\"status\":null}') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"user\":{\"name\":\"Bob\"}}", ExecuteScalar("SELECT JSONB_SET('{\"user\":{\"name\":\"Ana\"}}', STRING_TO_ARRAY('user,name', ','), 'Bob') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"user\":{}}", ExecuteScalar("SELECT JSONB_SET_LAX('{\"user\":{\"name\":\"Ana\"}}', STRING_TO_ARRAY('user,name', ','), NULL, true, 'delete_key') FROM Users WHERE Id = 1"));
        Assert.Equal("{\"tags\":[\"a\",\"b\",\"c\"]}", ExecuteScalar("SELECT JSONB_INSERT('{\"tags\":[\"a\",\"c\"]}', STRING_TO_ARRAY('tags,1', ','), 'b') FROM Users WHERE Id = 1"));

        Assert.Equal("[\"Ana\",\"Bob\"]", ExecuteScalar("SELECT JSON_AGG(Name) FROM Users"));
        Assert.Equal("[\"Ana\",\"Bob\"]", ExecuteScalar("SELECT JSONB_AGG(Name) FROM Users"));
        Assert.Equal("[\"Ana\",\"Bob\"]", ExecuteScalar("SELECT JSON_ARRAYAGG(Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSON_OBJECT_AGG(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSON_OBJECT_AGG_STRICT(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSON_OBJECT_AGG_UNIQUE(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSON_OBJECT_AGG_UNIQUE_STRICT(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSONB_OBJECT_AGG(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSONB_OBJECT_AGG_STRICT(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSONB_OBJECT_AGG_UNIQUE(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSONB_OBJECT_AGG_UNIQUE_STRICT(Id, Name) FROM Users"));
        Assert.Equal("{\"1\":\"Ana\",\"2\":\"Bob\"}", ExecuteScalar("SELECT JSON_OBJECTAGG(Id, Name) FROM Users"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL JSON pretty printers format JSONB values with indentation.
    /// PT: Garante que os formatadores JSON do PostgreSQL formatem valores JSONB com identacao.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void JsonPrettyFunctions_ShouldReturnExpectedValues()
    {
        var pretty = Assert.IsType<string>(ExecuteScalar("SELECT JSONB_PRETTY('{\"a\":1,\"b\":{\"c\":2}}') FROM Users WHERE Id = 1"));
        Assert.Contains("\n", pretty);
        Assert.Contains("\"a\": 1", pretty);
        Assert.Contains("\"c\": 2", pretty);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL UUID helpers return expected values.
    /// PT: Garante que helpers de UUID do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void UuidFunctions_ShouldReturnExpectedValues()
    {
        var value = ExecuteScalar("SELECT GEN_RANDOM_UUID() FROM Users WHERE Id = 1");
        var text = Assert.IsType<string>(value);
        Assert.True(Guid.TryParse(text, out _));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL aggregate functions return expected values.
    /// PT: Garante que funcoes agregadas do PostgreSQL retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void AggregateFunctions_ShouldReturnExpectedValues()
    {
        var arrayAgg = ExecuteScalar("SELECT ARRAY_AGG(Name) FROM Users");
        Assert.Equal(new object?[] { "Ana", "Bob" }, Assert.IsType<object?[]>(arrayAgg));

        Assert.Equal(2L, ExecuteScalar("SELECT COUNT(*) FROM Users"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT MIN(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MAX(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(3m, Convert.ToDecimal(ExecuteScalar("SELECT SUM(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1.5m, Convert.ToDecimal(ExecuteScalar("SELECT AVG(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal("Ana,Bob", ExecuteScalar("SELECT STRING_AGG(Name, ',') FROM Users"));
        Assert.Equal(true, ExecuteScalar("SELECT BOOL_AND(Id > 0) FROM Users"));
        Assert.Equal(true, ExecuteScalar("SELECT BOOL_OR(Name = 'Ana') FROM Users"));
        Assert.Equal(true, ExecuteScalar("SELECT EVERY(Id > 0) FROM Users"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL numeric conversion helpers return expected signs and parsed values.
    /// PT: Garante que helpers de conversao numerica do PostgreSQL retornem sinais e valores parseados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void NumericConversionFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(-1, Convert.ToInt32(ExecuteScalar("SELECT SIGN(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(123.45m, Convert.ToDecimal(ExecuteScalar("SELECT TO_NUMBER('123.45') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL window functions return expected values for ordered partitions.
    /// PT: Garante que funcoes de janela do PostgreSQL retornem valores esperados para particoes ordenadas.
    /// </summary>
    [Theory]
    [MemberDataNpgsqlVersion]
    [Trait("Category", "PostgreSqlMock")]
    public void WindowFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteColumn(connection, "SELECT ROW_NUMBER() OVER (ORDER BY Id) FROM Users ORDER BY Id"));
            return;
        }

        var rowNumbers = ExecuteColumn(connection, "SELECT ROW_NUMBER() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], rowNumbers);

        var ranks = ExecuteColumn(connection, "SELECT RANK() OVER (ORDER BY LENGTH(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], ranks);

        var denseRanks = ExecuteColumn(connection, "SELECT DENSE_RANK() OVER (ORDER BY LENGTH(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], denseRanks);

        var lags = ExecuteColumn(connection, "SELECT LAG(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([DBNull.Value, "Ana"], lags);

        var leads = ExecuteColumn(connection, "SELECT LEAD(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", DBNull.Value], leads);

        var firstValues = ExecuteColumn(connection, "SELECT FIRST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Ana", "Ana"], firstValues);

        var lastValues = ExecuteColumn(connection, "SELECT LAST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", "Bob"], lastValues);

        var nthValues = ExecuteColumn(connection, "SELECT NTH_VALUE(Name, 2) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", "Bob"], nthValues);

        var ntile = ExecuteColumn(connection, "SELECT NTILE(2) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], ntile);

        var percentRanks = ExecuteColumn(connection, "SELECT PERCENT_RANK() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0d, 1d], percentRanks);

        var cumeDist = ExecuteColumn(connection, "SELECT CUME_DIST() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0.5d, 1d], cumeDist);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL conditional and quantified expressions follow expected semantics.
    /// PT: Garante que expressoes condicionais e quantificadas do PostgreSQL sigam a semantica esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void ConditionalExpressions_ShouldReturnExpectedValues()
    {
        Assert.Equal("first", ExecuteScalar("SELECT CASE WHEN Id = 1 THEN 'first' ELSE 'other' END FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT Name ILIKE 'a%' FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT 2 = ANY (SELECT Id FROM Users) FROM Users WHERE Id = 1"));
        Assert.Equal(true, ExecuteScalar("SELECT 1 = SOME (SELECT Id FROM Users) FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL sequence functions keep session-scoped values in sync.
    /// PT: Garante que funcoes de sequence do PostgreSQL mantenham valores de sessao sincronizados.
    /// </summary>
    [Fact]
    [Trait("Category", "PostgreSqlMock")]
    public void SequenceFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT NEXTVAL('seq_users') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT CURRVAL('seq_users') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(5L, Convert.ToInt64(ExecuteScalar("SELECT SETVAL('seq_users', 5, true) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(5L, Convert.ToInt64(ExecuteScalar("SELECT LASTVAL() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(6L, Convert.ToInt64(ExecuteScalar("SELECT NEXTVAL('seq_users') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    private static NpgsqlConnectionMock CreateOpenConnection(int? version = null)
    {
        var db = new NpgsqlDbMock(version);
        db.AddSequence("seq_users");
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
        ]);

        var connection = new NpgsqlConnectionMock(db);
        connection.Open();

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')");
        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (2, 'Bob')");
        return connection;
    }

    private object? ExecuteScalar(string sql)
        => ExecuteScalar(_connection, sql);

    private static object? ExecuteScalar(NpgsqlConnectionMock connection, string sql)
    {
        using var command = new NpgsqlCommandMock(connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private List<object?> ExecuteColumn(string sql)
        => ExecuteColumn(_connection, sql);

    private static List<object?> ExecuteColumn(NpgsqlConnectionMock connection, string sql)
    {
        using var command = new NpgsqlCommandMock(connection)
        {
            CommandText = sql
        };
        using var reader = command.ExecuteReader();
        var values = new List<object?>();
        while (reader.Read())
            values.Add(reader.GetValue(0));

        return values;
    }

    private void ExecuteNonQuery(string sql)
        => ExecuteNonQuery(_connection, sql);

    private static void ExecuteNonQuery(NpgsqlConnectionMock connection, string sql)
    {
        using var command = new NpgsqlCommandMock(connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }
}
