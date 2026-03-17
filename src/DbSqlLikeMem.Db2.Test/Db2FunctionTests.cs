namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// EN: Validates DB2 function execution for provider-specific scalar and sequence features.
/// PT: Valida a execucao de funcoes DB2 para recursos escalares e de sequence especificos do provedor.
/// </summary>
public sealed class Db2FunctionTests
    : XUnitTestBase
{
    private readonly Db2ConnectionMock _connection;

    /// <summary>
    /// EN: Initializes DB2 function fixtures with sample tables and sequences.
    /// PT: Inicializa os fixtures de funcoes DB2 com tabelas e sequences de exemplo.
    /// </summary>
    public Db2FunctionTests(ITestOutputHelper helper)
        : base(helper)
    {
        _connection = CreateOpenConnection();
    }

    /// <summary>
    /// EN: Ensures DB2 executes the pragmatic scalar FUNCTION DDL subset end to end.
    /// PT: Garante que o DB2 execute end-to-end o subset pragmatico de DDL de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void ScalarFunctionDdlSubset_ShouldExecuteEndToEnd(int version)
    {
        using var connection = CreateOpenConnection(version);

        ExecuteNonQuery(connection, "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT RETURN baseValue + incrementValue");

        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        ExecuteNonQuery(connection, "DROP FUNCTION IF EXISTS fn_users(INT, INT)");

        Assert.Null(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures DB2 temporal identifiers return provider-compatible values.
    /// PT: Garante que identificadores temporais do DB2 retornem valores compativeis com o provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Mock")]
    public void TemporalFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(new DateTime(DateTime.UtcNow.Year, DateTime.UtcNow.Month, DateTime.UtcNow.Day), ((DateTime)ExecuteScalar("SELECT CURRENT DATE FROM Users WHERE Id = 1")!).Date);
        Assert.IsType<TimeSpan>(ExecuteScalar("SELECT CURRENT TIME FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT CURRENT TIMESTAMP FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures DB2 scalar helpers return expected values.
    /// PT: Garante que helpers escalares do DB2 retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Mock")]
    public void ScalarFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("fallback", ExecuteScalar("SELECT VALUE(NULL, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal("fallback", ExecuteScalar("SELECT IFNULL(NULL, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal("yes", ExecuteScalar("SELECT IIF(Id = 1, 'yes', 'no') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT COALESCE(Name, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal("fallback", ExecuteScalar("SELECT NVL(NULL, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal(DBNull.Value, ExecuteScalar("SELECT NULLIF('Ana', 'Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("65", ExecuteScalar("SELECT CHAR(65) FROM Users WHERE Id = 1"));
        Assert.Equal(42L, Convert.ToInt64(ExecuteScalar("SELECT BIGINT('42') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(12.5m, Convert.ToDecimal(ExecuteScalar("SELECT DECIMAL('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar("SELECT DOUBLE('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar("SELECT FLOAT('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar("SELECT INT('42') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar("SELECT INTEGER('42') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar("SELECT REAL('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(7, Convert.ToInt32(ExecuteScalar("SELECT SMALLINT('7') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Ana", ExecuteScalar("SELECT VARCHAR('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal(new DateTime(2020, 2, 14), Convert.ToDateTime(ExecuteScalar("SELECT DATE('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new TimeSpan(10, 11, 12), (TimeSpan)ExecuteScalar("SELECT TIME('2020-02-14 10:11:12') FROM Users WHERE Id = 1")!);
        Assert.Equal(new DateTime(2020, 2, 14, 10, 11, 12), Convert.ToDateTime(ExecuteScalar("SELECT TIMESTAMP('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar("SELECT LAST_DAY('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Friday", ExecuteScalar("SELECT DAYNAME('2020-02-14') FROM Users WHERE Id = 1"));
        Assert.Equal(14, Convert.ToInt32(ExecuteScalar("SELECT DAYOFMONTH('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(6, Convert.ToInt32(ExecuteScalar("SELECT DAYOFWEEK('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(5, Convert.ToInt32(ExecuteScalar("SELECT DAYOFWEEK_ISO('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(45, Convert.ToInt32(ExecuteScalar("SELECT DAYOFYEAR('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2020, Convert.ToInt32(ExecuteScalar("SELECT EXTRACT(YEAR FROM '2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("ana", ExecuteScalar("SELECT LOWER('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT LOCATE('n', 'Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("..Ana", ExecuteScalar("SELECT LPAD('Ana', 5, '.') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT LTRIM('  Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("February", ExecuteScalar("SELECT MONTHNAME('2020-02-14') FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT QUARTER('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("NaNa", ExecuteScalar("SELECT REPEAT('Na', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("Ana..", ExecuteScalar("SELECT RPAD('Ana', 5, '.') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT RTRIM('Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal("   ", ExecuteScalar("SELECT SPACE(3) FROM Users WHERE Id = 1"));
        Assert.Equal("ANA", ExecuteScalar("SELECT UCASE('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("ANA", ExecuteScalar("SELECT UPPER('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Bn", ExecuteScalar("SELECT REPLACE('Ban', 'a', '') FROM Users WHERE Id = 1"));
        Assert.Equal("na", ExecuteScalar("SELECT RIGHT('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("na", ExecuteScalar("SELECT SUBSTR('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("An", ExecuteScalar("SELECT SUBSTRING('Ana', 1, 2) FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT TRIM('  Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal(-1, Convert.ToInt32(ExecuteScalar("SELECT SIGN(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(14, Convert.ToInt32(ExecuteScalar("SELECT DAY('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MONTH('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2020, Convert.ToInt32(ExecuteScalar("SELECT YEAR('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(10, Convert.ToInt32(ExecuteScalar("SELECT HOUR('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(11, Convert.ToInt32(ExecuteScalar("SELECT MINUTE('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(12, Convert.ToInt32(ExecuteScalar("SELECT SECOND('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(737469, Convert.ToInt32(ExecuteScalar("SELECT DAYS('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 16), Convert.ToDateTime(ExecuteScalar("SELECT TIMESTAMPADD(DAY, 2, '2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(10m, Convert.ToDecimal(ExecuteScalar("SELECT ABS(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ACOS(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI / 2d, Convert.ToDouble(ExecuteScalar("SELECT ASIN(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI / 4d, Convert.ToDouble(ExecuteScalar("SELECT ATAN(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ATAN2(0, 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT COS(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT COT(0.7853981633974483) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(180d, Convert.ToDouble(ExecuteScalar("SELECT DEGREES(3.141592653589793) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.E, Convert.ToDouble(ExecuteScalar("SELECT EXP(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT FLOOR(1.9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT LN(2.718281828459045) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar("SELECT LOG10(100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1m, Convert.ToDecimal(ExecuteScalar("SELECT MOD(10, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(8d, Convert.ToDouble(ExecuteScalar("SELECT POWER(2, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar("SELECT RADIANS(180) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1.24m, Convert.ToDecimal(ExecuteScalar("SELECT ROUND(1.235, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT SIN(1.5707963267948966) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(3d, Convert.ToDouble(ExecuteScalar("SELECT SQRT(9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT TAN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1m, Convert.ToDecimal(ExecuteScalar("SELECT TRUNC(1.9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1.98m, Convert.ToDecimal(ExecuteScalar("SELECT TRUNCATE(1.987, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(7, Convert.ToInt32(ExecuteScalar("SELECT WEEK('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(7, Convert.ToInt32(ExecuteScalar("SELECT WEEK_ISO('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 aggregate functions return expected values for the sample data set.
    /// PT: Garante que funcoes de agregacao do DB2 retornem valores esperados para o conjunto de dados de exemplo.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Mock")]
    public void AggregateFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT COUNT(*) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar("SELECT COUNT_BIG(*) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(3m, Convert.ToDecimal(ExecuteScalar("SELECT SUM(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1.5m, Convert.ToDecimal(ExecuteScalar("SELECT AVG(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT MIN(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MAX(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal("Bob|Ana", ExecuteScalar("SELECT LISTAGG(Name, '|') WITHIN GROUP (ORDER BY Id DESC) FROM Users"));
    }

    /// <summary>
    /// EN: Ensures DB2 window functions return expected values for ordered rows.
    /// PT: Garante que funcoes de janela do DB2 retornem valores esperados para linhas ordenadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Mock")]
    public void WindowFunctions_ShouldReturnExpectedValues()
    {
        var rowNumbers = ExecuteColumn("SELECT ROW_NUMBER() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], rowNumbers);

        var ranks = ExecuteColumn("SELECT RANK() OVER (ORDER BY LENGTH(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], ranks);

        var denseRanks = ExecuteColumn("SELECT DENSE_RANK() OVER (ORDER BY LENGTH(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], denseRanks);

        var lagValues = ExecuteColumn("SELECT LAG(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([DBNull.Value, "Ana"], lagValues);

        var leadValues = ExecuteColumn("SELECT LEAD(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", DBNull.Value], leadValues);

        var firstValues = ExecuteColumn("SELECT FIRST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Ana", "Ana"], firstValues);

        var lastValues = ExecuteColumn("SELECT LAST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", "Bob"], lastValues);

        var ntileValues = ExecuteColumn("SELECT NTILE(2) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], ntileValues);

        var percentRanks = ExecuteColumn("SELECT PERCENT_RANK() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0d, 1d], percentRanks);

        var cumeDist = ExecuteColumn("SELECT CUME_DIST() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0.5d, 1d], cumeDist);
    }

    /// <summary>
    /// EN: Ensures DB2 JSON scalar extraction helpers return expected values when supported by the dialect version.
    /// PT: Garante que helpers escalares de extracao JSON do DB2 retornem valores esperados quando suportados pela versao do dialeto.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void JsonFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        if (version < Db2Dialect.JsonFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT JSON_QUERY(Email, '$.profile') FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT JSON_VALUE(Email, '$.profile.name') FROM Users WHERE Id = 1"));
            return;
        }

        Assert.Equal("{\"active\":true,\"name\":\"Ana\"}", ExecuteScalar(connection, "SELECT JSON_QUERY(Email, '$.profile') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT JSON_VALUE(Email, '$.profile.name') FROM Users WHERE Id = 1"));
    }


    /// <summary>
    /// EN: Ensures DB2 date-add alias functions return expected values across versions.
    /// PT: Garante que as funcoes de alias de data do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void DateAddAliasFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(new DateTime(2020, 2, 16), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_DAYS('2020-02-14', 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 14, 15, 11, 12), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_HOURS('2020-02-14 10:11:12', 5) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 14, 10, 41, 12), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_MINUTES('2020-02-14 10:11:12', 30) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 14, 10, 11, 42), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_SECONDS('2020-02-14 10:11:12', 30) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 3, 14), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_MONTHS('2020-02-14', 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2021, 2, 14), Convert.ToDateTime(ExecuteScalar(connection, "SELECT ADD_YEARS('2020-02-14', 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 ABSVAL, CHR, and CURDATE return expected values across versions.
    /// PT: Garante que ABSVAL, CHR e CURDATE do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void AbsValChrAndCurDateFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(10m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT ABSVAL(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("A", ExecuteScalar(connection, "SELECT CHR(65) FROM Users WHERE Id = 1"));

        var curDate = (DateTime)ExecuteScalar(connection, "SELECT CURDATE FROM Users WHERE Id = 1")!;
        Assert.Equal(DateTime.UtcNow.Date, curDate.Date);
    }

    /// <summary>
    /// EN: Ensures DB2 bitwise helpers return expected values across versions.
    /// PT: Garante que helpers bitwise do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void BitwiseFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar(connection, "SELECT BITAND(6, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(7L, Convert.ToInt64(ExecuteScalar(connection, "SELECT BITOR(6, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(5L, Convert.ToInt64(ExecuteScalar(connection, "SELECT BITXOR(6, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(-7L, Convert.ToInt64(ExecuteScalar(connection, "SELECT BITNOT(6) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4L, Convert.ToInt64(ExecuteScalar(connection, "SELECT BITANDNOT(6, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 hyperbolic helpers return expected values across versions.
    /// PT: Garante que helpers hiperbolicos do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void HyperbolicFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT COSH(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT SINH(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT TANH(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
    }

    /// <summary>
    /// EN: Ensures DB2 DECODE and DEC functions return expected values across versions.
    /// PT: Garante que DECODE e DEC do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void DecodeAndDecFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal("match", ExecuteScalar(connection, "SELECT DECODE('A', 'A', 'match', 'miss') FROM Users WHERE Id = 1"));
        Assert.Equal("miss", ExecuteScalar(connection, "SELECT DECODE('B', 'A', 'match', 'miss') FROM Users WHERE Id = 1"));
        Assert.Equal(12.5m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT DEC('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 empty LOB helpers return provider-compatible values across versions.
    /// PT: Garante que helpers de LOB vazios do DB2 retornem valores compativeis com o provedor em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void EmptyLobFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        var emptyBlob = (byte[]?)ExecuteScalar(connection, "SELECT EMPTY_BLOB() FROM Users WHERE Id = 1");
        Assert.NotNull(emptyBlob);
        Assert.Empty(emptyBlob!);

        Assert.Equal(string.Empty, ExecuteScalar(connection, "SELECT EMPTY_CLOB() FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures DB2 LOB, cast alias, and end-of-month helpers return expected values across versions.
    /// PT: Garante que helpers de LOB, alias de cast e fim de mes do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void LobCastAliasAndEomonthFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(string.Empty, ExecuteScalar(connection, "SELECT EMPTY_DBCLOB() FROM Users WHERE Id = 1"));
        Assert.Equal(string.Empty, ExecuteScalar(connection, "SELECT EMPTY_NCLOB() FROM Users WHERE Id = 1"));

        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT BPCHAR('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT DBCLOB('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT GRAPHIC('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT VARGRAPHIC('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT DOUBLE_PRECISION('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT FLOAT4('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(12.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT FLOAT8('12.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);

        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar(connection, "SELECT EOMONTH('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 3, 31), Convert.ToDateTime(ExecuteScalar(connection, "SELECT EOMONTH('2020-02-14', 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 monthly, next-day, NVL2, and RAND helpers return expected values across versions.
    /// PT: Garante que helpers mensais, next-day, NVL2 e RAND do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void MonthsBetweenNextDayNvl2AndRandFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(1m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT MONTHS_BETWEEN('2020-03-14', '2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 17), Convert.ToDateTime(ExecuteScalar(connection, "SELECT NEXT_DAY('2020-02-14', 'monday') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("no", ExecuteScalar(connection, "SELECT NVL2(NULL, 'yes', 'no') FROM Users WHERE Id = 1"));
        Assert.Equal("yes", ExecuteScalar(connection, "SELECT NVL2('Ana', 'yes', 'no') FROM Users WHERE Id = 1"));

        var randValue = Convert.ToDouble(ExecuteScalar(connection, "SELECT RAND() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.InRange(randValue, 0d, 1d);
        var randSeeded = Convert.ToDouble(ExecuteScalar(connection, "SELECT RAND(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.InRange(randSeeded, 0d, 1d);
    }

    /// <summary>
    /// EN: Ensures DB2 conversion, log, and timestamp diff helpers return expected values across versions.
    /// PT: Garante que helpers de conversao, log e diferenca de timestamps do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void ConversionLogAndTimestampDiffFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT LOG(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT TIMESTAMPDIFF(DAY, '2020-02-14', '2020-02-16') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        Assert.Equal(new DateTime(2020, 2, 14), Convert.ToDateTime(ExecuteScalar(connection, "SELECT TO_DATE('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 14, 10, 11, 12), Convert.ToDateTime(ExecuteScalar(connection, "SELECT TO_TIMESTAMP('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT TO_CLOB('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT TO_NCHAR('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar(connection, "SELECT TO_NCLOB('Ana') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures DB2 CARDINALITY, TRANSLATE, GROUPING, and RATIO_TO_REPORT helpers return expected values across versions.
    /// PT: Garante que CARDINALITY, TRANSLATE, GROUPING e RATIO_TO_REPORT do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void CardinalityTranslateGroupingAndRatioFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        connection.Db.AddTable("TagSamples",
        [
            new("Id", DbType.Int32, false),
            new("Tags", DbType.Object, true),
        ],
        [
            new Dictionary<int, object?> { [0] = 1, [1] = new[] { "a", "b", "c" } }
        ]);

        Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT CARDINALITY(Tags) FROM TagSamples WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("xyc", ExecuteScalar(connection, "SELECT TRANSLATE('abc', 'ab', 'xy') FROM Users WHERE Id = 1"));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT GROUPING(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        var ratio = ExecuteScalar(connection, "SELECT RATIO_TO_REPORT(1) FROM Users WHERE Id = 1");
        Assert.True(ratio is null || ratio is DBNull || (ratio is string text && string.IsNullOrWhiteSpace(text)));
    }

    /// <summary>
    /// EN: Ensures DB2 DIV performs integer-style division across versions.
    /// PT: Garante que DIV do DB2 executa divisao no estilo inteiro em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void DivFunction_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(3m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT DIV(10, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        var divByZero = ExecuteScalar(connection, "SELECT DIV(10, 0) FROM Users WHERE Id = 1");
        Assert.True(divByZero is null || divByZero is DBNull || (divByZero is string text && string.IsNullOrWhiteSpace(text)));
    }

    /// <summary>
    /// EN: Ensures DB2 MIDNIGHT_SECONDS returns expected seconds since midnight across versions.
    /// PT: Garante que MIDNIGHT_SECONDS do DB2 retorna os segundos desde meia-noite em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void MidnightSecondsFunction_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(36672, Convert.ToInt32(ExecuteScalar(connection, "SELECT MIDNIGHT_SECONDS('2020-02-14 10:11:12') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 DATE_TRUNC truncates dates to the requested unit across versions.
    /// PT: Garante que DATE_TRUNC do DB2 trunca datas para a unidade solicitada em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void DateTruncFunction_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(new DateTime(2020, 2, 1), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATE_TRUNC('month', '2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 1, 1), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATE_TRUNC('year', '2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures DB2 statistical aggregates return expected values across versions.
    /// PT: Garante que agregados estatisticos do DB2 retornem valores esperados em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void StatisticalAggregateFunctions_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal(1.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT MEDIAN(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT PERCENTILE_CONT(Id, 0.5) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT PERCENTILE_DISC(Id, 0.5) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT STDDEV(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        var stddevSamp = ExecuteScalar(connection, "SELECT STDDEV_SAMP(Id) FROM Users");
        if (stddevSamp is null || stddevSamp is DBNull || (stddevSamp is string stddevSampText && string.IsNullOrWhiteSpace(stddevSampText)))
        {
            Assert.Null(stddevSamp);
        }
        else
        {
            Assert.Equal(0.7071067811865476d, Convert.ToDouble(stddevSamp, CultureInfo.InvariantCulture), 12);
        }
        Assert.Equal(0.25d, Convert.ToDouble(ExecuteScalar(connection, "SELECT VARIANCE(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT VARIANCE_SAMP(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.25d, Convert.ToDouble(ExecuteScalar(connection, "SELECT COVARIANCE(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT COVARIANCE_SAMP(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT CORRELATION(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT REGR_COUNT(Id, Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_AVGX(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_AVGY(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_SXX(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_SYY(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_SXY(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_SLOPE(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_INTERCEPT(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_ICPT(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT REGR_R2(Id, Id) FROM Users"), CultureInfo.InvariantCulture), 12);
    }

    /// <summary>
    /// EN: Ensures DB2 SESSION_USER returns a provider-compatible identifier across versions.
    /// PT: Garante que SESSION_USER do DB2 retorna um identificador compativel com o provedor em todas as versoes.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versao do dialeto DB2 em teste.</param>
    [Theory]
    [MemberDataDb2Version]
    [Trait("Category", "Db2Mock")]
    public void SessionUserFunction_ShouldReturnExpectedValues(int version)
    {
        using var connection = CreateOpenConnection(version);

        Assert.Equal("dbo", ExecuteScalar(connection, "SELECT SESSION_USER() FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures DB2 sequence expressions expose both next and previous values in the session.
    /// PT: Garante que expressoes de sequence do DB2 exponham valores next e previous na sessao.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Mock")]
    public void SequenceExpressions_ShouldReturnExpectedValues()
    {
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT NEXT VALUE FOR seq_users FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT PREVIOUS VALUE FOR seq_users FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar("SELECT NEXT VALUE FOR seq_users FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    private static Db2ConnectionMock CreateOpenConnection(int? version = null)
    {
        var db = new Db2DbMock(version);
        db.AddSequence("seq_users");
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true),
        ]);

        var connection = new Db2ConnectionMock(db);
        connection.Open();

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', '{\"profile\":{\"active\":true,\"name\":\"Ana\"}}')");
        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bob', '{\"profile\":{\"active\":false,\"name\":\"Bob\"}}')");
        return connection;
    }

    private object? ExecuteScalar(string sql)
        => ExecuteScalar(_connection, sql);

    private static object? ExecuteScalar(Db2ConnectionMock connection, string sql)
    {
        using var command = new Db2CommandMock(connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private List<object?> ExecuteColumn(string sql)
    {
        using var command = new Db2CommandMock(_connection)
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

    private static void ExecuteNonQuery(Db2ConnectionMock connection, string sql)
    {
        using var command = new Db2CommandMock(connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }
}


