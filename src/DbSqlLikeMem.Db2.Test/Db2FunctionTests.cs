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
