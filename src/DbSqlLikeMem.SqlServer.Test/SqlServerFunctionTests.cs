namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Validates SQL Server function execution for provider-specific scalar and sequence features.
/// PT: Valida a execucao de funcoes SQL Server para recursos escalares e de sequence especificos do provedor.
/// </summary>
public sealed class SqlServerFunctionTests
    : XUnitTestBase
{
    private readonly SqlServerConnectionMock _connection;

    /// <summary>
    /// EN: Initializes SQL Server function fixtures with sample tables and sequences.
    /// PT: Inicializa os fixtures de funcoes SQL Server com tabelas e sequences de exemplo.
    /// </summary>
    public SqlServerFunctionTests(ITestOutputHelper helper)
        : base(helper)
    {
        var db = new SqlServerDbMock();
        db.AddSequence("seq_users");
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true),
        ]);

        _connection = new SqlServerConnectionMock(db);
        _connection.Open();

        ExecuteNonQuery("INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', '{\"profile\":{\"active\":true,\"name\":\"Ana\"}}')");
        ExecuteNonQuery("INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bob', '{\"profile\":{\"active\":false,\"name\":\"Bob\"}}')");
    }

    /// <summary>
    /// EN: Ensures SQL Server system functions return expected values.
    /// PT: Garante que funcoes de sistema do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void SystemFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("DbSqlLikeMem", ExecuteScalar("SELECT APP_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal("dbo", ExecuteScalar("SELECT CURRENT_USER FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT DB_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("DefaultSchema", ExecuteScalar("SELECT DB_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SCHEMA_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT SCHEMA_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal("2022", ExecuteScalar("SELECT SERVERPROPERTY('ProductVersion') FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SESSION_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT SESSION_USER FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SUSER_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("sa", ExecuteScalar("SELECT SUSER_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal("sa", ExecuteScalar("SELECT SUSER_SNAME() FROM Users WHERE Id = 1"));
        Assert.Equal("sa", ExecuteScalar("SELECT SYSTEM_USER FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT USER_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT USER_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT XACT_STATE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT CURRENT_TIMESTAMP FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT GETDATE() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT GETUTCDATE() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT SYSDATETIME() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTimeOffset>(ExecuteScalar("SELECT SYSDATETIMEOFFSET() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT SYSUTCDATETIME() FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Server transaction-state helpers reflect active transactions.
    /// PT: Garante que helpers de estado de transacao do SQL Server reflitam transacoes ativas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void TransactionStateFunctions_ShouldReturnExpectedValues()
    {
        using var transaction = _connection.BeginTransaction();
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT XACT_STATE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        transaction.Rollback();
    }

    /// <summary>
    /// EN: Ensures SQL Server scalar helpers return expected values.
    /// PT: Garante que helpers escalares do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void ScalarFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ACOS(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(Math.PI / 2d, Convert.ToDouble(ExecuteScalar("SELECT ASIN(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI / 4d, Convert.ToDouble(ExecuteScalar("SELECT ATAN(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT ATN2(0, 1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT COS(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.True(Convert.ToDouble(ExecuteScalar("SELECT COT(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture) > 0d);
        Assert.Equal(10, Convert.ToInt32(ExecuteScalar("SELECT ABS(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar("SELECT ASCII('A') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT CEILING(1.2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("A", ExecuteScalar("SELECT CHAR(65) FROM Users WHERE Id = 1"));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT CHARINDEX('bar', 'foobar') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("AnaMaria", ExecuteScalar("SELECT CONCAT('Ana', 'Maria') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana-Maria", ExecuteScalar("SELECT CONCAT_WS('-', 'Ana', NULL, 'Maria') FROM Users WHERE Id = 1"));
        Assert.Equal("fallback", ExecuteScalar("SELECT ISNULL(NULL, 'fallback') FROM Users WHERE Id = 1"));
        Assert.Equal("yes", ExecuteScalar("SELECT IIF(Id = 1, 'yes', 'no') FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT ISJSON('{\"a\":1}') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT ISNUMERIC('10.5') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("An", ExecuteScalar("SELECT LEFT('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal(3L, Convert.ToInt64(ExecuteScalar("SELECT LEN('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("ana", ExecuteScalar("SELECT LOWER('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT LTRIM('  Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("A", ExecuteScalar("SELECT NCHAR(65) FROM Users WHERE Id = 1"));
        Assert.Equal("dbo", ExecuteScalar("SELECT PARSENAME('server.database.dbo.Users', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("[Ana]", ExecuteScalar("SELECT QUOTENAME('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Bn", ExecuteScalar("SELECT REPLACE('Ban', 'a', '') FROM Users WHERE Id = 1"));
        Assert.Equal("NaNa", ExecuteScalar("SELECT REPLICATE('Na', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("anA", ExecuteScalar("SELECT REVERSE('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("na", ExecuteScalar("SELECT RIGHT('Ana', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT RTRIM('Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal(-1, Convert.ToInt32(ExecuteScalar("SELECT SIGN(-10) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("   ", ExecuteScalar("SELECT SPACE(3) FROM Users WHERE Id = 1"));
        Assert.Equal("Axxa", ExecuteScalar("SELECT STUFF('Ana', 2, 1, 'xx') FROM Users WHERE Id = 1"));
        Assert.Equal("An", ExecuteScalar("SELECT SUBSTRING('Ana', 1, 2) FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT TRIM('  Ana  ') FROM Users WHERE Id = 1"));
        Assert.Null(ExecuteScalar("SELECT TRY_CAST('abc' AS INT) FROM Users WHERE Id = 1"));
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar("SELECT TRY_CAST('42' AS INT) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Null(ExecuteScalar("SELECT TRY_CONVERT(INT, 'abc') FROM Users WHERE Id = 1"));
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar("SELECT TRY_CONVERT(INT, '42') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar("SELECT UNICODE('A') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("ANA", ExecuteScalar("SELECT UPPER('Ana') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Server date constructor and offset helpers return expected values.
    /// PT: Garante que helpers de construcao de data e offset do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void DateFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar("SELECT DATEFROMPARTS(2020, 2, 29) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12), Convert.ToDateTime(ExecuteScalar("SELECT DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), Convert.ToDateTime(ExecuteScalar("SELECT DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var offset = (DateTimeOffset)ExecuteScalar("SELECT DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 60) FROM Users WHERE Id = 1")!;
        Assert.Equal(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), TimeSpan.FromMinutes(60)), offset);
        Assert.Equal(new TimeSpan(10, 11, 12).Add(TimeSpan.FromTicks(1234567 * 10L)), Assert.IsType<TimeSpan>(ExecuteScalar("SELECT TIMEFROMPARTS(10, 11, 12, 1234567, 7) FROM Users WHERE Id = 1")));
        Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 0), Convert.ToDateTime(ExecuteScalar("SELECT SMALLDATETIMEFROMPARTS(2020, 2, 29, 10, 11) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar("SELECT EOMONTH('2020-02-15') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new DateTime(2020, 2, 16), Convert.ToDateTime(ExecuteScalar("SELECT DATEADD(day, 1, '2020-02-15') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT DATEDIFF(day, '2020-01-01', '2020-01-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar("SELECT DATEDIFF_BIG(day, '2020-01-01', '2020-01-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("February", ExecuteScalar("SELECT DATENAME(month, '2020-02-10') FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT DATEPART(month, '2020-02-10') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(14, Convert.ToInt32(ExecuteScalar("SELECT DAY('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MONTH('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2020, Convert.ToInt32(ExecuteScalar("SELECT YEAR('2020-02-14') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server metadata helpers return expected provider-compatible values.
    /// PT: Garante que helpers de metadados do SQL Server retornem valores compativeis com o provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void MetadataFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT GETANSINULL() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT DATALENGTH('AB') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT GROUPING(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT GROUPING_ID(1, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT HOST_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("localhost", ExecuteScalar("SELECT HOST_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT ISDATE('2020-01-01') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT ISDATE('invalid') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server JSON scalar helpers return expected values.
    /// PT: Garante que helpers escalares de JSON do SQL Server retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void JsonFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal("{\"active\":true,\"name\":\"Ana\"}", ExecuteScalar("SELECT JSON_QUERY(Email, '$.profile') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT JSON_VALUE(Email, '$.profile.name') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Server aggregate and window functions return expected ordered values.
    /// PT: Garante que funcoes de agregacao e janela do SQL Server retornem valores ordenados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void AggregateAndWindowFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT COUNT(*) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar("SELECT COUNT_BIG(*) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(3m, Convert.ToDecimal(ExecuteScalar("SELECT SUM(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT MIN(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT MAX(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal(1.5m, Convert.ToDecimal(ExecuteScalar("SELECT AVG(Id) FROM Users"), CultureInfo.InvariantCulture));
        Assert.Equal("Ana,Bob", ExecuteScalar("SELECT STRING_AGG(Name, ',') FROM Users"));
        Assert.Equal(Math.Sqrt(0.5d), Convert.ToDouble(ExecuteScalar("SELECT STDEV(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar("SELECT STDEVP(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.5d, Convert.ToDouble(ExecuteScalar("SELECT VAR(Id) FROM Users"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0.25d, Convert.ToDouble(ExecuteScalar("SELECT VARP(Id) FROM Users"), CultureInfo.InvariantCulture), 12);

        var lagValues = ExecuteColumn("SELECT LAG(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([null, "Ana"], lagValues);

        var leadValues = ExecuteColumn("SELECT LEAD(Name) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", null], leadValues);

        var firstValues = ExecuteColumn("SELECT FIRST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Ana", "Ana"], firstValues);

        var lastValues = ExecuteColumn("SELECT LAST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM Users ORDER BY Id");
        Assert.Equal(["Bob", "Bob"], lastValues);

        var denseRanks = ExecuteColumn("SELECT DENSE_RANK() OVER (ORDER BY LEN(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], denseRanks);

        var ranks = ExecuteColumn("SELECT RANK() OVER (ORDER BY LEN(Name)) FROM Users ORDER BY Id");
        Assert.Equal([1L, 1L], ranks);

        var rowNumbers = ExecuteColumn("SELECT ROW_NUMBER() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], rowNumbers);

        var percentRanks = ExecuteColumn("SELECT PERCENT_RANK() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0d, 1d], percentRanks);

        var cumeDist = ExecuteColumn("SELECT CUME_DIST() OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([0.5d, 1d], cumeDist);

        var ntile = ExecuteColumn("SELECT NTILE(2) OVER (ORDER BY Id) FROM Users ORDER BY Id");
        Assert.Equal([1L, 2L], ntile);
    }

    /// <summary>
    /// EN: Ensures SQL Server math and error helpers return expected scalar values.
    /// PT: Garante que helpers matematicos e de erro do SQL Server retornem valores escalares esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void MathAndErrorFunctions_ShouldReturnExpectedValues()
    {
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar("SELECT PI() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(180d, Convert.ToDouble(ExecuteScalar("SELECT DEGREES(PI()) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var exp = Convert.ToDouble(ExecuteScalar("SELECT EXP(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.True(exp > 2.7d && exp < 2.8d);

        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT FLOOR(1.9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar("SELECT LOG(10, 100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar("SELECT LOG10(100) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(8d, Convert.ToDouble(ExecuteScalar("SELECT POWER(2, 3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar("SELECT RADIANS(180) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        var seededRandFirst = Convert.ToDouble(ExecuteScalar("SELECT RAND(7) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        var seededRandSecond = Convert.ToDouble(ExecuteScalar("SELECT RAND(7) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture);
        Assert.Equal(seededRandFirst, seededRandSecond);
        Assert.Equal(1.24m, Convert.ToDecimal(ExecuteScalar("SELECT ROUND(1.235, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar("SELECT SIN(1.5707963267948966) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal("R163", ExecuteScalar("SELECT SOUNDEX('Robert') FROM Users WHERE Id = 1"));
        Assert.Equal(3d, Convert.ToDouble(ExecuteScalar("SELECT SQRT(9) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(9d, Convert.ToDouble(ExecuteScalar("SELECT SQUARE(3) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar("SELECT TAN(0) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT DIFFERENCE('Robert', 'Rupert') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT ERROR_LINE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(string.Empty, ExecuteScalar("SELECT ERROR_MESSAGE() FROM Users WHERE Id = 1"));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT ERROR_NUMBER() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(string.Empty, ExecuteScalar("SELECT ERROR_PROCEDURE() FROM Users WHERE Id = 1"));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT ERROR_SEVERITY() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT ERROR_STATE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server sequence expressions advance values in execution order.
    /// PT: Garante que expressoes de sequence do SQL Server avancem valores na ordem de execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void SequenceExpressions_ShouldReturnExpectedValues()
    {
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT NEXT VALUE FOR seq_users FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar("SELECT NEXT VALUE FOR seq_users FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server STRING_SPLIT materializes rows for APPLY-based table function usage.
    /// PT: Garante que STRING_SPLIT do SQL Server materialize linhas para uso de table function com APPLY.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void StringSplitFunction_ShouldReturnExpectedRows()
    {
        ExecuteNonQuery("INSERT INTO Users (Id, Name, Email) VALUES (3, 'Csv', 'red,blue')");

        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = """
                SELECT part.value
                FROM Users u
                CROSS APPLY STRING_SPLIT(u.Email, ',') part
                WHERE u.Id = 3
                """
        };

        using var reader = command.ExecuteReader();
        var values = new List<string>();
        while (reader.Read())
            values.Add(Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture) ?? string.Empty);

        Assert.Equal(["red", "blue"], values);
    }

    /// <summary>
    /// EN: Ensures SQL Server @@ROWCOUNT exposes the affected-row count from the previous statement.
    /// PT: Garante que @@ROWCOUNT do SQL Server exponha a contagem de linhas afetadas pela instrução anterior.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void RowCountIdentifier_ShouldReturnAffectedRows()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Ana Updated' WHERE Id = 1; SELECT @@ROWCOUNT;"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Server ROWCOUNT_BIG() exposes the last row-count value as bigint.
    /// PT: Garante que ROWCOUNT_BIG() do SQL Server exponha o ultimo valor de contagem de linhas como bigint.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerMock")]
    public void RowCountBigFunction_ShouldReturnAffectedRows()
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Bob Updated' WHERE Id = 2; SELECT ROWCOUNT_BIG();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    private object? ExecuteScalar(string sql)
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private List<object?> ExecuteColumn(string sql)
    {
        using var command = new SqlServerCommandMock(_connection)
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
    {
        using var command = new SqlServerCommandMock(_connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }
}
