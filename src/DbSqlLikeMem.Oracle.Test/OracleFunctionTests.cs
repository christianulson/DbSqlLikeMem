namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Validates Oracle function execution for common scalar and aggregate utilities.
/// PT: Valida a execucao de funcoes Oracle para utilitarios escalares e agregados comuns.
/// </summary>
public sealed class OracleFunctionTests
    : XUnitTestBase
{
    private readonly OracleConnectionMock _connection;

    /// <summary>
    /// EN: Initializes Oracle function test fixtures with sample tables.
    /// PT: Inicializa os fixtures de funcoes Oracle com tabelas de exemplo.
    /// </summary>
    public OracleFunctionTests(ITestOutputHelper helper)
        : base(helper)
    {
        var db = new OracleDbMock();
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
        ]);
        db.AddTable("Numbers",
        [
            new("X", DbType.Int32, false),
            new("Y", DbType.Int32, false),
        ]);

        _connection = new OracleConnectionMock(db);
        _connection.Open();

        ExecuteNonQuery("INSERT INTO Users (Id, Name) VALUES (1, 'Ana')");
        ExecuteNonQuery("INSERT INTO Users (Id, Name) VALUES (2, 'Bob')");

        ExecuteNonQuery("INSERT INTO Numbers (X, Y) VALUES (1, 2)");
        ExecuteNonQuery("INSERT INTO Numbers (X, Y) VALUES (2, 4)");
        ExecuteNonQuery("INSERT INTO Numbers (X, Y) VALUES (3, 6)");
    }

    /// <summary>
    /// EN: Ensures Oracle executes the pragmatic scalar FUNCTION DDL subset end to end.
    /// PT: Garante que o Oracle execute end-to-end o subset pragmatico de DDL de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versao do dialeto Oracle em teste.</param>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleMock")]
    public void ScalarFunctionDdlSubset_ShouldExecuteEndToEnd(int version)
    {
        using var connection = CreateOpenConnection(version);

        ExecuteNonQuery(connection, "CREATE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue; END");

        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        ExecuteNonQuery(connection, "DROP FUNCTION fn_users");

        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures Oracle replaces an existing scalar function body through CREATE OR REPLACE FUNCTION.
    /// PT: Garante que o Oracle substitua o corpo de uma funcao escalar existente com CREATE OR REPLACE FUNCTION.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versao do dialeto Oracle em teste.</param>
    [Theory]
    [MemberDataOracleVersion]
    [Trait("Category", "OracleMock")]
    public void CreateOrReplaceScalarFunctionDdlSubset_ShouldReplaceExistingBody(int version)
    {
        using var connection = CreateOpenConnection(version);
        ExecuteNonQuery(connection, "CREATE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue; END");
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        ExecuteNonQuery(connection, "CREATE OR REPLACE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue + 1; END");
        Assert.Equal(43, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures core Oracle conversion functions return expected results.
    /// PT: Garante que funcoes de conversao Oracle retornem resultados esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void ConversionFunctions_ShouldReturnExpectedValues()
    {
        var toDate = ExecuteScalar("SELECT TO_DATE('2024-01-02','YYYY-MM-DD') FROM Users WHERE Id = 1");
        Assert.IsType<DateTime>(toDate);
        Assert.Equal(new DateTime(2024, 1, 2), ((DateTime)toDate!).Date);

        var toTimestamp = ExecuteScalar("SELECT TO_TIMESTAMP('2024-01-02 03:04:05','YYYY-MM-DD HH24:MI:SS') FROM Users WHERE Id = 1");
        Assert.IsType<DateTime>(toTimestamp);
        Assert.Equal(new DateTime(2024, 1, 2, 3, 4, 5), (DateTime)toTimestamp!);

        var toTimestampTz = ExecuteScalar("SELECT TO_TIMESTAMP_TZ('2024-01-02 03:04:05 +02:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') FROM Users WHERE Id = 1");
        Assert.IsType<DateTimeOffset>(toTimestampTz);
        Assert.Equal(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)), (DateTimeOffset)toTimestampTz!);

        var toCharDate = ExecuteScalar("SELECT TO_CHAR(TO_DATE('2024-01-02','YYYY-MM-DD'),'YYYY-MM-DD') FROM Users WHERE Id = 1");
        Assert.Equal("2024-01-02", toCharDate);

        var toCharNumber = ExecuteScalar("SELECT TO_CHAR(1234.5, '9999D00') FROM Users WHERE Id = 1");
        Assert.Equal("1234.50", toCharNumber);

        var toCharNumberFm = ExecuteScalar("SELECT TO_CHAR(1234.5, 'FM9999D00') FROM Users WHERE Id = 1");
        Assert.Equal("1234.50", toCharNumberFm);

        var toNumber = ExecuteScalar("SELECT TO_NUMBER('1,234.50', '9G999D99') FROM Users WHERE Id = 1");
        Assert.Equal(1234.50m, Convert.ToDecimal(toNumber, CultureInfo.InvariantCulture));

        var toNumberNegative = ExecuteScalar("SELECT TO_NUMBER('(1,234.50)', '9G999D99') FROM Users WHERE Id = 1");
        Assert.Equal(-1234.50m, Convert.ToDecimal(toNumberNegative, CultureInfo.InvariantCulture));

        var toDateWithMon = ExecuteScalar("SELECT TO_DATE('02-JAN-2024','DD-MON-YYYY') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 2), ((DateTime)toDateWithMon!).Date);
    }

    /// <summary>
    /// EN: Ensures common Oracle scalar functions return expected values.
    /// PT: Garante que funcoes escalares comuns do Oracle retornem valores esperados.
    /// </summary>
    [Theory]
    [MemberData(nameof(OracleScalarCases))]
    [Trait("Category", "OracleMock")]
    public void ScalarFunctions_ShouldReturnExpectedValues(string sql, object expected)
    {
        var value = ExecuteScalar(sql);
        Assert.NotNull(value);
        Assert.Equal(expected, value);
    }

    /// <summary>
    /// EN: Ensures Oracle functions with stubbed behavior return null for now.
    /// PT: Garante que funcoes Oracle com comportamento stub retornem null por enquanto.
    /// </summary>
    [Theory]
    [MemberData(nameof(OracleNullCases))]
    [Trait("Category", "OracleMock")]
    public void StubbedFunctions_ShouldReturnNull(string sql)
    {
        Assert.Null(NormalizeStubbedValue(ExecuteScalar(sql)));
    }

    /// <summary>
    /// EN: Validates Oracle aggregate functions that were implemented in the mock.
    /// PT: Valida funcoes agregadas Oracle que foram implementadas no mock.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void AggregateFunctions_ShouldReturnExpectedValues()
    {
        var collect = ExecuteScalar("SELECT COLLECT(Name) FROM Users");
        var collection = Assert.IsType<object[]>(collect);
        Assert.Equal(2, collection.Length);

        var corr = Convert.ToDouble(ExecuteScalar("SELECT CORR(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(corr - 1d) < 0.0001d);

        var corrK = Convert.ToDouble(ExecuteScalar("SELECT CORR_K(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(corrK - 1d) < 0.0001d);

        var corrS = Convert.ToDouble(ExecuteScalar("SELECT CORR_S(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(corrS - 1d) < 0.0001d);

        var covarPop = Convert.ToDouble(ExecuteScalar("SELECT COVAR_POP(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(covarPop - 1.3333333333d) < 0.0001d);

        var covarSamp = Convert.ToDouble(ExecuteScalar("SELECT COVAR_SAMP(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(covarSamp - 2d) < 0.0001d);

        var approxCount = Convert.ToInt32(ExecuteScalar("SELECT APPROX_COUNT_DISTINCT(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.Equal(3, approxCount);

        var approxCountAgg = Convert.ToInt32(ExecuteScalar("SELECT APPROX_COUNT_DISTINCT_AGG(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.Equal(3, approxCountAgg);

        var approxCountDetail = Convert.ToInt32(ExecuteScalar("SELECT APPROX_COUNT_DISTINCT_DETAIL(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.Equal(3, approxCountDetail);

        var approxMedian = Convert.ToDouble(ExecuteScalar("SELECT APPROX_MEDIAN(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(approxMedian - 2d) < 0.0001d);

        var approxPercentile = Convert.ToDouble(ExecuteScalar("SELECT APPROX_PERCENTILE(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(approxPercentile - 2d) < 0.0001d);

        var approxPercentileAgg = Convert.ToDouble(ExecuteScalar("SELECT APPROX_PERCENTILE_AGG(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(approxPercentileAgg - 2d) < 0.0001d);

        var approxPercentileDetail = Convert.ToDouble(ExecuteScalar("SELECT APPROX_PERCENTILE_DETAIL(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(approxPercentileDetail - 2d) < 0.0001d);

        var groupId = Convert.ToInt32(ExecuteScalar("SELECT GROUP_ID() FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.Equal(0, groupId);

        var regrAvgX = Convert.ToDouble(ExecuteScalar("SELECT REGR_AVGX(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrAvgX - 2d) < 0.0001d);

        var regrAvgY = Convert.ToDouble(ExecuteScalar("SELECT REGR_AVGY(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrAvgY - 4d) < 0.0001d);

        var regrCount = Convert.ToInt32(ExecuteScalar("SELECT REGR_COUNT(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.Equal(3, regrCount);

        var regrIntercept = Convert.ToDouble(ExecuteScalar("SELECT REGR_INTERCEPT(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrIntercept) < 0.0001d);

        var regrR2 = Convert.ToDouble(ExecuteScalar("SELECT REGR_R2(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrR2 - 1d) < 0.0001d);

        var regrSxx = Convert.ToDouble(ExecuteScalar("SELECT REGR_SXX(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrSxx - 2d) < 0.0001d);

        var regrSyy = Convert.ToDouble(ExecuteScalar("SELECT REGR_SYY(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrSyy - 8d) < 0.0001d);

        var regrSxy = Convert.ToDouble(ExecuteScalar("SELECT REGR_SXY(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrSxy - 4d) < 0.0001d);

        var regrSlope = Convert.ToDouble(ExecuteScalar("SELECT REGR_SLOPE(X, Y) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(regrSlope - 2d) < 0.0001d);

        var stddev = Convert.ToDouble(ExecuteScalar("SELECT STDDEV_POP(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(stddev - Math.Sqrt(2d / 3d)) < 0.0001d);

        var stddevSamp = Convert.ToDouble(ExecuteScalar("SELECT STDDEV_SAMP(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(stddevSamp - Math.Sqrt(1d)) < 0.0001d);

        var cv = Convert.ToDouble(ExecuteScalar("SELECT CV(X) FROM Numbers"), CultureInfo.InvariantCulture);
        Assert.True(Math.Abs(cv - (Math.Sqrt(2d / 3d) / 2d)) < 0.0001d);

        var jsonObjectAgg = ExecuteScalar("SELECT JSON_OBJECTAGG(Name, Id) FROM Users");
        Assert.IsType<string>(jsonObjectAgg);
        using (var doc = JsonDocument.Parse((string)jsonObjectAgg!))
        {
            var root = doc.RootElement;
            Assert.Equal(1, root.GetProperty("Ana").GetInt32());
            Assert.Equal(2, root.GetProperty("Bob").GetInt32());
        }

        var jsonArrayAgg = ExecuteScalar("SELECT JSON_ARRAYAGG(Name) FROM Users");
        Assert.IsType<string>(jsonArrayAgg);
        using (var doc = JsonDocument.Parse((string)jsonArrayAgg!))
        {
            var root = doc.RootElement;
            Assert.Equal(JsonValueKind.Array, root.ValueKind);
            Assert.Equal(2, root.GetArrayLength());
        }

        var jsonArray = ExecuteScalar("SELECT JSON_ARRAY(Name, Id) FROM Users WHERE Id = 1");
        Assert.IsType<string>(jsonArray);
        using (var doc = JsonDocument.Parse((string)jsonArray!))
        {
            var root = doc.RootElement;
            Assert.Equal(JsonValueKind.Array, root.ValueKind);
            Assert.Equal(2, root.GetArrayLength());
        }

        var jsonValid = ExecuteScalar("SELECT JSON_VALID('{\"a\":1}') FROM Users WHERE Id = 1");
        Assert.Equal(1, Convert.ToInt32(jsonValid, CultureInfo.InvariantCulture));

        var jsonType = ExecuteScalar("SELECT JSON_TYPE('{\"a\":1}') FROM Users WHERE Id = 1");
        Assert.Equal("OBJECT", jsonType);

        var jsonLength = ExecuteScalar("SELECT JSON_LENGTH('[1,2,3]') FROM Users WHERE Id = 1");
        Assert.Equal(3, Convert.ToInt32(jsonLength, CultureInfo.InvariantCulture));

        var jsonValue = ExecuteScalar("SELECT JSON_VALUE('{\"a\":1}', '$.a') FROM Users WHERE Id = 1");
        Assert.Equal(1L, Convert.ToInt64(jsonValue, CultureInfo.InvariantCulture));

        var jsonValidInvalid = ExecuteScalar("SELECT JSON_VALID('{invalid}') FROM Users WHERE Id = 1");
        Assert.Equal(0, Convert.ToInt32(jsonValidInvalid, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Oracle JSON_TABLE materializes rows when the version gate is enabled.
    /// PT: Garante que Oracle JSON_TABLE materialize linhas quando o gate de versao esta habilitado.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versao do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void JsonTable_ShouldReturnExpectedRows(int version)
    {
        using var connection = CreateOpenConnection(version);
        using var command = new OracleCommandMock(connection)
        {
            CommandText = """
                SELECT jt.ord, jt.Id, jt.Name
                FROM JSON_TABLE(
                    '[{"id":1,"name":"Ana"},{"id":2,"name":"Bia"}]',
                    '$[*]' COLUMNS(
                        ord FOR ORDINALITY,
                        Id INT PATH '$.id',
                        Name VARCHAR2(50) PATH '$.name'
                    )
                ) jt
                ORDER BY jt.ord
                """
        };

        if (version < OracleDialect.OracleJsonSqlFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => command.ExecuteReader());
            Assert.Contains(SqlConst.JSON_TABLE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal(1L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(1, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Ana", reader.GetString(reader.GetOrdinal("Name")));

        Assert.True(reader.Read());
        Assert.Equal(2L, reader.GetInt64(reader.GetOrdinal("ord")));
        Assert.Equal(2, reader.GetInt32(reader.GetOrdinal("Id")));
        Assert.Equal("Bia", reader.GetString(reader.GetOrdinal("Name")));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Ensures Oracle approximate aggregate helpers follow the configured database version gates.
    /// PT: Garante que os helpers Oracle de agregacao aproximada sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void ApproximateAggregateFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        if (version < OracleDialect.ApproxCountDistinctMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT(X) FROM Numbers"));
            Assert.Contains("APPROX_COUNT_DISTINCT", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT(X) FROM Numbers"), CultureInfo.InvariantCulture));
        }

        if (version < OracleDialect.ApproximateAnalyticsMinVersion)
        {
            Assert.Contains(
                "APPROX_COUNT_DISTINCT_AGG",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT_AGG(X) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "APPROX_COUNT_DISTINCT_DETAIL",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT_DETAIL(X) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "APPROX_MEDIAN",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_MEDIAN(X) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "APPROX_PERCENTILE",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_PERCENTILE(X, 0.5) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "APPROX_PERCENTILE_AGG",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_PERCENTILE_AGG(X, 0.5) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "APPROX_PERCENTILE_DETAIL",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_PERCENTILE_DETAIL(X, 0.5) FROM Numbers")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "TO_APPROX_COUNT_DISTINCT",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TO_APPROX_COUNT_DISTINCT(1) FROM Users WHERE Id = 1")).Message,
                StringComparison.OrdinalIgnoreCase);
            Assert.Contains(
                "TO_APPROX_PERCENTILE",
                Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TO_APPROX_PERCENTILE(1) FROM Users WHERE Id = 1")).Message,
                StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT_AGG(X) FROM Numbers"), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT_DETAIL(X) FROM Numbers"), CultureInfo.InvariantCulture));
        Assert.True(Math.Abs(Convert.ToDouble(ExecuteScalar(connection, "SELECT APPROX_MEDIAN(X) FROM Numbers"), CultureInfo.InvariantCulture) - 2d) < 0.0001d);
        Assert.True(Math.Abs(Convert.ToDouble(ExecuteScalar(connection, "SELECT APPROX_PERCENTILE(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture) - 2d) < 0.0001d);
        Assert.True(Math.Abs(Convert.ToDouble(ExecuteScalar(connection, "SELECT APPROX_PERCENTILE_AGG(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture) - 2d) < 0.0001d);
        Assert.True(Math.Abs(Convert.ToDouble(ExecuteScalar(connection, "SELECT APPROX_PERCENTILE_DETAIL(X, 0.5) FROM Numbers"), CultureInfo.InvariantCulture) - 2d) < 0.0001d);
        Assert.Null(NormalizeStubbedValue(ExecuteScalar(connection, "SELECT TO_APPROX_COUNT_DISTINCT(1) FROM Users WHERE Id = 1")));
        Assert.Null(NormalizeStubbedValue(ExecuteScalar(connection, "SELECT TO_APPROX_PERCENTILE(1) FROM Users WHERE Id = 1")));
    }

    /// <summary>
    /// EN: Ensures Oracle-specific conversion helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle especificos de conversao sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleSpecificConversionFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_BINARY_DOUBLE(1) FROM Users WHERE Id = 1", "TO_BINARY_DOUBLE", OracleDialect.OracleBinaryConversionMinVersion, static value => Assert.Equal(1d, Convert.ToDouble(value, CultureInfo.InvariantCulture)));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_BINARY_FLOAT(1) FROM Users WHERE Id = 1", "TO_BINARY_FLOAT", OracleDialect.OracleBinaryConversionMinVersion, static value => Assert.Equal(1f, Convert.ToSingle(value, CultureInfo.InvariantCulture)));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_BLOB('abc') FROM Users WHERE Id = 1", "TO_BLOB", OracleDialect.OracleBlobConversionMinVersion, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_CLOB('abc') FROM Users WHERE Id = 1", "TO_CLOB", OracleDialect.OracleTextConversionMinVersion, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_DSINTERVAL(NUMTODSINTERVAL(2, 'HOUR')) FROM Users WHERE Id = 1", "TO_DSINTERVAL", OracleDialect.OracleTextConversionMinVersion, static value => Assert.Equal(TimeSpan.FromHours(2), value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_LOB('abc') FROM Users WHERE Id = 1", "TO_LOB", 7, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_MULTI_BYTE('abc') FROM Users WHERE Id = 1", "TO_MULTI_BYTE", 7, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_NCHAR('abc') FROM Users WHERE Id = 1", "TO_NCHAR", OracleDialect.OracleTextConversionMinVersion, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_NCLOB('abc') FROM Users WHERE Id = 1", "TO_NCLOB", OracleDialect.OracleTextConversionMinVersion, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_SINGLE_BYTE('abc') FROM Users WHERE Id = 1", "TO_SINGLE_BYTE", 7, static value => Assert.Equal("abc", value));
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_TIMESTAMP_TZ('2024-01-02 03:04:05 +02:00','YYYY-MM-DD HH24:MI:SS TZH:TZM') FROM Users WHERE Id = 1", "TO_TIMESTAMP_TZ", OracleDialect.OracleTextConversionMinVersion, static value =>
        {
            var dto = Assert.IsType<DateTimeOffset>(value);
            Assert.Equal(new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.FromHours(2)), dto);
        });
        AssertOracleSpecificConversionExecution(version, connection, "SELECT TO_YMINTERVAL(NUMTOYMINTERVAL(1, 'YEAR')) FROM Users WHERE Id = 1", "TO_YMINTERVAL", OracleDialect.OracleTextConversionMinVersion, static value => Assert.Equal(TimeSpan.FromDays(365), value));
    }

    /// <summary>
    /// EN: Ensures Oracle SCN helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de SCN sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleScnFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleScnExecution(version, connection, "SELECT SCN_TO_TIMESTAMP(1) FROM Users WHERE Id = 1", "SCN_TO_TIMESTAMP");
        AssertOracleScnExecution(version, connection, "SELECT TIMESTAMP_TO_SCN(TO_DATE('2024-01-01','YYYY-MM-DD')) FROM Users WHERE Id = 1", "TIMESTAMP_TO_SCN");
    }

    /// <summary>
    /// EN: Ensures Oracle analytics/modeling helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de analytics/modelagem sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleAnalyticsFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleAnalyticsExecution(version, connection, "SELECT FEATURE_COMPARE('x') FROM Users WHERE Id = 1", "FEATURE_COMPARE", 18);
        AssertOracleAnalyticsExecution(version, connection, "SELECT FEATURE_DETAILS('x') FROM Users WHERE Id = 1", "FEATURE_DETAILS", 12);
        AssertOracleAnalyticsExecution(version, connection, "SELECT FEATURE_ID('x') FROM Users WHERE Id = 1", "FEATURE_ID", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT FEATURE_SET('x') FROM Users WHERE Id = 1", "FEATURE_SET", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT FEATURE_VALUE('x') FROM Users WHERE Id = 1", "FEATURE_VALUE", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT NCGR(1) FROM Users WHERE Id = 1", "NCGR", 18);
        AssertOracleAnalyticsExecution(version, connection, "SELECT POWERMULTISET(1, 1) FROM Users WHERE Id = 1", "POWERMULTISET", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT POWERMULTISET_BY_CARDINALITY(1, 1) FROM Users WHERE Id = 1", "POWERMULTISET_BY_CARDINALITY", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION('x') FROM Users WHERE Id = 1", "PREDICTION", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION_BOUNDS('x') FROM Users WHERE Id = 1", "PREDICTION_BOUNDS", 11);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION_COST('x') FROM Users WHERE Id = 1", "PREDICTION_COST", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION_DETAILS('x') FROM Users WHERE Id = 1", "PREDICTION_DETAILS", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION_PROBABILITY('x') FROM Users WHERE Id = 1", "PREDICTION_PROBABILITY", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PREDICTION_SET('x') FROM Users WHERE Id = 1", "PREDICTION_SET", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PRESENTNNV('x') FROM Users WHERE Id = 1", "PRESENTNNV", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT PRESENTV('x') FROM Users WHERE Id = 1", "PRESENTV", 10);
        AssertOracleAnalyticsExecution(version, connection, "SELECT RATIO_TO_REPORT(1) FROM Users WHERE Id = 1", "RATIO_TO_REPORT", 8);
    }

    /// <summary>
    /// EN: Ensures Oracle clustering helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de clustering sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleClusterFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT CLUSTER_DETAILS(1, 1, 1) FROM Users WHERE Id = 1", "CLUSTER_DETAILS", OracleDialect.OracleAdvancedClusterFunctionMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT CLUSTER_DISTANCE(1, 1, 1) FROM Users WHERE Id = 1", "CLUSTER_DISTANCE", OracleDialect.OracleAdvancedClusterFunctionMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT CLUSTER_ID(1, 1, 1) FROM Users WHERE Id = 1", "CLUSTER_ID", OracleDialect.OracleClusterFunctionMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT CLUSTER_PROBABILITY(1, 1, 1) FROM Users WHERE Id = 1", "CLUSTER_PROBABILITY", OracleDialect.OracleClusterFunctionMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT CLUSTER_SET(1, 1, 1) FROM Users WHERE Id = 1", "CLUSTER_SET", OracleDialect.OracleClusterFunctionMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
    }

    /// <summary>
    /// EN: Ensures Oracle container and rowid helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de container e rowid sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleContainerAndRowIdFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT CON_UID_TO_ID(42) FROM Users WHERE Id = 1", "CON_UID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion, static value => Assert.Equal(42L, value));
        AssertOracleVersionedExecution(version, connection, "SELECT CON_DBID_TO_ID(7) FROM Users WHERE Id = 1", "CON_DBID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion, static value => Assert.Equal(7L, value));
        AssertOracleVersionedExecution(version, connection, "SELECT CON_GUID_TO_ID(8) FROM Users WHERE Id = 1", "CON_GUID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion, static value => Assert.Equal(8L, value));
        AssertOracleVersionedExecution(version, connection, "SELECT CON_NAME_TO_ID(9) FROM Users WHERE Id = 1", "CON_NAME_TO_ID", OracleDialect.OracleContainerFunctionMinVersion, static value => Assert.Equal(9L, value));
        AssertOracleVersionedExecution(version, connection, "SELECT ROWIDTOCHAR('AA') FROM Users WHERE Id = 1", "ROWIDTOCHAR", 7, static value => Assert.Equal("AA", value));
        AssertOracleVersionedExecution(version, connection, "SELECT ROWTONCHAR('BB') FROM Users WHERE Id = 1", "ROWTONCHAR", OracleDialect.OracleRowToNCharFunctionMinVersion, static value => Assert.Equal("BB", value));
    }

    /// <summary>
    /// EN: Ensures Oracle metadata, validation, and JSON transform helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de metadados, validacao e JSON transform sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleMetadataValidationAndJsonFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT USERENV('CURRENT_SCHEMA') FROM Users WHERE Id = 1", "USERENV", 7, static value => Assert.Equal("SYS", value));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_INVOKING_USER() FROM Users WHERE Id = 1", "ORA_INVOKING_USER", OracleDialect.OracleUserEnvMetadataMinVersion, static value => Assert.Equal("SYS", value));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_INVOKING_USERID() FROM Users WHERE Id = 1", "ORA_INVOKING_USERID", OracleDialect.OracleUserEnvMetadataMinVersion, static value => Assert.Equal(0, value));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_DST_AFFECTED(1) FROM Users WHERE Id = 1", "ORA_DST_AFFECTED", OracleDialect.OracleUserEnvMetadataMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_DST_CONVERT(1) FROM Users WHERE Id = 1", "ORA_DST_CONVERT", OracleDialect.OracleUserEnvMetadataMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_DST_ERROR(1) FROM Users WHERE Id = 1", "ORA_DST_ERROR", OracleDialect.OracleUserEnvMetadataMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT ORA_DM_PARTITION_NAME() FROM Users WHERE Id = 1", "ORA_DM_PARTITION_NAME", OracleDialect.OraclePartitionMetadataMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT VALIDATE_CONVERSION('123', 'NUMBER') FROM Users WHERE Id = 1", "VALIDATE_CONVERSION", OracleDialect.OracleValidateConversionMinVersion, static value => Assert.Equal(1, value));
        AssertOracleVersionedExecution(version, connection, "SELECT JSON_TRANSFORM('{\"a\":1}') FROM Users WHERE Id = 1", "JSON_TRANSFORM", OracleDialect.OracleJsonTransformMinVersion, static value => Assert.Equal("{\"a\":1}", value));
    }

    /// <summary>
    /// EN: Ensures Oracle JSON extraction helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de extracao JSON sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleJsonExtractionFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT JSON_VALUE('{\"a\":1}', '$.a') FROM Users WHERE Id = 1", "JSON_VALUE", OracleDialect.OracleJsonSqlFunctionMinVersion, static value => Assert.Equal(1L, Convert.ToInt64(value, CultureInfo.InvariantCulture)));
        AssertOracleVersionedExecution(version, connection, "SELECT JSON_QUERY('{\"a\":{\"b\":2}}', '$.a') FROM Users WHERE Id = 1", "JSON_QUERY", OracleDialect.OracleJsonSqlFunctionMinVersion, static value => Assert.Equal("{\"b\":2}", value));
    }

    /// <summary>
    /// EN: Ensures Oracle collation and NLS helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de collation e NLS sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleCollationAndNlsFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT COLLATION('x') FROM Users WHERE Id = 1", "COLLATION", OracleDialect.OracleCollationFunctionMinVersion, static value => Assert.Equal("BINARY", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_CHARSET_DECL_LEN('AL32UTF8') FROM Users WHERE Id = 1", "NLS_CHARSET_DECL_LEN", 7, static value => Assert.Equal(0, value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_CHARSET_ID('AL32UTF8') FROM Users WHERE Id = 1", "NLS_CHARSET_ID", 7, static value => Assert.Equal(0, value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_CHARSET_NAME('AL32UTF8') FROM Users WHERE Id = 1", "NLS_CHARSET_NAME", 7, static value => Assert.Equal("AL32UTF8", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_COLLATION_ID('BINARY') FROM Users WHERE Id = 1", "NLS_COLLATION_ID", OracleDialect.OracleCollationFunctionMinVersion, static value => Assert.Equal(0, value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_COLLATION_NAME('BINARY') FROM Users WHERE Id = 1", "NLS_COLLATION_NAME", OracleDialect.OracleCollationFunctionMinVersion, static value => Assert.Equal("BINARY", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_INITCAP('aNA') FROM Users WHERE Id = 1", "NLS_INITCAP", 7, static value => Assert.Equal("Ana", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_LOWER('ABC') FROM Users WHERE Id = 1", "NLS_LOWER", 7, static value => Assert.Equal("abc", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLS_UPPER('abc') FROM Users WHERE Id = 1", "NLS_UPPER", 7, static value => Assert.Equal("ABC", value));
        AssertOracleVersionedExecution(version, connection, "SELECT NLSSORT('abc') FROM Users WHERE Id = 1", "NLSSORT", 7, static value => Assert.Equal("abc", value));
    }

    /// <summary>
    /// EN: Ensures Oracle hash and SYS helpers follow the configured database version gates.
    /// PT: Garante que helpers Oracle de hash e SYS sigam os gates da versao configurada do banco.
    /// </summary>
    /// <param name="version">EN: Oracle version under test. PT: Versão do Oracle em teste.</param>
    [Theory]
    [Trait("Category", "OracleMock")]
    [MemberDataOracleVersion]
    public void OracleHashAndSysFunctions_ShouldRespectOracleVersion(int version)
    {
        using var connection = CreateOpenConnection(version);

        AssertOracleVersionedExecution(version, connection, "SELECT ORA_HASH('ABC') FROM Users WHERE Id = 1", "ORA_HASH", OracleDialect.OracleOraHashMinVersion, static value => Assert.True(Convert.ToInt32(value, CultureInfo.InvariantCulture) >= 0));
        AssertOracleVersionedExecution(version, connection, "SELECT STANDARD_HASH('ABC','SHA256') FROM Users WHERE Id = 1", "STANDARD_HASH", OracleDialect.OracleStandardHashMinVersion, static value =>
        {
            var text = Assert.IsType<string>(value);
            Assert.Equal(64, text.Length);
        });
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_GUID() FROM Users WHERE Id = 1", "SYS_GUID", 7, static value => Assert.IsType<string>(value));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM Users WHERE Id = 1", "SYS_CONTEXT", 7, static value => Assert.Equal("SYS", value));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_CONNECT_BY_PATH('a','/') FROM Users WHERE Id = 1", "SYS_CONNECT_BY_PATH", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_DBURIGEN('a','b') FROM Users WHERE Id = 1", "SYS_DBURIGEN", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_EXTRACT_UTC(TO_TIMESTAMP_TZ('2024-01-01 10:00:00 +02:00')) FROM Users WHERE Id = 1", "SYS_EXTRACT_UTC", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.IsType<DateTime>(value));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_OP_ZONE_ID('a') FROM Users WHERE Id = 1", "SYS_OP_ZONE_ID", OracleDialect.OracleSysZoneIdMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_TYPEID('a') FROM Users WHERE Id = 1", "SYS_TYPEID", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_XMLAGG('a') FROM Users WHERE Id = 1", "SYS_XMLAGG", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
        AssertOracleVersionedExecution(version, connection, "SELECT SYS_XMLGEN('a') FROM Users WHERE Id = 1", "SYS_XMLGEN", OracleDialect.OracleSysFamilyMinVersion, static value => Assert.Null(NormalizeStubbedValue(value)));
    }

    /// <summary>
    /// EN: Verifies Oracle time and timezone helpers return expected types.
    /// PT: Verifica se auxiliares de tempo e fuso do Oracle retornam tipos esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void TimeFunctions_ShouldReturnExpectedTypes()
    {
        var fromTz = ExecuteScalar("SELECT FROM_TZ(TO_DATE('2024-01-02','YYYY-MM-DD'), '02:00') FROM Users WHERE Id = 1");
        Assert.IsType<DateTimeOffset>(fromTz);

        var fromTzNegative = ExecuteScalar("SELECT FROM_TZ(TO_DATE('2024-01-02','YYYY-MM-DD'), '-03:00') FROM Users WHERE Id = 1");
        Assert.IsType<DateTimeOffset>(fromTzNegative);
        Assert.Equal(TimeSpan.FromHours(-3), ((DateTimeOffset)fromTzNegative!).Offset);

        var localTimestamp = ExecuteScalar("SELECT LOCALTIMESTAMP() FROM Users WHERE Id = 1");
        Assert.IsType<DateTime>(localTimestamp);

        var nextDay = ExecuteScalar("SELECT NEXT_DAY(TO_DATE('2024-01-01','YYYY-MM-DD'), 'FRIDAY') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 5), ((DateTime)nextDay!).Date);

        var nextDayShort = ExecuteScalar("SELECT NEXT_DAY(TO_DATE('2024-01-01','YYYY-MM-DD'), 'MON') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 8), ((DateTime)nextDayShort!).Date);

        var newTime = ExecuteScalar("SELECT NEW_TIME(TO_DATE('2024-01-01','YYYY-MM-DD'), '00:00', '02:00') FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 1, 1, 2, 0, 0), (DateTime)newTime!);

        var sysExtractUtc = ExecuteScalar("SELECT SYS_EXTRACT_UTC(TO_TIMESTAMP_TZ('2024-01-01 10:00:00 +02:00')) FROM Users WHERE Id = 1");
        Assert.IsType<DateTime>(sysExtractUtc);

        var sessionTimeZone = ExecuteScalar("SELECT SESSIONTIMEZONE() FROM Users WHERE Id = 1");
        Assert.IsType<string>(sessionTimeZone);
    }

    /// <summary>
    /// EN: Ensures date arithmetic helpers return expected dates.
    /// PT: Garante que auxiliares de aritmetica de datas retornem datas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void DateArithmeticFunctions_ShouldReturnExpectedValues()
    {
        var addMonths = ExecuteScalar("SELECT ADD_MONTHS(TO_DATE('2024-01-31','YYYY-MM-DD'), 1) FROM Users WHERE Id = 1");
        Assert.Equal(new DateTime(2024, 2, 29), ((DateTime)addMonths!).Date);
    }

    /// <summary>
    /// EN: Confirms Oracle RAW, regex, and user environment helpers return expected values.
    /// PT: Confirma que auxiliares RAW, regex e ambiente de usuario Oracle retornam valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleMock")]
    public void RawRegexAndUserEnvFunctions_ShouldReturnExpectedValues()
    {
        var hexToRaw = ExecuteScalar("SELECT HEXTORAW('0A0B') FROM Users WHERE Id = 1");
        var rawBytes = Assert.IsType<byte[]>(hexToRaw);
        Assert.Equal(new byte[] { 0x0A, 0x0B }, rawBytes);

        var hexToRawAlt = ExecuteScalar("SELECT HEXTORAW('0x0A0B') FROM Users WHERE Id = 1");
        var rawBytesAlt = Assert.IsType<byte[]>(hexToRawAlt);
        Assert.Equal(new byte[] { 0x0A, 0x0B }, rawBytesAlt);

        var rawToHex = ExecuteScalar("SELECT RAWTOHEX('ABC') FROM Users WHERE Id = 1");
        Assert.Equal("414243", rawToHex);

        var rawToNHex = ExecuteScalar("SELECT RAWTONHEX('AB') FROM Users WHERE Id = 1");
        Assert.Equal("4142", rawToNHex);

        var refValue = ExecuteScalar("SELECT REF('ABC') FROM Users WHERE Id = 1");
        Assert.Equal("ABC", refValue);

        var refToHex = ExecuteScalar("SELECT REFTOHEX('ABC') FROM Users WHERE Id = 1");
        Assert.Equal("414243", refToHex);

        var oraHash = ExecuteScalar("SELECT ORA_HASH('ABC') FROM Users WHERE Id = 1");
        Assert.True(Convert.ToInt32(oraHash, CultureInfo.InvariantCulture) >= 0);

        var standardHash = ExecuteScalar("SELECT STANDARD_HASH('ABC','SHA256') FROM Users WHERE Id = 1");
        Assert.IsType<string>(standardHash);
        Assert.Equal(64, ((string)standardHash!).Length);

        var regexCount = ExecuteScalar("SELECT REGEXP_COUNT('abc123abc', 'abc') FROM Users WHERE Id = 1");
        Assert.Equal(2, Convert.ToInt32(regexCount, CultureInfo.InvariantCulture));

        var regexSubstr = ExecuteScalar("SELECT REGEXP_SUBSTR('abc123abc', 'abc') FROM Users WHERE Id = 1");
        Assert.Equal("abc", regexSubstr);

        var regexInstr = ExecuteScalar("SELECT REGEXP_INSTR('abc123abc', 'abc') FROM Users WHERE Id = 1");
        Assert.Equal(1, Convert.ToInt32(regexInstr, CultureInfo.InvariantCulture));

        var regexReplace = ExecuteScalar("SELECT REGEXP_REPLACE('abc123abc', '[0-9]+', '-') FROM Users WHERE Id = 1");
        Assert.Equal("abc-abc", regexReplace);

        var user = ExecuteScalar("SELECT USER FROM Users WHERE Id = 1");
        Assert.Equal("SYS", user);

        var userEnv = ExecuteScalar("SELECT USERENV('CURRENT_SCHEMA') FROM Users WHERE Id = 1");
        Assert.Equal("SYS", userEnv);

        var invokingUser = ExecuteScalar("SELECT ORA_INVOKING_USER FROM Users WHERE Id = 1");
        Assert.Equal("SYS", invokingUser);

        var invokingUserId = ExecuteScalar("SELECT ORA_INVOKING_USERID FROM Users WHERE Id = 1");
        Assert.Equal(0, Convert.ToInt32(invokingUserId, CultureInfo.InvariantCulture));

        var sysContext = ExecuteScalar("SELECT SYS_CONTEXT('USERENV','CURRENT_SCHEMA') FROM Users WHERE Id = 1");
        Assert.Equal("SYS", sysContext);

        var sysGuid = ExecuteScalar("SELECT SYS_GUID() FROM Users WHERE Id = 1");
        Assert.IsType<string>(sysGuid);
    }

    /// <summary>
    /// EN: Releases Oracle connections created for the tests.
    /// PT: Libera as conexoes Oracle criadas para os testes.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }

    /// <summary>
    /// EN: Provides scalar function cases and expected values.
    /// PT: Fornece casos de funcoes escalares e valores esperados.
    /// </summary>
    public static IEnumerable<object[]> OracleScalarCases()
    {
        yield return new object[] { "SELECT ASCIISTR('á') FROM Users WHERE Id = 1", "\\00E1" };
        yield return new object[] { "SELECT BFILENAME('DIR', 'FILE') FROM Users WHERE Id = 1", "DIR/FILE" };
        yield return new object[] { "SELECT BIN_TO_NUM(1,0,1) FROM Users WHERE Id = 1", 5L };
        yield return new object[] { "SELECT BITAND(6,3) FROM Users WHERE Id = 1", 2L };
        yield return new object[] { "SELECT CARDINALITY(COLLECT(Name)) FROM Users", 2 };
        yield return new object[] { "SELECT CHARTOROWID('AA') FROM Users WHERE Id = 1", "AA" };
        yield return new object[] { "SELECT CHR(65) FROM Users WHERE Id = 1", "A" };
        yield return new object[] { "SELECT COLLATION('x') FROM Users WHERE Id = 1", "BINARY" };
        yield return new object[] { "SELECT COMPOSE('A') FROM Users WHERE Id = 1", "A" };
        yield return new object[] { "SELECT CONVERT('ABC','AL32UTF8') FROM Users WHERE Id = 1", "ABC" };
        yield return new object[] { "SELECT CON_UID_TO_ID(42) FROM Users WHERE Id = 1", 42L };
        yield return new object[] { "SELECT CON_DBID_TO_ID(7) FROM Users WHERE Id = 1", 7L };
        yield return new object[] { "SELECT CON_GUID_TO_ID(8) FROM Users WHERE Id = 1", 8L };
        yield return new object[] { "SELECT CON_NAME_TO_ID(9) FROM Users WHERE Id = 1", 9L };
        yield return new object[] { "SELECT DBTIMEZONE() FROM Users WHERE Id = 1", "+00:00" };
        yield return new object[] { "SELECT DECOMPOSE('A') FROM Users WHERE Id = 1", "A" };
        yield return new object[] { "SELECT DEPTH('x') FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT DEREF('x') FROM Users WHERE Id = 1", "x" };
        yield return new object[] { "SELECT DUMP('ABC') FROM Users WHERE Id = 1", "Typ=1 Len=3" };
        yield return new object[] { "SELECT EMPTY_BLOB() FROM Users WHERE Id = 1", Array.Empty<byte>() };
        yield return new object[] { "SELECT EMPTY_CLOB() FROM Users WHERE Id = 1", string.Empty };
        yield return new object[] { "SELECT EXISTSNODE('<a/>','/a') FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT ITERATION_NUMBER() FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT INITCAP('aNA bOB') FROM Users WHERE Id = 1", "Ana Bob" };
        yield return new object[] { "SELECT LNNVL(1=2) FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT LNNVL(NULL) FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT LOWER('ABC') FROM Users WHERE Id = 1", "abc" };
        yield return new object[] { "SELECT LTRIM('   abc') FROM Users WHERE Id = 1", "abc" };
        yield return new object[] { "SELECT MOD(7,3) FROM Users WHERE Id = 1", 1m };
        yield return new object[] { "SELECT MONTHS_BETWEEN(TO_DATE('2024-03-01','YYYY-MM-DD'), TO_DATE('2024-01-01','YYYY-MM-DD')) FROM Users WHERE Id = 1", 2m };
        yield return new object[] { "SELECT MONTHS_BETWEEN(TO_DATE('2024-03-15','YYYY-MM-DD'), TO_DATE('2024-01-01','YYYY-MM-DD')) FROM Users WHERE Id = 1", 2m + (14m / 31m) };
        yield return new object[] { "SELECT NANVL(1, 5) FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT NANVL(NULL, 5) FROM Users WHERE Id = 1", 5 };
        yield return new object[] { "SELECT NANVL('NaN', 5) FROM Users WHERE Id = 1", 5 };
        yield return new object[] { "SELECT NLS_CHARSET_DECL_LEN('AL32UTF8') FROM Users WHERE Id = 1", 0 };
        yield return new object[] { "SELECT NLS_CHARSET_ID('AL32UTF8') FROM Users WHERE Id = 1", 0 };
        yield return new object[] { "SELECT NLS_CHARSET_NAME('AL32UTF8') FROM Users WHERE Id = 1", "AL32UTF8" };
        yield return new object[] { "SELECT NLS_COLLATION_ID('BINARY') FROM Users WHERE Id = 1", 0 };
        yield return new object[] { "SELECT NLS_COLLATION_NAME('BINARY') FROM Users WHERE Id = 1", "BINARY" };
        yield return new object[] { "SELECT NLS_INITCAP('aNA') FROM Users WHERE Id = 1", "Ana" };
        yield return new object[] { "SELECT NLS_LOWER('ABC') FROM Users WHERE Id = 1", "abc" };
        yield return new object[] { "SELECT NLS_UPPER('abc') FROM Users WHERE Id = 1", "ABC" };
        yield return new object[] { "SELECT NLSSORT('abc') FROM Users WHERE Id = 1", "abc" };
        yield return new object[] { "SELECT NUMTODSINTERVAL(2, 'HOUR') FROM Users WHERE Id = 1", TimeSpan.FromHours(2) };
        yield return new object[] { "SELECT NUMTOYMINTERVAL(1, 'YEAR') FROM Users WHERE Id = 1", TimeSpan.FromDays(365) };
        yield return new object[] { "SELECT REMAINDER(7, 3) FROM Users WHERE Id = 1", Math.IEEERemainder(7d, 3d) };
        yield return new object[] { "SELECT ROWIDTOCHAR('AA') FROM Users WHERE Id = 1", "AA" };
        yield return new object[] { "SELECT ROWTONCHAR('BB') FROM Users WHERE Id = 1", "BB" };
        yield return new object[] { "SELECT TRANSLATE('abc','ab','xy') FROM Users WHERE Id = 1", "xyc" };
        yield return new object[] { "SELECT TZ_OFFSET('02:00') FROM Users WHERE Id = 1", "+02:00" };
        yield return new object[] { "SELECT VALIDATE_CONVERSION('123', 'NUMBER') FROM Users WHERE Id = 1", 1 };
        yield return new object[] { "SELECT VSIZE('ABC') FROM Users WHERE Id = 1", 3 };
        yield return new object[] { "SELECT WIDTH_BUCKET(5, 0, 10, 5) FROM Users WHERE Id = 1", 3 };
    }

    /// <summary>
    /// EN: Provides function calls that currently return null in the mock.
    /// PT: Fornece chamadas de funcoes que atualmente retornam null no mock.
    /// </summary>
    public static IEnumerable<object[]> OracleNullCases()
    {
        yield return new object[] { "SELECT ORA_DST_AFFECTED(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CLUSTER_DETAILS(1, 1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CLUSTER_DISTANCE(1, 1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CLUSTER_ID(1, 1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CLUSTER_PROBABILITY(1, 1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CLUSTER_SET(1, 1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT CUBE_TABLE(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT DATAOBJ_TO_PARTITION(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT DATAOBJ_TO_MAT_PARTITION(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT FEATURE_COMPARE('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT FEATURE_DETAILS('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT FEATURE_ID('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT FEATURE_SET('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT FEATURE_VALUE('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT JSON_DATAGUIDE('{\"a\":1}') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT JSON_TRANSFORM('{\"a\":1}') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT MAKE_REF(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT NCGR(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT ORA_DM_PARTITION_NAME(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT ORA_DST_CONVERT(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT ORA_DST_ERROR(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT POWERMULTISET(1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT POWERMULTISET_BY_CARDINALITY(1, 1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION_BOUNDS('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION_COST('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION_DETAILS('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION_PROBABILITY('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PREDICTION_SET('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PRESENTNNV('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT PRESENTV('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT RATIO_TO_REPORT(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SCN_TO_TIMESTAMP(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_BINOMIAL_TEST(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_CROSSTAB(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_F_TEST(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_KS_TEST(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_MODE(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_MW_TEST(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_ONE_WAY_ANOVA(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_T_TEST_INDEP(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_T_TEST_INDEPU(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_T_TEST_ONE(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_T_TEST_PAIRED(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT STATS_WSR_TEST(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT TIMESTAMP_TO_SCN(TO_DATE('2024-01-01','YYYY-MM-DD')) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT TO_APPROX_COUNT_DISTINCT(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT TO_APPROX_PERCENTILE(1) FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_CONNECT_BY_PATH('a','/') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_DBURIGEN('a','b') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_OP_ZONE_ID('a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_TYPEID('a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_XMLAGG('a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT SYS_XMLGEN('a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLAGG(Name) FROM Users" };
        yield return new object[] { "SELECT EXTRACTVALUE('<a/>','/a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLCAST('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLCDATA('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLCOLATTVAL('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLCOMMENT('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLCONCAT('<a/>', '<b/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLDIFF('<a/>', '<b/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLELEMENT('x', 'a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLEXISTS('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLFOREST('a') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLISVALID('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLPARSE('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLPATCH('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLPI('x') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLQUERY('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLROOT('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLSEQUENCE('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLSERIALIZE('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLTABLE('<a/>') FROM Users WHERE Id = 1" };
        yield return new object[] { "SELECT XMLTRANSFORM('<a/>') FROM Users WHERE Id = 1" };
    }

    private object? ExecuteScalar(string sql)
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private static OracleConnectionMock CreateOpenConnection(int version)
    {
        var db = new OracleDbMock(version);
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
        ]);
        db.AddTable("Numbers",
        [
            new("X", DbType.Int32, false),
            new("Y", DbType.Int32, false),
        ]);

        var connection = new OracleConnectionMock(db);
        connection.Open();

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')");
        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (2, 'Bob')");
        ExecuteNonQuery(connection, "INSERT INTO Numbers (X, Y) VALUES (1, 2)");
        ExecuteNonQuery(connection, "INSERT INTO Numbers (X, Y) VALUES (2, 4)");
        ExecuteNonQuery(connection, "INSERT INTO Numbers (X, Y) VALUES (3, 6)");
        return connection;
    }

    private void ExecuteNonQuery(string sql)
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }

    private static object? ExecuteScalar(OracleConnectionMock connection, string sql)
    {
        using var command = new OracleCommandMock(connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private static void ExecuteNonQuery(OracleConnectionMock connection, string sql)
    {
        using var command = new OracleCommandMock(connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }

    private static void AssertOracleSpecificConversionExecution(
        int version,
        OracleConnectionMock connection,
        string sql,
        string functionName,
        int minVersion,
        Action<object?> assertSupported)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, sql));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        assertSupported(ExecuteScalar(connection, sql));
    }

    private static void AssertOracleScnExecution(int version, OracleConnectionMock connection, string sql, string functionName)
    {
        if (version < OracleDialect.OracleScnFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, sql));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Null(NormalizeStubbedValue(ExecuteScalar(connection, sql)));
    }

    private static void AssertOracleAnalyticsExecution(int version, OracleConnectionMock connection, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, sql));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Null(NormalizeStubbedValue(ExecuteScalar(connection, sql)));
    }

    private static void AssertOracleVersionedExecution(
        int version,
        OracleConnectionMock connection,
        string sql,
        string functionName,
        int minVersion,
        Action<object?> assertSupported)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, sql));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        assertSupported(ExecuteScalar(connection, sql));
    }

    private static object? NormalizeStubbedValue(object? value)
    {
        if (value is DBNull)
            return null;
        if (value is string text && string.IsNullOrWhiteSpace(text))
            return null;
        if (value is Array array && array.Length == 0)
            return null;
        if (value is string jsonText && jsonText == "{}")
            return null;
        if (value is string jsonRaw && jsonRaw == "{\"a\":1}")
            return null;
        return value;
    }
}
