
using System.Text;
using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Validates SQL Azure function execution across supported compatibility levels.
/// PT: Valida a execucao de funcoes do SQL Azure nos niveis de compatibilidade suportados.
/// </summary>
public sealed class SqlAzureFunctionTests
    : XUnitTestBase
{
    private readonly SqlAzureConnectionMock _connection;

    /// <summary>
    /// EN: Initializes SQL Azure function fixtures with sample tables and sequences.
    /// PT: Inicializa os fixtures de funcoes do SQL Azure com tabelas e sequences de exemplo.
    /// </summary>
    public SqlAzureFunctionTests(ITestOutputHelper helper)
        : base(helper)
    {
        _connection = CreateOpenConnection();
    }

    /// <summary>
    /// EN: Ensures SQL Azure system functions return expected values.
    /// PT: Garante que funcoes de sistema do SQL Azure retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
    public void SystemFunctions_ShouldReturnExpectedValues()
    {
        ExecuteNonQuery("INSERT INTO IdentityUsers (Name) VALUES ('Auto')");

        Assert.Equal("DbSqlLikeMem", ExecuteScalar("SELECT APP_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal("127.0.0.1", ExecuteScalar("SELECT CONNECTIONPROPERTY('local_net_address') FROM Users WHERE Id = 1"));
        Assert.Equal("TCP", ExecuteScalar("SELECT CONNECTIONPROPERTY('net_transport') FROM Users WHERE Id = 1"));
        Assert.Equal("ONLINE", ExecuteScalar("SELECT DATABASEPROPERTYEX('DefaultSchema', 'Status') FROM Users WHERE Id = 1"));
        Assert.Equal("READ_WRITE", ExecuteScalar("SELECT DATABASEPROPERTYEX('DefaultSchema', 'Updateability') FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT DATABASE_PRINCIPAL_ID('dbo') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT CURRENT_USER FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT COLUMNPROPERTY(OBJECT_ID('Users'), 'Name', 'ColumnId') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT COLUMNPROPERTY(OBJECT_ID('IdentityUsers'), 'Id', 'IsIdentity') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT COLUMNPROPERTY(OBJECT_ID('Users'), 'Email', 'AllowsNull') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT COL_LENGTH('Users', 'Id') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Name", ExecuteScalar("SELECT COL_NAME(2, 2) FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT DB_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("DefaultSchema", ExecuteScalar("SELECT DB_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar("SELECT OBJECT_ID('Users') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT OBJECTPROPERTY(OBJECT_ID('Users'), 'IsTable') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT OBJECTPROPERTYEX(OBJECT_ID('sp_ping'), 'IsProcedure') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Users", ExecuteScalar("SELECT OBJECT_NAME(2) FROM Users WHERE Id = 1"));
        Assert.Equal("DefaultSchema", ExecuteScalar("SELECT OBJECT_SCHEMA_NAME(2) FROM Users WHERE Id = 1"));
        Assert.Equal("DefaultSchema", ExecuteScalar("SELECT ORIGINAL_DB_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SCHEMA_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT SCHEMA_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal("2022", ExecuteScalar("SELECT SERVERPROPERTY('ProductVersion') FROM Users WHERE Id = 1"));
        Assert.Equal("sa", ExecuteScalar("SELECT ORIGINAL_LOGIN() FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT CURRENT_REQUEST_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SESSION_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(56, Convert.ToInt32(ExecuteScalar("SELECT TYPE_ID('int') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("int", ExecuteScalar("SELECT TYPE_NAME(56) FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT TYPEPROPERTY('int', 'OwnerId') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT SESSION_USER FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SUSER_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("sa", ExecuteScalar("SELECT SUSER_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(new byte[] { 0x01 }, Assert.IsType<byte[]>(ExecuteScalar("SELECT SUSER_SID() FROM Users WHERE Id = 1")));
        Assert.Equal("sa", ExecuteScalar("SELECT SUSER_SNAME() FROM Users WHERE Id = 1"));
        Assert.Equal("sa", ExecuteScalar("SELECT SYSTEM_USER FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT USER_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("dbo", ExecuteScalar("SELECT USER_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar("SELECT XACT_STATE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT GETUTCDATE() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTimeOffset>(ExecuteScalar("SELECT SYSDATETIMEOFFSET() FROM Users WHERE Id = 1"));
        Assert.IsType<DateTime>(ExecuteScalar("SELECT SYSUTCDATETIME() FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Azure transaction-state helpers reflect active transactions.
    /// PT: Garante que helpers de estado de transacao do SQL Azure reflitam transacoes ativas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
    public void TransactionStateFunctions_ShouldReturnExpectedValues()
    {
        using var transaction = _connection.BeginTransaction();
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT XACT_STATE() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1L, Convert.ToInt64(ExecuteScalar("SELECT CURRENT_TRANSACTION_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        transaction.Rollback();
    }

    /// <summary>
    /// EN: Ensures SQL Azure scalar helpers return expected values.
    /// PT: Garante que helpers escalares do SQL Azure retornem valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
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
        Assert.NotEqual(
            Convert.ToInt32(ExecuteScalar("SELECT BINARY_CHECKSUM('ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture),
            Convert.ToInt32(ExecuteScalar("SELECT BINARY_CHECKSUM('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar("SELECT CHARINDEX('bar', 'foobar') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(
            Convert.ToInt32(ExecuteScalar("SELECT CHECKSUM('ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture),
            Convert.ToInt32(ExecuteScalar("SELECT CHECKSUM('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("0042", ExecuteScalar("SELECT FORMAT(42, 'D4') FROM Users WHERE Id = 1"));
        Assert.Equal("Hello Bob #7", ExecuteScalar("SELECT FORMATMESSAGE('Hello %s #%d', 'Bob', 7) FROM Users WHERE Id = 1"));
        Assert.Equal(3L, Convert.ToInt64(ExecuteScalar("SELECT LEN('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("Ana", ExecuteScalar("SELECT LTRIM('  Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("dbo", ExecuteScalar("SELECT PARSENAME('server.database.dbo.Users', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("[Ana]", ExecuteScalar("SELECT QUOTENAME('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("NaNa", ExecuteScalar("SELECT REPLICATE('Na', 2) FROM Users WHERE Id = 1"));
        Assert.Equal("anA", ExecuteScalar("SELECT REVERSE('Ana') FROM Users WHERE Id = 1"));
        Assert.Equal("Ana", ExecuteScalar("SELECT RTRIM('Ana  ') FROM Users WHERE Id = 1"));
        Assert.Equal("   ", ExecuteScalar("SELECT SPACE(3) FROM Users WHERE Id = 1"));
        Assert.Equal(" 123.5", ExecuteScalar("SELECT STR(123.45, 6, 1) FROM Users WHERE Id = 1"));
        Assert.Equal("Axxa", ExecuteScalar("SELECT STUFF('Ana', 2, 1, 'xx') FROM Users WHERE Id = 1"));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar("SELECT UNICODE('A') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Azure date helper availability follows compatibility-level rules.
    /// PT: Garante que a disponibilidade de helpers de data no SQL Azure siga as regras de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade do SQL Azure em teste.</param>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    [Trait("Category", "SqlAzureMock")]
    public void DateFunctions_ShouldRespectCompatibility(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        var sqlVersion = ToSqlServerVersion(compatibilityLevel);

        Assert.Equal(new DateTime(2020, 2, 16), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATEADD(day, 1, '2020-02-15') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT DATEDIFF(day, '2020-01-01', '2020-01-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("February", ExecuteScalar(connection, "SELECT DATENAME(month, '2020-02-10') FROM Users WHERE Id = 1"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT DATEPART(month, '2020-02-10') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        if (sqlVersion < SqlServerDialect.FromPartsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DATEFROMPARTS(2020, 2, 29) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 60) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TIMEFROMPARTS(10, 11, 12, 1234567, 7) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT SMALLDATETIMEFROMPARTS(2020, 2, 29, 10, 11) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT EOMONTH('2020-02-15') FROM Users WHERE Id = 1"));
        }
        else
        {
            Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATEFROMPARTS(2020, 2, 29) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), Convert.ToDateTime(ExecuteScalar(connection, "SELECT DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            var offset = (DateTimeOffset)ExecuteScalar(connection, "SELECT DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 60) FROM Users WHERE Id = 1")!;
            Assert.Equal(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12).AddTicks(1234567 * 10L), TimeSpan.FromMinutes(60)), offset);
            Assert.Equal(new TimeSpan(10, 11, 12).Add(TimeSpan.FromTicks(1234567 * 10L)), Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT TIMEFROMPARTS(10, 11, 12, 1234567, 7) FROM Users WHERE Id = 1")));
            Assert.Equal(new DateTime(2020, 2, 29, 10, 11, 0), Convert.ToDateTime(ExecuteScalar(connection, "SELECT SMALLDATETIMEFROMPARTS(2020, 2, 29, 10, 11) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            Assert.Equal(new DateTime(2020, 2, 29), Convert.ToDateTime(ExecuteScalar(connection, "SELECT EOMONTH('2020-02-15') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        }

        Assert.Equal(new DateTimeOffset(new DateTime(2020, 2, 29, 10, 11, 12), TimeSpan.FromHours(2)), Assert.IsType<DateTimeOffset>(ExecuteScalar(connection, "SELECT TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00') FROM Users WHERE Id = 1")));
        Assert.Equal(new DateTimeOffset(new DateTime(2020, 2, 29, 9, 11, 12), TimeSpan.Zero), Assert.IsType<DateTimeOffset>(ExecuteScalar(connection, "SELECT SWITCHOFFSET('2020-02-29T10:11:12+01:00', '+00:00') FROM Users WHERE Id = 1")));

        if (sqlVersion < SqlServerDialect.DateDiffBigMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DATEDIFF_BIG(day, '2020-01-01', '2020-01-03') FROM Users WHERE Id = 1"));
            return;
        }

        Assert.Equal(2L, Convert.ToInt64(ExecuteScalar(connection, "SELECT DATEDIFF_BIG(day, '2020-01-01', '2020-01-03') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Azure metadata helpers follow compatibility-level rules.
    /// PT: Garante que helpers de metadados do SQL Azure sigam as regras de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade do SQL Azure em teste.</param>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    [Trait("Category", "SqlAzureMock")]
    public void MetadataFunctions_ShouldRespectCompatibility(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        var sqlVersion = ToSqlServerVersion(compatibilityLevel);
        connection.SetSessionContextValue("tenant_id", 42);
        connection.SetContextInfo([0x0A, 0x0B]);

        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT GETANSINULL() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar(connection, "SELECT DATALENGTH('AB') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT GROUPING(1) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT GROUPING_ID(1, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(new byte[] { 0x0A, 0x0B }, Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT CONTEXT_INFO() FROM Users WHERE Id = 1")));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT HOST_ID() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("localhost", ExecuteScalar(connection, "SELECT HOST_NAME() FROM Users WHERE Id = 1"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT IS_MEMBER('db_owner') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT IS_ROLEMEMBER('db_datareader') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT IS_SRVROLEMEMBER('sysadmin') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT ISDATE('2020-01-01') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT ISDATE('invalid') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(5, Convert.ToInt32(ExecuteScalar(connection, "SELECT PATINDEX('%Bob%', 'Ana Bob') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT PATINDEX('%Z%', 'Ana Bob') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        if (sqlVersion < SqlServerDialect.SessionContextMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT SESSION_CONTEXT(N'tenant_id') FROM Users WHERE Id = 1"));
            return;
        }

        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT SESSION_CONTEXT(N'tenant_id') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT SESSION_CONTEXT(N'missing') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Azure JSON helpers follow compatibility-level rules.
    /// PT: Garante que helpers JSON do SQL Azure sigam as regras de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade do SQL Azure em teste.</param>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    [Trait("Category", "SqlAzureMock")]
    public void JsonScalarFunctions_ShouldRespectCompatibility(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        var sqlVersion = ToSqlServerVersion(compatibilityLevel);

        if (sqlVersion < SqlServerDialect.JsonFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT ISJSON('{\"a\":1}') FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT JSON_MODIFY('{\"profile\":{\"active\":true,\"name\":\"Ana\"}}', '$.profile.name', 'Bia') FROM Users WHERE Id = 1"));
            return;
        }

        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT ISJSON('{\"a\":1}') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        Assert.Equal("{\"profile\":{\"active\":true,\"name\":\"Bia\"}}", ExecuteScalar(connection, "SELECT JSON_MODIFY('{\"profile\":{\"active\":true,\"name\":\"Ana\"}}', '$.profile.name', 'Bia') FROM Users WHERE Id = 1"));
    }

    /// <summary>
    /// EN: Ensures SQL Azure aggregate helpers follow compatibility-level rules.
    /// PT: Garante que helpers de agregacao do SQL Azure sigam as regras de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade do SQL Azure em teste.</param>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    [Trait("Category", "SqlAzureMock")]
    public void AggregateFunctions_ShouldRespectCompatibility(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        var sqlVersion = ToSqlServerVersion(compatibilityLevel);

        Assert.NotEqual(
            Convert.ToInt32(ExecuteScalar(connection, "SELECT CHECKSUM('Ana') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture),
            Convert.ToInt32(ExecuteScalar(connection, "SELECT CHECKSUM_AGG(Name) FROM Users"), CultureInfo.InvariantCulture));

        if (sqlVersion < SqlServerDialect.ApproxCountDistinctMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT(Name) FROM Users"));
            return;
        }

        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT APPROX_COUNT_DISTINCT(Name) FROM Users"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Azure versioned scalar helpers follow compatibility rules.
    /// PT: Garante que helpers escalares versionados do SQL Azure sigam as regras de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade do SQL Azure em teste.</param>
    [Theory]
    [MemberDataSqlAzureCompatibilityLevel]
    [Trait("Category", "SqlAzureMock")]
    public void VersionedScalarFunctions_ShouldRespectCompatibility(int compatibilityLevel)
    {
        using var connection = CreateOpenConnection(compatibilityLevel);
        var sqlVersion = ToSqlServerVersion(compatibilityLevel);

        if (sqlVersion < SqlServerDialect.FormatMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT FORMAT(42, 'D4') FROM Users WHERE Id = 1"));
        }
        else
        {
            Assert.Equal("0042", ExecuteScalar(connection, "SELECT FORMAT(42, 'D4') FROM Users WHERE Id = 1"));
        }

        if (sqlVersion < SqlServerDialect.ParseMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT PARSE('42' AS INT) FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TRY_CONVERT(INT, '42') FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TRY_PARSE('42' AS INT) FROM Users WHERE Id = 1"));
        }
        else
        {
            Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT PARSE('42' AS INT) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT TRY_CONVERT(INT, '42') FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
            Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT TRY_PARSE('42' AS INT) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
        }

        if (sqlVersion < SqlServerDialect.StringEscapeMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT STRING_ESCAPE('\\\"Ana\\nBob\\\"', 'json') FROM Users WHERE Id = 1"));
        }
        else
        {
            Assert.Equal("\\\"Ana\\nBob\\\"", ExecuteScalar(connection, "SELECT STRING_ESCAPE('\\\"Ana\\nBob\\\"', 'json') FROM Users WHERE Id = 1"));
        }

        if (sqlVersion < SqlServerDialect.TranslateMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT TRANSLATE('abc', 'ab', 'xy') FROM Users WHERE Id = 1"));
        }
        else
        {
            Assert.Equal("xyc", ExecuteScalar(connection, "SELECT TRANSLATE('abc', 'ab', 'xy') FROM Users WHERE Id = 1"));
        }

        if (sqlVersion < SqlServerDialect.CompressionFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT COMPRESS('Ana') FROM Users WHERE Id = 1"));
            Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT DECOMPRESS(COMPRESS('Ana')) FROM Users WHERE Id = 1"));
            return;
        }

        var compressed = Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT COMPRESS('Ana') FROM Users WHERE Id = 1"));
        Assert.NotEmpty(compressed);
        Assert.Equal(Encoding.Unicode.GetBytes("Ana"), Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT DECOMPRESS(COMPRESS('Ana')) FROM Users WHERE Id = 1")));
    }

    /// <summary>
    /// EN: Ensures SQL Azure math and error helpers return expected scalar values.
    /// PT: Garante que helpers matematicos e de erro do SQL Azure retornem valores escalares esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
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
    /// EN: Ensures SQL Azure SCOPE_IDENTITY returns the last identity value generated on the current connection scope.
    /// PT: Garante que SCOPE_IDENTITY do SQL Azure retorne o ultimo valor identity gerado no escopo atual da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
    public void ScopeIdentity_ShouldReturnLastGeneratedIdentityValue()
    {
        ExecuteNonQuery("INSERT INTO IdentityUsers (Name) VALUES ('Auto')");

        Assert.Equal(1, Convert.ToInt32(ExecuteScalar("SELECT SCOPE_IDENTITY() FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures SQL Azure ROWCOUNT_BIG() exposes the last row-count value as bigint.
    /// PT: Garante que ROWCOUNT_BIG() do SQL Azure exponha o ultimo valor de contagem de linhas como bigint.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlAzureMock")]
    public void RowCountBigFunction_ShouldReturnAffectedRows()
    {
        using var command = new SqlAzureCommandMock(_connection)
        {
            CommandText = "UPDATE Users SET Name = 'Bob Updated' WHERE Id = 2; SELECT ROWCOUNT_BIG();"
        };

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
    }

    private static SqlAzureConnectionMock CreateOpenConnection(int? compatibilityLevel = null)
    {
        var db = new SqlAzureDbMock(compatibilityLevel);
        db.AddTable("Users",
        [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true),
        ]);
        db.AddTable("IdentityUsers",
        [
            new("Id", DbType.Int32, false, identity: true),
            new("Name", DbType.String, false),
        ]);

        var connection = new SqlAzureConnectionMock(db);
        connection.Open();
        connection.AddProdecure("sp_ping", new ProcedureDef([], [], [], null));

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name, Email) VALUES (1, 'Ana', '{\"profile\":{\"active\":true,\"name\":\"Ana\"}}')");
        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bob', '{\"profile\":{\"active\":false,\"name\":\"Bob\"}}')");
        return connection;
    }

    private static int ToSqlServerVersion(int compatibilityLevel)
        => compatibilityLevel switch
        {
            SqlAzureDbCompatibilityLevels.SqlServer2008 => 2008,
            SqlAzureDbCompatibilityLevels.SqlServer2012 => 2012,
            SqlAzureDbCompatibilityLevels.SqlServer2014 => 2014,
            SqlAzureDbCompatibilityLevels.SqlServer2016 => 2016,
            SqlAzureDbCompatibilityLevels.SqlServer2017 => 2017,
            SqlAzureDbCompatibilityLevels.SqlServer2019 => 2019,
            SqlAzureDbCompatibilityLevels.SqlServer2022 => 2022,
            SqlAzureDbCompatibilityLevels.SqlServer2025 => 2025,
            _ => compatibilityLevel,
        };

    private object? ExecuteScalar(string sql)
        => ExecuteScalar(_connection, sql);

    private static object? ExecuteScalar(SqlAzureConnectionMock connection, string sql)
    {
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private static void ExecuteNonQuery(SqlAzureConnectionMock connection, string sql)
    {
        using var command = new SqlAzureCommandMock(connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }

    private void ExecuteNonQuery(string sql)
        => ExecuteNonQuery(_connection, sql);
}

