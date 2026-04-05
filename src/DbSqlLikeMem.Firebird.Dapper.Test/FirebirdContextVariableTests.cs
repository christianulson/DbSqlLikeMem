namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird context variables through the Dapper-facing mock surface.
/// PT: Cobre variaveis de contexto do Firebird pela surface simulada voltada para Dapper.
/// </summary>
public sealed class FirebirdContextVariableTests
{
    /// <summary>
    /// EN: Verifies Firebird context variables resolve through the Dapper-facing provider surface.
    /// PT: Verifica se as variaveis de contexto do Firebird sao resolvidas pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ContextVariables_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var currentProcess = System.Diagnostics.Process.GetCurrentProcess();

        Assert.Equal("SYSDBA", connection.QuerySingle<string>("SELECT CURRENT_USER FROM RDB$DATABASE"));
        Assert.Equal("SYSDBA", connection.QuerySingle<string>("SELECT USER FROM RDB$DATABASE"));
        Assert.Equal("NONE", connection.QuerySingle<string>("SELECT CURRENT_ROLE FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, connection.QuerySingle<string>("SELECT CURRENT_DATABASE FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, connection.QuerySingle<string>("SELECT CURRENT_CATALOG FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT CURRENT_CONNECTION FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT CURRENT_TRANSACTION FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT SESSION_ID FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT TRANSACTION_ID FROM RDB$DATABASE"));
        Assert.Equal("XNET", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'NETWORK_PROTOCOL') FROM RDB$DATABASE"));
        Assert.Equal(Environment.MachineName, connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_HOST') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PID') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_ADDRESS') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.ProcessName, connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PROCESS') FROM RDB$DATABASE"));
        Assert.Equal($"Firebird {(FirebirdDbVersions.Default / 10d):0.0}", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE"));
        Assert.Equal("SNAPSHOT TABLE STABILITY", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ISOLATION_LEVEL') FROM RDB$DATABASE"));
        connection.Execute("UPDATE Users SET Name = 'Ana2' WHERE Id = 999");
        Assert.Equal(0L, connection.QuerySingle<long>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ROW_COUNT') FROM RDB$DATABASE"));
        Assert.Null(connection.QuerySingle<object?>("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE"));
        Assert.Null(connection.QuerySingle<object?>("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_ENCRYPTED') FROM RDB$DATABASE"));
        Assert.Equal(DateTime.Now.Date, connection.QuerySingle<DateTime>("SELECT TODAY FROM RDB$DATABASE").Date);
        Assert.Equal(DateTime.Now.Date.AddDays(1), connection.QuerySingle<DateTime>("SELECT TOMORROW FROM RDB$DATABASE").Date);
        Assert.Equal(DateTime.Now.Date.AddDays(-1), connection.QuerySingle<DateTime>("SELECT YESTERDAY FROM RDB$DATABASE").Date);
        Assert.IsType<DateTime>(connection.QuerySingle<object>("SELECT NOW FROM RDB$DATABASE"));
        Assert.Equal("00000", connection.QuerySingle<string>("SELECT SQLSTATE FROM RDB$DATABASE"));
        Assert.Equal(0, connection.QuerySingle<int>("SELECT SQLCODE FROM RDB$DATABASE"));
        Assert.Equal(0, connection.QuerySingle<int>("SELECT GDSCODE FROM RDB$DATABASE"));
        Assert.False(connection.QuerySingle<bool>("SELECT RESETTING FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates EXTRACT against the supported date and timestamp fields through Dapper.
    /// PT: Verifica se o Firebird avalia EXTRACT sobre os campos suportados de data e timestamp via Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExtractDateAndTimestampFields_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(DateTime.Now.Year, connection.QuerySingle<int>("SELECT EXTRACT(YEAR FROM CURRENT_DATE) FROM RDB$DATABASE"));
        Assert.InRange(connection.QuerySingle<int>("SELECT EXTRACT(MONTH FROM CURRENT_DATE) FROM RDB$DATABASE"), 1, 12);
        Assert.InRange(connection.QuerySingle<int>("SELECT EXTRACT(DAY FROM CURRENT_DATE) FROM RDB$DATABASE"), 1, 31);
        Assert.InRange(connection.QuerySingle<int>("SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), 0, 23);
        Assert.InRange(connection.QuerySingle<int>("SELECT EXTRACT(MINUTE FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), 0, 59);
        Assert.InRange(connection.QuerySingle<int>("SELECT EXTRACT(SECOND FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), 0, 59);
        Assert.Equal(1, connection.QuerySingle<int>("SELECT EXTRACT(WEEK FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"));
        Assert.Equal(3, connection.QuerySingle<int>("SELECT EXTRACT(WEEKDAY FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"));
        Assert.Equal(2, connection.QuerySingle<int>("SELECT EXTRACT(YEARDAY FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"));
        Assert.Equal(789, connection.QuerySingle<int>("SELECT EXTRACT(MILLISECOND FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates DATEADD through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia DATEADD pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void DateAddTemporalFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var current = connection.QuerySingle<DateTime>("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE");
        var nextDay = connection.QuerySingle<DateTime>("SELECT DATEADD(1 DAY TO CURRENT_TIMESTAMP) FROM RDB$DATABASE");

        Assert.Equal(current.Date.AddDays(1), nextDay.Date);
        Assert.True(nextDay >= current);
    }

    /// <summary>
    /// EN: Verifies Firebird resolves RDB$GET_CONTEXT for the SYSTEM namespace through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird resolve RDB$GET_CONTEXT para o namespace SYSTEM pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void GetContextSystemNamespace_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var currentProcess = System.Diagnostics.Process.GetCurrentProcess();

        Assert.Equal("SYSDBA", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_USER') FROM RDB$DATABASE"));
        Assert.Equal("NONE", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_ROLE') FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_DATABASE') FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_CONNECTION') FROM RDB$DATABASE"));
        Assert.IsType<int>(connection.QuerySingle<int>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_TRANSACTION') FROM RDB$DATABASE"));
        Assert.Equal("XNET", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'NETWORK_PROTOCOL') FROM RDB$DATABASE"));
        Assert.Equal(Environment.MachineName, connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_HOST') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PID') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_ADDRESS') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.ProcessName, connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PROCESS') FROM RDB$DATABASE"));
        Assert.Equal($"Firebird {(FirebirdDbVersions.Default / 10d):0.0}", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE"));
        Assert.Equal("SNAPSHOT TABLE STABILITY", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ISOLATION_LEVEL') FROM RDB$DATABASE"));
        connection.Execute("UPDATE Users SET Name = 'Ana2' WHERE Id = 999");
        Assert.Equal(0L, connection.QuerySingle<long>("SELECT RDB$GET_CONTEXT('SYSTEM', 'ROW_COUNT') FROM RDB$DATABASE"));
        Assert.Null(connection.QuerySingle<object?>("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE"));
        Assert.Null(connection.QuerySingle<object?>("SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_ENCRYPTED') FROM RDB$DATABASE"));
        Assert.Equal(DateTime.Now.Date, connection.QuerySingle<DateTime>("SELECT TODAY FROM RDB$DATABASE").Date);
        Assert.Equal(DateTime.Now.Date.AddDays(1), connection.QuerySingle<DateTime>("SELECT TOMORROW FROM RDB$DATABASE").Date);
        Assert.Equal(DateTime.Now.Date.AddDays(-1), connection.QuerySingle<DateTime>("SELECT YESTERDAY FROM RDB$DATABASE").Date);
        Assert.IsType<DateTime>(connection.QuerySingle<object>("SELECT NOW FROM RDB$DATABASE"));
        Assert.Equal("00000", connection.QuerySingle<string>("SELECT SQLSTATE FROM RDB$DATABASE"));
        Assert.Equal(0, connection.QuerySingle<int>("SELECT SQLCODE FROM RDB$DATABASE"));
        Assert.Equal(0, connection.QuerySingle<int>("SELECT GDSCODE FROM RDB$DATABASE"));
        Assert.False(connection.QuerySingle<bool>("SELECT RESETTING FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird supports setting and reading user-session and user-transaction context variables through Dapper.
    /// PT: Verifica se o Firebird suporta gravar e ler variaveis de contexto user-session e user-transaction via Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void SetContextUserNamespaces_ShouldRoundTripValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var transaction = connection.BeginTransaction();

        Assert.Equal(0, connection.QuerySingle<int>("SELECT RDB$SET_CONTEXT('USER_SESSION', 'MyVar', 'hello') FROM RDB$DATABASE"));
        Assert.Equal("hello", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('USER_SESSION', 'MyVar') FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<int>("SELECT RDB$SET_CONTEXT('USER_SESSION', 'MyVar', 'world') FROM RDB$DATABASE"));
        Assert.Equal("world", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('USER_SESSION', 'MyVar') FROM RDB$DATABASE"));

        Assert.Equal(0, connection.QuerySingle<int>("SELECT RDB$SET_CONTEXT('USER_TRANSACTION', 'TxnVar', 'abc') FROM RDB$DATABASE"));
        Assert.Equal("abc", connection.QuerySingle<string>("SELECT RDB$GET_CONTEXT('USER_TRANSACTION', 'TxnVar') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates supported conditional functions through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia as funcoes condicionais suportadas pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ConditionalFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("yes", connection.QuerySingle<string>("SELECT IIF(1 = 1, 'yes', 'no') FROM RDB$DATABASE"));
        Assert.Equal(2, connection.QuerySingle<int>("SELECT DECODE('b', 'a', 1, 'b', 2, 0) FROM RDB$DATABASE"));
        Assert.Null(connection.QuerySingle<int?>("SELECT NULLIF(1, 1) FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<int>("SELECT NULLIF(1, 2) FROM RDB$DATABASE"));
        Assert.Equal("fallback", connection.QuerySingle<string>("SELECT COALESCE(NULL, 'fallback') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates MAXVALUE and MINVALUE through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia MAXVALUE e MINVALUE pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void MinMaxValueFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(9, connection.QuerySingle<int>("SELECT MAXVALUE(1, 9, 4) FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<int>("SELECT MINVALUE(1, 9, 4) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates ASCII_CHAR and ASCII_VAL through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia ASCII_CHAR e ASCII_VAL pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void AsciiCharacterFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("A", connection.QuerySingle<string>("SELECT CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal("A", connection.QuerySingle<string>("SELECT NCHAR(65) FROM RDB$DATABASE"));
        Assert.Equal("A", connection.QuerySingle<string>("SELECT ASCII_CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal(65, connection.QuerySingle<int>("SELECT ASCII_VAL('A') FROM RDB$DATABASE"));
        Assert.Equal("A", connection.QuerySingle<string>("SELECT UNICODE_CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal(65, connection.QuerySingle<int>("SELECT UNICODE_VAL('A') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird converts between ASCII UUID text and binary UUID values through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird converte entre texto ASCII de UUID e valores binarios de UUID pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void UuidConversionFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var uuidText = "A0bF4E45-3029-2a44-D493-4998c9b439A3";
        var uuidBytes = connection.QuerySingle<byte[]>($"SELECT CHAR_TO_UUID('{uuidText}') FROM RDB$DATABASE");
        Assert.Equal(16, uuidBytes.Length);
        Assert.Equal(uuidText.ToUpperInvariant(), connection.QuerySingle<string>("SELECT UUID_TO_CHAR(CHAR_TO_UUID('A0bF4E45-3029-2a44-D493-4998c9b439A3')) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates MD5, CRYPT_HASH, HEX, UNHEX, HEX_ENCODE, HEX_DECODE, BASE64_ENCODE, and BASE64_DECODE through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia MD5, CRYPT_HASH, HEX, UNHEX, HEX_ENCODE, HEX_DECODE, BASE64_ENCODE e BASE64_DECODE pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void BinaryTextFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using var md5 = System.Security.Cryptography.MD5.Create();
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var expectedMd5 = ToLowerHex(md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird")));
        var expectedCryptHashMd5 = md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        var expectedCryptHashSha256 = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        var expectedBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        Assert.Equal(expectedMd5, connection.QuerySingle<string>("SELECT MD5('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(expectedCryptHashMd5, connection.QuerySingle<byte[]>("SELECT CRYPT_HASH('Firebird' USING MD5) FROM RDB$DATABASE"));
        Assert.Equal(expectedCryptHashSha256, connection.QuerySingle<byte[]>("SELECT CRYPT_HASH('Firebird' USING SHA256) FROM RDB$DATABASE"));
        Assert.Equal("4669726562697264", connection.QuerySingle<string>("SELECT HEX('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), connection.QuerySingle<byte[]>("SELECT UNHEX('4669726562697264') FROM RDB$DATABASE"));
        Assert.Equal("4669726562697264", connection.QuerySingle<string>("SELECT HEX_ENCODE('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), connection.QuerySingle<byte[]>("SELECT HEX_DECODE('4669726562697264') FROM RDB$DATABASE"));
        Assert.Equal(expectedBase64, connection.QuerySingle<string>("SELECT BASE64_ENCODE('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), connection.QuerySingle<byte[]>("SELECT BASE64_DECODE('RmlyZWJpcmQ=') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird generates a 16-byte UUID value through GEN_UUID via the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird gera um valor UUID de 16 bytes via GEN_UUID pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void GenUuidFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var uuidBytes = connection.QuerySingle<byte[]>("SELECT GEN_UUID() FROM RDB$DATABASE");
        Assert.Equal(16, uuidBytes.Length);
        Assert.Equal(16, FromHexString(ToLowerHex(uuidBytes)).Length);
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates HASH through the Dapper-facing provider surface, including the CRC32 variant.
    /// PT: Verifica se o Firebird avalia HASH pela surface do provedor voltada para Dapper, incluindo a variante CRC32.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void HashFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var hash1 = connection.QuerySingle<long>("SELECT HASH('Firebird') FROM RDB$DATABASE");
        var hash2 = connection.QuerySingle<long>("SELECT HASH('Firebird') FROM RDB$DATABASE");
        var crc32 = connection.QuerySingle<int>("SELECT HASH('Firebird' USING CRC32) FROM RDB$DATABASE");
        Assert.Equal(hash1, hash2);
        Assert.NotEqual(0L, hash1);
        Assert.Null(connection.QuerySingle<long?>("SELECT HASH(NULL) FROM RDB$DATABASE"));
        Assert.Equal(unchecked((int)ComputeCrc32(System.Text.Encoding.UTF8.GetBytes("Firebird"))), crc32);
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates RAND through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia RAND pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void RandFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var value = connection.QuerySingle<double>("SELECT RAND() FROM RDB$DATABASE");
        Assert.InRange(value, 0d, 1d);
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates POSITION, REPLACE, and REVERSE through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia POSITION, REPLACE e REVERSE pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void StringSearchAndTransformFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(4, connection.QuerySingle<int>("SELECT POSITION('be', 'To be or not to be') FROM RDB$DATABASE"));
        Assert.Equal(4, connection.QuerySingle<int>("SELECT LOCATE('be', 'To be or not to be') FROM RDB$DATABASE"));
        Assert.Equal(17, connection.QuerySingle<int>("SELECT LOCATE('be', 'To be or not to be', 10) FROM RDB$DATABASE"));
        Assert.Equal("axcaxc", connection.QuerySingle<string>("SELECT REPLACE('abcabc', 'b', 'x') FROM RDB$DATABASE"));
        Assert.Equal("cba", connection.QuerySingle<string>("SELECT REVERSE('abc') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates bitwise binary helper functions through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia as funcoes auxiliares binarias bitwise pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void BinaryBitwiseFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(4, connection.QuerySingle<long>("SELECT BIN_AND(6, 5) FROM RDB$DATABASE"));
        Assert.Equal(7, connection.QuerySingle<long>("SELECT BIN_OR(6, 5) FROM RDB$DATABASE"));
        Assert.Equal(3, connection.QuerySingle<long>("SELECT BIN_XOR(6, 5) FROM RDB$DATABASE"));
        Assert.Equal(-7, connection.QuerySingle<long>("SELECT BIN_NOT(6) FROM RDB$DATABASE"));
        Assert.Equal(12, connection.QuerySingle<long>("SELECT BIN_SHL(3, 2) FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<long>("SELECT BIN_SHR(2, 1) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates BIT_LENGTH and OCTET_LENGTH through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia BIT_LENGTH e OCTET_LENGTH pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void StringLengthFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(8, connection.QuerySingle<int>("SELECT CHAR_LENGTH('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(8, connection.QuerySingle<int>("SELECT CHARACTER_LENGTH('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(16, connection.QuerySingle<int>("SELECT BIT_LENGTH('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(8, connection.QuerySingle<int>("SELECT OCTET_LENGTH('Firebird') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates OVERLAY through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia OVERLAY pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void OverlayStringFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("abXYZef", connection.QuerySingle<string>("SELECT OVERLAY('abcdef' PLACING 'XYZ' FROM 3 FOR 2) FROM RDB$DATABASE"));
        Assert.Equal("abXYZf", connection.QuerySingle<string>("SELECT OVERLAY('abcdef' PLACING 'XYZ' FROM 3) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates common string transform functions through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia funcoes comuns de transformacao de string pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void StringTransformFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("firebird", connection.QuerySingle<string>("SELECT LOWER('FireBird') FROM RDB$DATABASE"));
        Assert.Equal("FIREBIRD", connection.QuerySingle<string>("SELECT UPPER('FireBird') FROM RDB$DATABASE"));
        Assert.Equal("abc", connection.QuerySingle<string>("SELECT TRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("abc  ", connection.QuerySingle<string>("SELECT LTRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("  abc", connection.QuerySingle<string>("SELECT RTRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("bcd", connection.QuerySingle<string>("SELECT SUBSTRING('abcdef' FROM 2 FOR 3) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates REPEAT and TRANSLATE through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia REPEAT e TRANSLATE pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void RepeatAndTranslateFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("ababab", connection.QuerySingle<string>("SELECT REPEAT('ab', 3) FROM RDB$DATABASE"));
        Assert.Equal("FIrEbIrd", connection.QuerySingle<string>("SELECT TRANSLATE('Firebird', 'ie', 'IE') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates SPACE, LEFT, RIGHT, LPAD, and RPAD through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia SPACE, LEFT, RIGHT, LPAD e RPAD pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void StringPaddingFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal("   ", connection.QuerySingle<string>("SELECT SPACE(3) FROM RDB$DATABASE"));
        Assert.Equal("Fire", connection.QuerySingle<string>("SELECT LEFT('Firebird', 4) FROM RDB$DATABASE"));
        Assert.Equal("bird", connection.QuerySingle<string>("SELECT RIGHT('Firebird', 4) FROM RDB$DATABASE"));
        Assert.Equal("xxFirebird", connection.QuerySingle<string>("SELECT LPAD('Firebird', 10, 'x') FROM RDB$DATABASE"));
        Assert.Equal("Firebirdxx", connection.QuerySingle<string>("SELECT RPAD('Firebird', 10, 'x') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates common numeric helper functions through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia funcoes auxiliares numericas comuns pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void NumericHelperFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(5, connection.QuerySingle<int>("SELECT ABS(-5) FROM RDB$DATABASE"));
        Assert.Equal(5, connection.QuerySingle<int>("SELECT ABSVAL(-5) FROM RDB$DATABASE"));
        Assert.Equal(2, connection.QuerySingle<int>("SELECT CEIL(1.2) FROM RDB$DATABASE"));
        Assert.Equal(2, connection.QuerySingle<int>("SELECT CEILING(1.2) FROM RDB$DATABASE"));
        Assert.Equal(180d, connection.QuerySingle<double>("SELECT DEGREES(PI()) FROM RDB$DATABASE"), 12);
        Assert.Equal(9, connection.QuerySingle<int>("SELECT GREATEST(1, 9, 4) FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<int>("SELECT LEAST(1, 9, 4) FROM RDB$DATABASE"));
        Assert.Equal("110", connection.QuerySingle<string>("SELECT BIN(6) FROM RDB$DATABASE"));
        Assert.Equal(1, connection.QuerySingle<int>("SELECT MOD(7, 3) FROM RDB$DATABASE"));
        Assert.Equal(8d, connection.QuerySingle<double>("SELECT POW(2, 3) FROM RDB$DATABASE"), 12);
        Assert.Equal(8d, connection.QuerySingle<double>("SELECT POWER(2, 3) FROM RDB$DATABASE"), 12);
        Assert.Equal(Math.PI, connection.QuerySingle<double>("SELECT PI() FROM RDB$DATABASE"), 12);
        Assert.Equal(Math.PI, connection.QuerySingle<double>("SELECT RADIANS(180) FROM RDB$DATABASE"), 12);
        Assert.Equal(1, connection.QuerySingle<int>("SELECT FLOOR(1.8) FROM RDB$DATABASE"));
        Assert.Equal(-1, connection.QuerySingle<int>("SELECT SIGN(-3) FROM RDB$DATABASE"));
        Assert.Equal(3, connection.QuerySingle<int>("SELECT SQRT(9) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates TRUNC through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia TRUNC pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void TruncFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(123m, connection.QuerySingle<decimal>("SELECT TRUNC(123.456) FROM RDB$DATABASE"));
        Assert.Equal(123.45m, connection.QuerySingle<decimal>("SELECT TRUNC(123.456, 2) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates ROUND through the Dapper-facing provider surface using the Firebird rounding rule.
    /// PT: Verifica se o Firebird avalia ROUND pela surface do provedor voltada para Dapper usando a regra de arredondamento do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void RoundFunction_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(2m, connection.QuerySingle<decimal>("SELECT ROUND(1.5) FROM RDB$DATABASE"));
        Assert.Equal(1.23m, connection.QuerySingle<decimal>("SELECT ROUND(1.234, 2) FROM RDB$DATABASE"));
        Assert.Equal(-2m, connection.QuerySingle<decimal>("SELECT ROUND(-1.5) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Verifies Firebird evaluates common transcendental numeric helpers through the Dapper-facing provider surface.
    /// PT: Verifica se o Firebird avalia funcoes numericas transcendentais comuns pela surface do provedor voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void TranscendentalNumericFunctions_ShouldReturnExpectedValues_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Equal(0d, connection.QuerySingle<double>("SELECT ACOS(1) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT ASIN(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT ATAN(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(Math.PI / 4d, connection.QuerySingle<double>("SELECT ATAN2(1, 1) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d, connection.QuerySingle<double>("SELECT COS(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d, connection.QuerySingle<double>("SELECT COSH(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d / Math.Tan(1d), connection.QuerySingle<double>("SELECT COT(1) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d, connection.QuerySingle<double>("SELECT EXP(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d, connection.QuerySingle<double>("SELECT LN(EXP(1)) FROM RDB$DATABASE"), 12);
        Assert.Equal(2d, connection.QuerySingle<double>("SELECT LOG(10, 100) FROM RDB$DATABASE"), 12);
        Assert.Equal(2d, connection.QuerySingle<double>("SELECT LOG10(100) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT SIN(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT SINH(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT TAN(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT TANH(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(1d, connection.QuerySingle<double>("SELECT ACOSH(1) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT ASINH(0) FROM RDB$DATABASE"), 12);
        Assert.Equal(0d, connection.QuerySingle<double>("SELECT ATANH(0) FROM RDB$DATABASE"), 12);
    }

    /// <summary>
    /// EN: Verifies Firebird trigger context variables follow the active trigger event through Dapper.
    /// PT: Verifica se as variáveis de contexto de trigger do Firebird seguem o evento ativo via Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void TriggerContextVariables_ShouldReflectActiveTriggerEvent_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var insertingSeen = false;
        var updatingSeen = false;
        var deletingSeen = false;

        var table = Assert.IsAssignableFrom<TableMock>(connection.Db.GetTable("Users"));
        table.AddTrigger(TableTriggerEvent.BeforeInsert, _ => insertingSeen = connection.QuerySingle<bool>("SELECT INSERTING FROM RDB$DATABASE"));
        table.AddTrigger(TableTriggerEvent.BeforeUpdate, _ => updatingSeen = connection.QuerySingle<bool>("SELECT UPDATING FROM RDB$DATABASE"));
        table.AddTrigger(TableTriggerEvent.BeforeDelete, _ => deletingSeen = connection.QuerySingle<bool>("SELECT DELETING FROM RDB$DATABASE"));

        connection.Execute("INSERT INTO Users (Id, Name) VALUES (3, 'Carol')");
        connection.Execute("UPDATE Users SET Name = 'Carolyn' WHERE Id = 3");
        connection.Execute("DELETE FROM Users WHERE Id = 3");

        Assert.True(insertingSeen);
        Assert.True(updatingSeen);
        Assert.True(deletingSeen);
    }

    /// <summary>
    /// EN: Converts binary data to a lowercase hexadecimal string.
    /// PT: Converte dados binarios para uma string hexadecimal em minusculas.
    /// </summary>
    private static string ToLowerHex(byte[] bytes)
    {
        var chars = new char[bytes.Length * 2];
        const string hex = "0123456789abcdef";
        for (var i = 0; i < bytes.Length; i++)
        {
            var value = bytes[i];
            chars[i * 2] = hex[value >> 4];
            chars[i * 2 + 1] = hex[value & 0xF];
        }
        return new string(chars);
    }

    /// <summary>
    /// EN: Converts a hexadecimal string to binary data.
    /// PT: Converte uma string hexadecimal para dados binarios.
    /// </summary>
    private static byte[] FromHexString(string hex)
    {
        if (hex.Length % 2 != 0)
            throw new FormatException("Hex string must have an even length.");

        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        return bytes;
    }

    /// <summary>
    /// EN: Computes the CRC32 hash used by the Firebird HASH CRC32 variant in tests.
    /// PT: Computa o hash CRC32 usado pela variante CRC32 do HASH do Firebird nos testes.
    /// </summary>
    private static uint ComputeCrc32(byte[] bytes)
    {
        var crc = uint.MaxValue;
        foreach (var b in bytes)
        {
            var index = (crc ^ b) & 0xFF;
            crc = (crc >> 8) ^ Crc32Table.Value[index];
        }

        return crc ^ uint.MaxValue;
    }

    /// <summary>
    /// EN: Stores the lookup table used to compute CRC32 values for Firebird HASH tests.
    /// PT: Armazena a tabela de consulta usada para computar valores CRC32 nos testes de HASH do Firebird.
    /// </summary>
    private static readonly Lazy<uint[]> Crc32Table = new(static () =>
    {
        var table = new uint[256];
        for (var i = 0; i < table.Length; i++)
        {
            var crc = (uint)i;
            for (var bit = 0; bit < 8; bit++)
            {
                crc = (crc & 1) != 0
                    ? (crc >> 1) ^ 0xEDB88320u
                    : crc >> 1;
            }

            table[i] = crc;
        }

        return table;
    });
}
