namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Validates Firebird function execution for the supported scalar FUNCTION DDL subset.
/// PT: Valida a execucao de funcoes Firebird para o subset suportado de DDL de FUNCTION escalar.
/// </summary>
public sealed class FirebirdFunctionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures Firebird executes the supported scalar FUNCTION DDL subset end to end.
    /// PT: Garante que o Firebird execute end-to-end o subset suportado de DDL de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [MemberDataByFirebirdVersion(nameof(FirebirdVersions))]
    [Trait("Category", "FirebirdMock")]
    public void ScalarFunctionDdlSubset_ShouldExecuteEndToEnd(int version)
    {
        using var connection = CreateOpenConnection(version);

        var createSql = "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END";
        if (version < FirebirdDialect.FunctionDdlMinVersion)
        {
            var notSupported = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(connection, createSql));
            Assert.Contains("CREATE FUNCTION", notSupported.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        ExecuteNonQuery(connection, createSql);

        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        ExecuteNonQuery(connection, "DROP FUNCTION IF EXISTS fn_users");

        var ex = Assert.Throws<NotSupportedException>(() => ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"));
        Assert.Contains("FN_USERS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Firebird uses a default scalar function parameter when the caller omits the trailing argument.
    /// PT: Garante que o Firebird use um parametro padrao de funcao escalar quando o chamador omite o argumento final.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [MemberDataByFirebirdVersion(nameof(FirebirdVersions))]
    [Trait("Category", "FirebirdMock")]
    public void ScalarFunctionDdlSubset_ShouldUseDefaultParameterValue(int version)
    {
        using var connection = CreateOpenConnection(version);

        var createSql = "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT DEFAULT 2) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END";
        if (version < FirebirdDialect.FunctionDdlMinVersion)
        {
            var notSupported = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(connection, createSql));
            Assert.Contains("CREATE FUNCTION", notSupported.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        ExecuteNonQuery(connection, createSql);

        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird replaces an existing scalar function body through CREATE OR REPLACE FUNCTION only when supported.
    /// PT: Garante que o Firebird substitua o corpo de uma funcao escalar existente com CREATE OR REPLACE FUNCTION somente quando suportado.
    /// </summary>
    /// <param name="version">EN: Firebird dialect version under test. PT: Versao do dialeto Firebird em teste.</param>
    [Theory]
    [MemberDataByFirebirdVersion(nameof(FirebirdVersions))]
    [Trait("Category", "FirebirdMock")]
    public void CreateOrReplaceScalarFunctionDdlSubset_ShouldReplaceExistingBody(int version)
    {
        using var connection = CreateOpenConnection(version);

        if (version < FirebirdDialect.FunctionDdlMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(connection, "CREATE OR REPLACE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END"));
            Assert.Contains("CREATE OR REPLACE FUNCTION", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        ExecuteNonQuery(connection, "CREATE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END");
        Assert.Equal(42, Convert.ToInt32(ExecuteScalar(connection, "SELECT fn_users(40, 2) FROM Users WHERE Id = 1"), CultureInfo.InvariantCulture));

        var notSupported = Assert.Throws<NotSupportedException>(() => ExecuteNonQuery(connection, "CREATE OR REPLACE FUNCTION fn_users(baseValue INT, incrementValue INT) RETURNS INT AS BEGIN RETURN baseValue + incrementValue + 1; END"));
        Assert.Contains("CREATE OR REPLACE FUNCTION", notSupported.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies GEN_ID follows the Firebird sequence alias semantics through the scalar surface.
    /// PT: Verifica se GEN_ID segue a semantica de alias de sequence do Firebird pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void GenIdSequenceFunction_ShouldFollowFirebirdSemantics()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        ExecuteNonQuery(connection, "CREATE SEQUENCE seq_gen START WITH 10 INCREMENT BY 10");

        Assert.Equal(1, Convert.ToInt64(ExecuteScalar(connection, "SELECT GEN_ID(seq_gen, 1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt64(ExecuteScalar(connection, "SELECT GEN_ID(seq_gen, 0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(11, Convert.ToInt64(ExecuteScalar(connection, "SELECT NEXT VALUE FOR seq_gen FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird returns expected values for the supported temporal system functions.
    /// PT: Garante que o Firebird retorne valores esperados para as funcoes temporais de sistema suportadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void SystemTemporalFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var currentDate = Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT_DATE FROM RDB$DATABASE"));
        Assert.Equal(DateTime.Now.Date, currentDate.Date);

        Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT CURRENT_TIME FROM RDB$DATABASE"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE"));
        Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT LOCALTIME FROM RDB$DATABASE"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT LOCALTIMESTAMP FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates EXTRACT against the supported date and timestamp fields.
    /// PT: Garante que o Firebird avalie EXTRACT sobre os campos suportados de data e timestamp.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ExtractDateAndTimestampFields_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);
        var now = DateTime.Now;

        Assert.Equal(now.Year, Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(YEAR FROM CURRENT_DATE) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.InRange(Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(MONTH FROM CURRENT_DATE) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 1, 12);
        Assert.InRange(Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(DAY FROM CURRENT_DATE) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 1, 31);
        Assert.InRange(Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(HOUR FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 0, 23);
        Assert.InRange(Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(MINUTE FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 0, 59);
        Assert.InRange(Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(SECOND FROM CURRENT_TIMESTAMP) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 0, 59);
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(WEEK FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(WEEKDAY FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(YEARDAY FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(789, Convert.ToInt32(ExecuteScalar(connection, "SELECT EXTRACT(MILLISECOND FROM CAST('2024-01-03 12:34:56.789' AS TIMESTAMP)) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates supported conditional functions through the scalar surface.
    /// PT: Garante que o Firebird avalie as funcoes condicionais suportadas pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ConditionalFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("yes", ExecuteScalar(connection, "SELECT IIF(1 = 1, 'yes', 'no') FROM RDB$DATABASE"));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT DECODE('b', 'a', 1, 'b', 2, 0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT NULLIF(1, 1) FROM RDB$DATABASE"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT NULLIF(1, 2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("fallback", ExecuteScalar(connection, "SELECT COALESCE(NULL, 'fallback') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates MAXVALUE and MINVALUE through the scalar surface.
    /// PT: Garante que o Firebird avalie MAXVALUE e MINVALUE pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void MinMaxValueFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(9, Convert.ToInt32(ExecuteScalar(connection, "SELECT MAXVALUE(1, 9, 4) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT MINVALUE(1, 9, 4) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates ASCII_CHAR and ASCII_VAL through the scalar surface.
    /// PT: Garante que o Firebird avalie ASCII_CHAR e ASCII_VAL pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AsciiCharacterFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("A", ExecuteScalar(connection, "SELECT CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal("A", ExecuteScalar(connection, "SELECT NCHAR(65) FROM RDB$DATABASE"));
        Assert.Equal("A", ExecuteScalar(connection, "SELECT ASCII_CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar(connection, "SELECT ASCII_VAL('A') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("A", ExecuteScalar(connection, "SELECT UNICODE_CHAR(65) FROM RDB$DATABASE"));
        Assert.Equal(65, Convert.ToInt32(ExecuteScalar(connection, "SELECT UNICODE_VAL('A') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird converts between ASCII UUID text and binary UUID values.
    /// PT: Garante que o Firebird converta entre texto ASCII de UUID e valores binarios de UUID.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void UuidConversionFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var uuidText = "A0bF4E45-3029-2a44-D493-4998c9b439A3";
        var uuidBytes = Assert.IsType<byte[]>(ExecuteScalar(connection, $"SELECT CHAR_TO_UUID('{uuidText}') FROM RDB$DATABASE"));
        Assert.Equal(16, uuidBytes.Length);
        Assert.Equal(uuidText.ToUpperInvariant(), ExecuteScalar(connection, "SELECT UUID_TO_CHAR(CHAR_TO_UUID('A0bF4E45-3029-2a44-D493-4998c9b439A3')) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates MD5, CRYPT_HASH, HEX, UNHEX, HEX_ENCODE, HEX_DECODE, BASE64_ENCODE, and BASE64_DECODE through the scalar surface.
    /// PT: Garante que o Firebird avalie MD5, CRYPT_HASH, HEX, UNHEX, HEX_ENCODE, HEX_DECODE, BASE64_ENCODE e BASE64_DECODE pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void BinaryTextFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        using var md5 = System.Security.Cryptography.MD5.Create();
        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var expectedMd5 = ToHexString(md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird")));
        var expectedCryptHashMd5 = md5.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        var expectedCryptHashSha256 = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        var expectedBase64 = Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        Assert.Equal(expectedMd5, ExecuteScalar(connection, "SELECT MD5('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(expectedCryptHashMd5, Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT CRYPT_HASH('Firebird' USING MD5) FROM RDB$DATABASE")));
        Assert.Equal(expectedCryptHashSha256, Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT CRYPT_HASH('Firebird' USING SHA256) FROM RDB$DATABASE")));
        Assert.Equal("4669726562697264", ExecuteScalar(connection, "SELECT HEX('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT UNHEX('4669726562697264') FROM RDB$DATABASE")));
        Assert.Equal("4669726562697264", ExecuteScalar(connection, "SELECT HEX_ENCODE('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT HEX_DECODE('4669726562697264') FROM RDB$DATABASE")));
        Assert.Equal(expectedBase64, ExecuteScalar(connection, "SELECT BASE64_ENCODE('Firebird') FROM RDB$DATABASE"));
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes("Firebird"), Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT BASE64_DECODE('RmlyZWJpcmQ=') FROM RDB$DATABASE")));
    }

    /// <summary>
    /// EN: Ensures Firebird generates a 16-byte UUID value through GEN_UUID.
    /// PT: Garante que o Firebird gere um valor UUID de 16 bytes via GEN_UUID.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void GenUuidFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var uuidBytes = Assert.IsType<byte[]>(ExecuteScalar(connection, "SELECT GEN_UUID() FROM RDB$DATABASE"));
        Assert.Equal(16, uuidBytes.Length);
        Assert.Equal(uuidBytes, FromHexString(ToHexString(uuidBytes)));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates HASH through the scalar surface, including the CRC32 variant, and keeps the result stable.
    /// PT: Garante que o Firebird avalie HASH pela surface escalar, incluindo a variante CRC32, e mantenha o resultado estavel.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void HashFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var hash1 = Convert.ToInt64(ExecuteScalar(connection, "SELECT HASH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var hash2 = Convert.ToInt64(ExecuteScalar(connection, "SELECT HASH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var crc32 = Convert.ToInt32(ExecuteScalar(connection, "SELECT HASH('Firebird' USING CRC32) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        Assert.Equal(hash1, hash2);
        Assert.NotEqual(0L, hash1);
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT HASH(NULL) FROM RDB$DATABASE"));
        Assert.Equal(unchecked((int)ComputeCrc32(System.Text.Encoding.UTF8.GetBytes("Firebird"))), crc32);
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates RAND through the scalar surface.
    /// PT: Garante que o Firebird avalie RAND pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void RandFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var value = Convert.ToDouble(ExecuteScalar(connection, "SELECT RAND() FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        Assert.InRange(value, 0d, 1d);
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates POSITION, REPLACE, and REVERSE through the scalar surface.
    /// PT: Garante que o Firebird avalie POSITION, REPLACE e REVERSE pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void StringSearchAndTransformFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(4, Convert.ToInt32(ExecuteScalar(connection, "SELECT POSITION('be', 'To be or not to be') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(4, Convert.ToInt32(ExecuteScalar(connection, "SELECT LOCATE('be', 'To be or not to be') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(17, Convert.ToInt32(ExecuteScalar(connection, "SELECT LOCATE('be', 'To be or not to be', 10) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("axcaxc", ExecuteScalar(connection, "SELECT REPLACE('abcabc', 'b', 'x') FROM RDB$DATABASE"));
        Assert.Equal("cba", ExecuteScalar(connection, "SELECT REVERSE('abc') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates bitwise binary helper functions through the scalar surface.
    /// PT: Garante que o Firebird avalie as funcoes auxiliares binarias bitwise pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void BinaryBitwiseFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(4, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_AND(6, 5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(7, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_OR(6, 5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_XOR(6, 5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(-7, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_NOT(6) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(12, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_SHL(3, 2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt64(ExecuteScalar(connection, "SELECT BIN_SHR(2, 1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates BIT_LENGTH and OCTET_LENGTH through the scalar surface.
    /// PT: Garante que o Firebird avalie BIT_LENGTH e OCTET_LENGTH pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void StringLengthFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(8, Convert.ToInt32(ExecuteScalar(connection, "SELECT CHAR_LENGTH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(8, Convert.ToInt32(ExecuteScalar(connection, "SELECT CHARACTER_LENGTH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(64, Convert.ToInt32(ExecuteScalar(connection, "SELECT BIT_LENGTH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(8, Convert.ToInt32(ExecuteScalar(connection, "SELECT OCTET_LENGTH('Firebird') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates OVERLAY through the scalar surface using the supported syntax.
    /// PT: Garante que o Firebird avalie OVERLAY pela surface escalar usando a sintaxe suportada.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void OverlayStringFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("abXYZef", ExecuteScalar(connection, "SELECT OVERLAY('abcdef' PLACING 'XYZ' FROM 3 FOR 2) FROM RDB$DATABASE"));
        Assert.Equal("abXYZf", ExecuteScalar(connection, "SELECT OVERLAY('abcdef' PLACING 'XYZ' FROM 3) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates common string transform functions through the scalar surface.
    /// PT: Garante que o Firebird avalie funcoes comuns de transformacao de string pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void StringTransformFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("firebird", ExecuteScalar(connection, "SELECT LOWER('FireBird') FROM RDB$DATABASE"));
        Assert.Equal("FIREBIRD", ExecuteScalar(connection, "SELECT UPPER('FireBird') FROM RDB$DATABASE"));
        Assert.Equal("abc", ExecuteScalar(connection, "SELECT TRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("abc  ", ExecuteScalar(connection, "SELECT LTRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("  abc", ExecuteScalar(connection, "SELECT RTRIM('  abc  ') FROM RDB$DATABASE"));
        Assert.Equal("bcd", ExecuteScalar(connection, "SELECT SUBSTRING('abcdef' FROM 2 FOR 3) FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates REPEAT and TRANSLATE through the scalar surface.
    /// PT: Garante que o Firebird avalie REPEAT e TRANSLATE pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void RepeatAndTranslateFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("ababab", ExecuteScalar(connection, "SELECT REPEAT('ab', 3) FROM RDB$DATABASE"));
        Assert.Equal("FIrEbIrd", ExecuteScalar(connection, "SELECT TRANSLATE('Firebird', 'ie', 'IE') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates SPACE, LEFT, RIGHT, LPAD, and RPAD through the scalar surface.
    /// PT: Garante que o Firebird avalie SPACE, LEFT, RIGHT, LPAD e RPAD pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void StringPaddingFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal("   ", ExecuteScalar(connection, "SELECT SPACE(3) FROM RDB$DATABASE"));
        Assert.Equal("Fire", ExecuteScalar(connection, "SELECT LEFT('Firebird', 4) FROM RDB$DATABASE"));
        Assert.Equal("bird", ExecuteScalar(connection, "SELECT RIGHT('Firebird', 4) FROM RDB$DATABASE"));
        Assert.Equal("xxFirebird", ExecuteScalar(connection, "SELECT LPAD('Firebird', 10, 'x') FROM RDB$DATABASE"));
        Assert.Equal("Firebirdxx", ExecuteScalar(connection, "SELECT RPAD('Firebird', 10, 'x') FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates common numeric helper functions through the scalar surface.
    /// PT: Garante que o Firebird avalie funcoes auxiliares numericas comuns pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void NumericHelperFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(5, Convert.ToInt32(ExecuteScalar(connection, "SELECT ABS(-5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(5, Convert.ToInt32(ExecuteScalar(connection, "SELECT ABSVAL(-5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT CEIL(1.2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(ExecuteScalar(connection, "SELECT CEILING(1.2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(180d, Convert.ToDouble(ExecuteScalar(connection, "SELECT DEGREES(PI()) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(9, Convert.ToInt32(ExecuteScalar(connection, "SELECT GREATEST(1, 9, 4) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT LEAST(1, 9, 4) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("110", ExecuteScalar(connection, "SELECT BIN(6) FROM RDB$DATABASE"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT MOD(7, 3) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(8d, Convert.ToDouble(ExecuteScalar(connection, "SELECT POW(2, 3) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(8d, Convert.ToDouble(ExecuteScalar(connection, "SELECT POWER(2, 3) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar(connection, "SELECT PI() FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI, Convert.ToDouble(ExecuteScalar(connection, "SELECT RADIANS(180) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT FLOOR(1.8) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(-1, Convert.ToInt32(ExecuteScalar(connection, "SELECT SIGN(-3) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(ExecuteScalar(connection, "SELECT SQRT(9) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates TRUNC through the scalar surface.
    /// PT: Garante que o Firebird avalie TRUNC pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TruncFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(123m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT TRUNC(123.456) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(123.45m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT TRUNC(123.456, 2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates ROUND through the scalar surface using the Firebird rounding rule.
    /// PT: Garante que o Firebird avalie ROUND pela surface escalar usando a regra de arredondamento do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void RoundFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(2m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT ROUND(1.5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(1.23m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT ROUND(1.234, 2) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(-2m, Convert.ToDecimal(ExecuteScalar(connection, "SELECT ROUND(-1.5) FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates common transcendental numeric helpers through the scalar surface.
    /// PT: Garante que o Firebird avalie funcoes numericas transcendentes comuns pela surface escalar.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TranscendentalNumericFunctions_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ACOS(1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ASIN(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ATAN(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(Math.PI / 4d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ATAN2(1, 1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT COS(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT COSH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d / Math.Tan(1d), Convert.ToDouble(ExecuteScalar(connection, "SELECT COT(1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT EXP(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(1d, Convert.ToDouble(ExecuteScalar(connection, "SELECT LN(EXP(1)) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar(connection, "SELECT LOG(10, 100) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(2d, Convert.ToDouble(ExecuteScalar(connection, "SELECT LOG10(100) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT SIN(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT SINH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT TAN(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT TANH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ACOSH(1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ASINH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
        Assert.Equal(0d, Convert.ToDouble(ExecuteScalar(connection, "SELECT ATANH(0) FROM RDB$DATABASE"), CultureInfo.InvariantCulture), 12);
    }

    /// <summary>
    /// EN: Ensures Firebird evaluates DATEADD against the current timestamp expression.
    /// PT: Garante que o Firebird avalie DATEADD sobre a expressao de timestamp atual.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void DateAddTemporalFunction_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var current = Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE"));
        var nextDay = Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT DATEADD(1 DAY TO CURRENT_TIMESTAMP) FROM RDB$DATABASE"));
        Assert.Equal(current.Date.AddDays(1), nextDay.Date);
        Assert.True(nextDay >= current);
    }

    /// <summary>
    /// EN: Ensures Firebird accepts the spaced keyword forms for the supported temporal system functions.
    /// PT: Garante que o Firebird aceite as formas com espaco das funcoes temporais de sistema suportadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void SpacedSystemTemporalKeywords_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT DATE FROM RDB$DATABASE"));
        Assert.IsType<TimeSpan>(ExecuteScalar(connection, "SELECT CURRENT TIME FROM RDB$DATABASE"));
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT CURRENT TIMESTAMP FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird resolves the supported context variables for user, role, database, and connection.
    /// PT: Garante que o Firebird resolva as variaveis de contexto suportadas para usuario, role, banco e conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ContextVariables_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
        Assert.Equal("SYSDBA", ExecuteScalar(connection, "SELECT CURRENT_USER FROM RDB$DATABASE"));
        Assert.Equal("SYSDBA", ExecuteScalar(connection, "SELECT USER FROM RDB$DATABASE"));
        Assert.Equal("NONE", ExecuteScalar(connection, "SELECT CURRENT_ROLE FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, ExecuteScalar(connection, "SELECT CURRENT_DATABASE FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, ExecuteScalar(connection, "SELECT CURRENT_CATALOG FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT CURRENT_CONNECTION FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT CURRENT_TRANSACTION FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT SESSION_ID FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT TRANSACTION_ID FROM RDB$DATABASE"));
        Assert.Equal("XNET", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'NETWORK_PROTOCOL') FROM RDB$DATABASE"));
        Assert.Equal(Environment.MachineName, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_HOST') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PID') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_ADDRESS') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.ProcessName, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PROCESS') FROM RDB$DATABASE"));
        Assert.Equal(string.Format(CultureInfo.InvariantCulture, "Firebird {0:0.0}", FirebirdDbVersions.Default / 10d), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE"));
        Assert.Equal("SNAPSHOT TABLE STABILITY", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ISOLATION_LEVEL') FROM RDB$DATABASE"));
        ExecuteNonQuery(connection, "UPDATE Users SET Name = 'Ana2' WHERE Id = 999");
        Assert.Equal(0L, Convert.ToInt64(ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ROW_COUNT') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE"));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_ENCRYPTED') FROM RDB$DATABASE"));
        Assert.Equal(DateTime.Now.Date, Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT TODAY FROM RDB$DATABASE")).Date);
        Assert.Equal(DateTime.Now.Date.AddDays(1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT TOMORROW FROM RDB$DATABASE")).Date);
        Assert.Equal(DateTime.Now.Date.AddDays(-1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT YESTERDAY FROM RDB$DATABASE")).Date);
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT NOW FROM RDB$DATABASE"));
        Assert.Equal(DateTime.Now.Date, Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'TODAY' FROM RDB$DATABASE")).Date);
        Assert.Equal(DateTime.Now.Date.AddDays(1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'TOMORROW' FROM RDB$DATABASE")).Date);
        Assert.Equal(DateTime.Now.Date.AddDays(-1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'YESTERDAY' FROM RDB$DATABASE")).Date);
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'NOW' FROM RDB$DATABASE"));
        Assert.Equal("00000", ExecuteScalar(connection, "SELECT SQLSTATE FROM RDB$DATABASE"));
        Assert.Equal(0, ExecuteScalar(connection, "SELECT SQLCODE FROM RDB$DATABASE"));
        Assert.Equal(0, ExecuteScalar(connection, "SELECT GDSCODE FROM RDB$DATABASE"));
        Assert.Equal(false, ExecuteScalar(connection, "SELECT RESETTING FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird trigger context variables reflect the active trigger event.
    /// PT: Garante que as variáveis de contexto de trigger do Firebird reflitam o evento de trigger ativo.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TriggerContextVariables_ShouldReflectActiveTriggerEvent()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);

        var insertingSeen = false;
        var updatingSeen = false;
        var deletingSeen = false;

        var table = Assert.IsAssignableFrom<TableMock>(connection.Db.GetTable("Users"));
        table.AddTrigger(TableTriggerEvent.BeforeInsert, _ => insertingSeen = Convert.ToBoolean(ExecuteScalar(connection, "SELECT INSERTING FROM RDB$DATABASE")));
        table.AddTrigger(TableTriggerEvent.BeforeUpdate, _ => updatingSeen = Convert.ToBoolean(ExecuteScalar(connection, "SELECT UPDATING FROM RDB$DATABASE")));
        table.AddTrigger(TableTriggerEvent.BeforeDelete, _ => deletingSeen = Convert.ToBoolean(ExecuteScalar(connection, "SELECT DELETING FROM RDB$DATABASE")));

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (3, 'Carol')");
        ExecuteNonQuery(connection, "UPDATE Users SET Name = 'Carolyn' WHERE Id = 3");
        ExecuteNonQuery(connection, "DELETE FROM Users WHERE Id = 3");

        Assert.True(insertingSeen);
        Assert.True(updatingSeen);
        Assert.True(deletingSeen);
    }

    /// <summary>
    /// EN: Ensures Firebird resolves RDB$GET_CONTEXT for the SYSTEM namespace using the supported context variables.
    /// PT: Garante que o Firebird resolva RDB$GET_CONTEXT para o namespace SYSTEM usando as variaveis de contexto suportadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void GetContextSystemNamespace_ShouldReturnExpectedValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);
        using var transaction = connection.BeginTransaction(IsolationLevel.Serializable);
        using var currentProcess = System.Diagnostics.Process.GetCurrentProcess();
        var now = DateTime.Now;

        Assert.Equal("SYSDBA", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_USER') FROM RDB$DATABASE"));
        Assert.Equal("NONE", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_ROLE') FROM RDB$DATABASE"));
        Assert.Equal(connection.Database, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_DATABASE') FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_CONNECTION') FROM RDB$DATABASE"));
        Assert.IsType<int>(ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CURRENT_TRANSACTION') FROM RDB$DATABASE"));
        Assert.Equal("XNET", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'NETWORK_PROTOCOL') FROM RDB$DATABASE"));
        Assert.Equal(Environment.MachineName, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_HOST') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PID') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.Id.ToString(CultureInfo.InvariantCulture), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_ADDRESS') FROM RDB$DATABASE"));
        Assert.Equal(currentProcess.ProcessName, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'CLIENT_PROCESS') FROM RDB$DATABASE"));
        Assert.Equal(string.Format(CultureInfo.InvariantCulture, "Firebird {0:0.0}", FirebirdDbVersions.Default / 10d), ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ENGINE_VERSION') FROM RDB$DATABASE"));
        Assert.Equal("SNAPSHOT TABLE STABILITY", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ISOLATION_LEVEL') FROM RDB$DATABASE"));
        ExecuteNonQuery(connection, "UPDATE Users SET Name = 'Ana2' WHERE Id = 999");
        Assert.Equal(0L, Convert.ToInt64(ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'ROW_COUNT') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_COMPRESSED') FROM RDB$DATABASE"));
        Assert.Equal(DBNull.Value, ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('SYSTEM', 'WIRE_ENCRYPTED') FROM RDB$DATABASE"));
        Assert.Equal(now.Date, Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT TODAY FROM RDB$DATABASE")).Date);
        Assert.Equal(now.Date.AddDays(1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT TOMORROW FROM RDB$DATABASE")).Date);
        Assert.Equal(now.Date.AddDays(-1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT YESTERDAY FROM RDB$DATABASE")).Date);
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT NOW FROM RDB$DATABASE"));
        Assert.Equal(now.Date, Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'TODAY' FROM RDB$DATABASE")).Date);
        Assert.Equal(now.Date.AddDays(1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'TOMORROW' FROM RDB$DATABASE")).Date);
        Assert.Equal(now.Date.AddDays(-1), Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'YESTERDAY' FROM RDB$DATABASE")).Date);
        Assert.IsType<DateTime>(ExecuteScalar(connection, "SELECT 'NOW' FROM RDB$DATABASE"));
        Assert.Equal("00000", ExecuteScalar(connection, "SELECT SQLSTATE FROM RDB$DATABASE"));
        Assert.Equal(0, ExecuteScalar(connection, "SELECT SQLCODE FROM RDB$DATABASE"));
        Assert.Equal(0, ExecuteScalar(connection, "SELECT GDSCODE FROM RDB$DATABASE"));
        Assert.Equal(false, ExecuteScalar(connection, "SELECT RESETTING FROM RDB$DATABASE"));
    }

    /// <summary>
    /// EN: Ensures Firebird supports RDB$SET_CONTEXT and RDB$GET_CONTEXT for user-session and user-transaction namespaces.
    /// PT: Garante que o Firebird suporte RDB$SET_CONTEXT e RDB$GET_CONTEXT para os namespaces user-session e user-transaction.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void SetContextUserNamespaces_ShouldRoundTripValues()
    {
        using var connection = CreateOpenConnection(FirebirdDbVersions.Default);
        using var transaction = connection.BeginTransaction();

        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT RDB$SET_CONTEXT('USER_SESSION', 'MyVar', 'hello') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("hello", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('USER_SESSION', 'MyVar') FROM RDB$DATABASE"));
        Assert.Equal(1, Convert.ToInt32(ExecuteScalar(connection, "SELECT RDB$SET_CONTEXT('USER_SESSION', 'MyVar', 'world') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("world", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('USER_SESSION', 'MyVar') FROM RDB$DATABASE"));

        Assert.Equal(0, Convert.ToInt32(ExecuteScalar(connection, "SELECT RDB$SET_CONTEXT('USER_TRANSACTION', 'TxnVar', 'abc') FROM RDB$DATABASE"), CultureInfo.InvariantCulture));
        Assert.Equal("abc", ExecuteScalar(connection, "SELECT RDB$GET_CONTEXT('USER_TRANSACTION', 'TxnVar') FROM RDB$DATABASE"));
    }

    private static IEnumerable<object[]> FirebirdVersions()
        => [Array.Empty<object>()];

    private static FirebirdConnectionMock CreateOpenConnection(int? version = null)
    {
        var db = new FirebirdDbMock(version);
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);

        var connection = new FirebirdConnectionMock(db);
        connection.Open();

        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (1, 'Ana')");
        ExecuteNonQuery(connection, "INSERT INTO Users (Id, Name) VALUES (2, 'Bob')");
        return connection;
    }

    private static object? ExecuteScalar(FirebirdConnectionMock connection, string sql)
    {
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = sql
        };
        return command.ExecuteScalar();
    }

    private static void ExecuteNonQuery(FirebirdConnectionMock connection, string sql)
    {
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = sql
        };
        command.ExecuteNonQuery();
    }

    private static string ToHexString(byte[] bytes)
        => BitConverter.ToString(bytes).Replace("-", string.Empty).ToLowerInvariant();

    private static byte[] FromHexString(string hex)
    {
        if (hex.Length % 2 != 0)
            throw new ArgumentException("Hex string must have an even length.", nameof(hex));

        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
            bytes[i] = byte.Parse(hex.Substring(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);

        return bytes;
    }

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
