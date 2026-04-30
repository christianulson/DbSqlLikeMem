namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Verifies the fallback automatic dialect picks up Firebird-specific function registrations when the Firebird assembly is available.
/// PT: Verifica se o dialeto automatico de fallback captura registros de funcoes especificas do Firebird quando o assembly Firebird esta disponivel.
/// </summary>
public sealed class AutoDialectFirebirdFunctionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the fallback Auto dialect exposes the Firebird scalar registrations used by the automatic parser path.
    /// PT: Verifica se o dialeto Auto de fallback expõe os registros escalares do Firebird usados pelo caminho automatico do parser.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeFirebirdScalarRegistrations()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsSequenceFunctionCall("GEN_ID"));
        Assert.True(dialect.TryGetScalarFunctionDefinition("DATEADD", out _));
        Assert.True(dialect.TryGetScalarFunctionDefinition("HASH", out _));
        Assert.True(dialect.TryGetScalarFunctionDefinition("CRYPT_HASH", out _));
    }

    /// <summary>
    /// EN: Verifies the fallback Auto dialect parses the Firebird temporal and hash function syntax used by the shared automatic path.
    /// PT: Verifica se o dialeto Auto de fallback interpreta a sintaxe temporal e de hash do Firebird usada pelo caminho automatico compartilhado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseFirebirdFunctionFamilies()
    {
        var dialect = new AutoSqlDialect();
        var db = new FirebirdDbMock();

        var dateAdd = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATEADD(1 DAY TO CURRENT_TIMESTAMP)", db, dialect));
        var hash = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("HASH('Firebird' USING CRC32)", db, dialect));
        var cryptHash = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CRYPT_HASH('Firebird' USING SHA256)", db, dialect));
        var genId = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("GEN_ID(seq_orders, 1)", db, dialect));

        Assert.Equal("DATEADD", dateAdd.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DAY", Assert.IsType<RawSqlExpr>(dateAdd.Args[0]).Sql, ignoreCase: true);
        Assert.Equal("HASH", hash.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CRC32", Assert.IsType<RawSqlExpr>(hash.Args[1]).Sql, ignoreCase: true);
        Assert.Equal("CRYPT_HASH", cryptHash.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SHA256", Assert.IsType<RawSqlExpr>(cryptHash.Args[1]).Sql, ignoreCase: true);
        Assert.Equal("GEN_ID", genId.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies the fallback Auto dialect executes Firebird temporal, sequence, and hash functions through the connection auto mode.
    /// PT: Verifica se o dialeto Auto de fallback executa funcoes temporais, de sequence e de hash do Firebird pelo modo automatico da conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AutoDialect_ShouldExecuteFirebirdFunctionFamilies()
    {
        var db = new FirebirdDbMock();
        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        connection.UseAutoSqlDialect = true;

        using (var createSequence = connection.CreateCommand())
        {
            createSequence.CommandText = "CREATE SEQUENCE seq_orders START WITH 10 INCREMENT BY 10";
            createSequence.ExecuteNonQuery();
        }

        object? ExecuteScalar(string sql)
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            return command.ExecuteScalar();
        }

        var current = Assert.IsType<DateTime>(ExecuteScalar("SELECT CURRENT_TIMESTAMP FROM RDB$DATABASE"));
        var nextDay = Assert.IsType<DateTime>(ExecuteScalar("SELECT DATEADD(1 DAY TO CURRENT_TIMESTAMP) FROM RDB$DATABASE"));
        var genId = Convert.ToInt64(ExecuteScalar("SELECT GEN_ID(seq_orders, 1) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);
        var hash = Convert.ToInt32(ExecuteScalar("SELECT HASH('Firebird' USING CRC32) FROM RDB$DATABASE"), CultureInfo.InvariantCulture);

        using var sha256 = System.Security.Cryptography.SHA256.Create();
        var expectedCryptHash = sha256.ComputeHash(System.Text.Encoding.UTF8.GetBytes("Firebird"));
        var cryptHash = Assert.IsType<byte[]>(ExecuteScalar("SELECT CRYPT_HASH('Firebird' USING SHA256) FROM RDB$DATABASE"));

        Assert.Equal(current.Date.AddDays(1), nextDay.Date);
        Assert.True(nextDay >= current);
        Assert.Equal(1L, genId);
        Assert.Equal(unchecked((int)ComputeCrc32(System.Text.Encoding.UTF8.GetBytes("Firebird"))), hash);
        Assert.Equal(expectedCryptHash, cryptHash);
    }

    /// <summary>
    /// EN: Computes the CRC32 hash used by the Firebird HASH CRC32 variant in the Auto fallback tests.
    /// PT: Computa o hash CRC32 usado pela variante CRC32 do HASH do Firebird nos testes do fallback Auto.
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
    /// EN: Stores the lookup table used to compute CRC32 values for the Auto fallback tests.
    /// PT: Armazena a tabela de consulta usada para computar valores CRC32 nos testes do fallback Auto.
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
