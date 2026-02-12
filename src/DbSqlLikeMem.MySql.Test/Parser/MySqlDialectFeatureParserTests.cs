namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers MySQL-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do MySQL.
/// </summary>
public sealed class MySqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT syntax is rejected for MySQL.
    /// PT: Garante que a sintaxe ON CONFLICT do PostgreSQL seja rejeitada no MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflict_ShouldRespectDialectRule(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE support follows the configured MySQL version.
    /// PT: Garante que o suporte a WITH RECURSIVE siga a versão configurada do MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithRecursive_ShouldRespectVersion(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte";

        if (version < MySqlDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures MySQL index/keyword hints are parsed.
    /// PT: Garante que hints de índice/palavras-chave do MySQL sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users AS u USE INDEX (idx_users_id) IGNORE KEY FOR ORDER BY (idx_users_name)";

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }


    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [MemberDataMySqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new MySqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.Equal(false, d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
