namespace DbSqlLikeMem.Oracle.Test.Parser;

/// <summary>
/// EN: Covers Oracle-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do Oracle.
/// </summary>
public sealed class OracleDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures WITH RECURSIVE syntax is rejected for Oracle.
    /// PT: Garante que a sintaxe WITH RECURSIVE seja rejeitada no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithRecursive_ShouldBeRejected(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1 FROM dual) SELECT n FROM cte";

        if (version < OracleDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
            return;
        }

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT syntax is rejected for Oracle.
    /// PT: Garante que a sintaxe ON CONFLICT seja rejeitada no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseInsert_OnConflict_ShouldBeRejected(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints are rejected for Oracle.
    /// PT: Garante que hints de tabela do SQL Server sejam rejeitados no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for Oracle.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
        Assert.Contains("OPTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) is also rejected after UNION tails for Oracle.
    /// PT: Garante que OPTION(...) de SQL Server também seja rejeitado após tail de UNION no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseUnion_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users UNION SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
        Assert.Contains("OPTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures PIVOT clause parsing is available for this dialect.
    /// PT: Garante que o parsing da cláusula PIVOT esteja disponível para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithPivot_ShouldParse(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var parsed = SqlQueryParser.Parse(sql, new OracleDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new OracleDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.True(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new OracleDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("oracle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
