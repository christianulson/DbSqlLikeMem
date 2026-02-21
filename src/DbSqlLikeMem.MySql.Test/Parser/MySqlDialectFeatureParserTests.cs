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
    [Trait("Category", "Parser")]
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
    [Trait("Category", "Parser")]
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
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users AS u USE INDEX (idx_users_id) IGNORE KEY FOR ORDER BY (idx_users_name)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.NotNull(parsed.Table);
        Assert.Equal(2, parsed.Table!.MySqlIndexHints?.Count ?? 0);
    }




    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR ORDER BY is captured in AST.
    /// PT: Garante que o escopo FOR ORDER BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForOrderBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u IGNORE INDEX FOR ORDER BY (idx_users_name) ORDER BY u.name";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Ignore, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.OrderBy, hint.Scope);
        Assert.Equal(["idx_users_name"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR GROUP BY is captured in AST.
    /// PT: Garante que o escopo FOR GROUP BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForGroupBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR GROUP BY (idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.GroupBy, hint.Scope);
        Assert.Equal(["idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures advanced MySQL index hints with PRIMARY and FOR JOIN are parsed.
    /// PT: Garante que hints avançados de índice MySQL com PRIMARY e FOR JOIN sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithAdvancedIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR JOIN (PRIMARY, idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Join, hint.Scope);
        Assert.Equal(["PRIMARY", "idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures empty MySQL index hint list is rejected.
    /// PT: Garante que lista vazia em hint de índice MySQL seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintList_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX ()";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("lista de índices vazia", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint list containing empty item is rejected.
    /// PT: Garante que lista de hints MySQL contendo item vazio seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintItem_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id, )";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("item vazio", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint names with dollar and escaped backtick quoted names are parsed.
    /// PT: Garante que nomes de índice MySQL com cifrão e nomes quoted com escape de backtick sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithExtendedValidIndexHintNames_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx$users, `idx``quoted`)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Use, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Any, hint.Scope);
        Assert.Equal(["idx$users", "idx`quoted"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures OFFSET/FETCH compatibility syntax is accepted for MySQL parser.
    /// PT: Garante que a sintaxe de compatibilidade OFFSET/FETCH seja aceita pelo parser MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula PIVOT seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mysql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new MySqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for MySQL.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.Contains("OPTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
