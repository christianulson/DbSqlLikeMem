namespace DbSqlLikeMem.Sqlite.Test.Parser;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class SqliteDialectFeatureParserTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseInsert_OnConflict_DoUpdate_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = 'b'";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseWithCte_AsNotMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithMySqlIndexHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for SQLite.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para SQLite.
    /// </summary>
    /// <param name="version">EN: SQLite dialect version under test. PT: Versão do dialeto SQLite em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));
        Assert.Contains("OPTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users USE INDEX (idx_users_id)", new SqliteDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sqlite", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_UnionOrderBy_ShouldParseAsUnion(int version)
    {
        var sql = "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2 ORDER BY id";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        var union = Assert.IsType<SqlUnionQuery>(parsed);
        Assert.Equal(2, union.Parts.Count);
        Assert.Single(union.AllFlags);
        Assert.False(union.AllFlags[0]);
    }


    /// <summary>
    /// EN: Ensures OFFSET/FETCH compatibility syntax is accepted for SQLite parser.
    /// PT: Garante que a sintaxe de compatibilidade OFFSET/FETCH seja aceita pelo parser SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }




    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula PIVOT seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sqlite", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new SqliteDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }

}
