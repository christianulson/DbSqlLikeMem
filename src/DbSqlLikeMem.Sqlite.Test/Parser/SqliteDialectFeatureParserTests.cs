namespace DbSqlLikeMem.Sqlite.Test.Parser;

public sealed class SqliteDialectFeatureParserTests
{
    [Theory]
    [MemberDataSqliteVersion]
    public void ParseInsert_OnConflict_DoUpdate_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = 'b'";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
    }

    [Theory]
    [MemberDataSqliteVersion]
    public void ParseWithCte_AsNotMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }
    [Theory]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithMySqlIndexHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));
    }


    [Theory]
    [MemberDataSqliteVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users USE INDEX (idx_users_id)", new SqliteDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sqlite", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
    [Theory]
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

}
