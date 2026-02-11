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

}
