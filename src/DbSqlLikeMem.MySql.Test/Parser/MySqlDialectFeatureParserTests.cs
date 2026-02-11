namespace DbSqlLikeMem.MySql.Test.Parser;

public sealed class MySqlDialectFeatureParserTests
{
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflict_ShouldRespectDialectRule(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
    }

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
    [Theory]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users AS u USE INDEX (idx_users_id) IGNORE KEY FOR ORDER BY (idx_users_name)";

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }


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