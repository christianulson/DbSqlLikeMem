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
}
