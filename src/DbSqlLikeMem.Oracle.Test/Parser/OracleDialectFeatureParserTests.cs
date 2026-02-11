namespace DbSqlLikeMem.Oracle.Test.Parser;

public sealed class OracleDialectFeatureParserTests
{
    [Theory]
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

    [Theory]
    [MemberDataOracleVersion]
    public void ParseInsert_OnConflict_ShouldBeRejected(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
    }
}
