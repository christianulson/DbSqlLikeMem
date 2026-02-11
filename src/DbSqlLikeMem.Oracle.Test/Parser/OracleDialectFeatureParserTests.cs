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
    [Theory]
    [MemberDataOracleVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
    }


    [Theory]
    [MemberDataOracleVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new OracleDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("oracle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}