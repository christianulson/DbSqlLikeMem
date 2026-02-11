namespace DbSqlLikeMem.Db2.Test.Parser;

/// <summary>
/// EN: Tests DB2 dialect parser behavior for unsupported SQL features.
/// PT: Testa o comportamento do parser de dialeto DB2 para recursos SQL não suportados.
/// </summary>
public sealed class Db2DialectFeatureParserTests
{
    /// <summary>
    /// EN: Tests ParseSelect_WithRecursive_ShouldBeRejected behavior.
    /// PT: Testa o comportamento de ParseSelect_WithRecursive_ShouldBeRejected.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    public void ParseSelect_WithRecursive_ShouldBeRejected(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1 FROM SYSIBM.SYSDUMMY1) SELECT n FROM cte";

        if (version < Db2Dialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
            return;
        }

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests ParseInsert_OnConflict_ShouldBeRejected behavior.
    /// PT: Testa o comportamento de ParseInsert_OnConflict_ShouldBeRejected.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    public void ParseInsert_OnConflict_ShouldBeRejected(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Tests ParseSelect_WithMySqlIndexHints_ShouldBeRejected behavior.
    /// PT: Testa o comportamento de ParseSelect_WithMySqlIndexHints_ShouldBeRejected.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    public void ParseSelect_WithMySqlIndexHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }


    [Theory]
    [MemberDataDb2Version]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users USE INDEX (idx_users_id)", new Db2Dialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("db2", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
    [Theory]
    [MemberDataDb2Version]
    public void ParseSelect_UnionOrderBy_ShouldParseAsUnion(int version)
    {
        var sql = "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2 ORDER BY id";

        var parsed = SqlQueryParser.Parse(sql, new Db2Dialect(version));

        var union = Assert.IsType<SqlUnionQuery>(parsed);
        Assert.Equal(2, union.Parts.Count);
        Assert.Single(union.AllFlags);
        Assert.False(union.AllFlags[0]);
    }

    [Theory]
    [MemberDataDb2Version]
    public void ParseSelect_WithCteSimple_ShouldParse(int version)
    {
        var sql = "WITH u AS (SELECT id FROM users) SELECT id FROM u";

        if (version < Db2Dialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new Db2Dialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }
}
