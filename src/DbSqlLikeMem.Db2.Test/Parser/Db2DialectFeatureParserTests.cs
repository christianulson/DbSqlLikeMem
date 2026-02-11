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

}
