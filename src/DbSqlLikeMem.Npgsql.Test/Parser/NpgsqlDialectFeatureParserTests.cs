namespace DbSqlLikeMem.Npgsql.Test.Parser;

public sealed class NpgsqlDialectFeatureParserTests
{
    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothing_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Empty(ins.OnDupAssigns);
    }

    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseWithCte_AsMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdate_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Equal("name", ins.OnDupAssigns[0].Col);
    }

    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_Returning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
    }
    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
    }

}
