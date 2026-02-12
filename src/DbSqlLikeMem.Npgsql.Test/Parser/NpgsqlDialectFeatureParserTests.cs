namespace DbSqlLikeMem.Npgsql.Test.Parser;

/// <summary>
/// EN: Covers PostgreSQL/Npgsql-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos de PostgreSQL/Npgsql.
/// </summary>
public sealed class NpgsqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING is parsed as duplicate-key handling.
    /// PT: Garante que ON CONFLICT DO NOTHING seja interpretado como tratamento de chave duplicada.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
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

    /// <summary>
    /// EN: Ensures MATERIALIZED CTE syntax is accepted.
    /// PT: Garante que a sintaxe de CTE MATERIALIZED seja aceita.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseWithCte_AsMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE is parsed correctly.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE seja interpretado corretamente.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
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

    /// <summary>
    /// EN: Ensures conflict target WHERE, update WHERE, and RETURNING are parsed together.
    /// PT: Garante que WHERE no alvo do conflito, WHERE do update e RETURNING sejam interpretados em conjunto.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
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

    /// <summary>
    /// EN: Ensures SQL Server table hints are rejected for Npgsql.
    /// PT: Garante que hints de tabela do SQL Server sejam rejeitados para Npgsql.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [MemberDataNpgsqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new NpgsqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [MemberDataNpgsqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new NpgsqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
