namespace DbSqlLikeMem.SqlServer.Test.Parser;

/// <summary>
/// EN: Covers SQL Server-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do SQL Server.
/// </summary>
public sealed class SqlServerDialectFeatureParserTests
{
    /// <summary>
    /// EN: Validates OFFSET/FETCH without ORDER BY according to dialect version rules.
    /// PT: Valida OFFSET/FETCH sem ORDER BY conforme as regras de versão do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseSelect_OffsetWithoutOrderBy_ShouldRespectDialectRule(int version)
    {
        var sql = "SELECT id FROM users OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        if (version < SqlServerDialect.OffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE syntax is rejected.
    /// PT: Garante que a sintaxe WITH RECURSIVE seja rejeitada.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithRecursive_ShouldBeRejected(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte";

        if (version < SqlServerDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints in WITH (...) form are parsed.
    /// PT: Garante que hints de tabela SQL Server na forma WITH (...) sejam interpretados.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u WITH (NOLOCK, INDEX([IX_Users_Id]))";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures legacy SQL Server table hint syntax is parsed.
    /// PT: Garante que a sintaxe legada de hint de tabela do SQL Server seja interpretada.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithLegacySqlServerTableHint_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u (NOLOCK)";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new SqlServerDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.Equal(false, d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [MemberDataSqlServerVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte", new SqlServerDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sqlserver", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
