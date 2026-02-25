namespace DbSqlLikeMem.Sqlite.Test.Parser;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class SqliteDialectFeatureParserTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseInsert_OnConflict_DoUpdate_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = 'b'";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseWithCte_AsNotMaterialized_ShouldParse(int version)
    {
        var sql = "WITH x AS NOT MATERIALIZED (SELECT 1 AS id) SELECT id FROM x";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithMySqlIndexHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for SQLite.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para SQLite.
    /// </summary>
    /// <param name="version">EN: SQLite dialect version under test. PT: Versão do dialeto SQLite em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));
        Assert.Contains("OPTION", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Use hints compatíveis", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users USE INDEX (idx_users_id)", new SqliteDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sqlite", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_UnionOrderBy_ShouldParseAsUnion(int version)
    {
        var sql = "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2 ORDER BY id";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));

        var union = Assert.IsType<SqlUnionQuery>(parsed);
        Assert.Equal(2, union.Parts.Count);
        Assert.Single(union.AllFlags);
        Assert.False(union.AllFlags[0]);
    }


    /// <summary>
    /// EN: Ensures OFFSET/FETCH compatibility syntax is accepted for SQLite parser.
    /// PT: Garante que a sintaxe de compatibilidade OFFSET/FETCH seja aceita pelo parser SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new SqliteDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }




    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("sqlite", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new SqliteDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new SqliteDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new SqliteDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new SqliteDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }



    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseMerge_UnsupportedDialect_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new SqliteDialect(version)));

        Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates known and unknown window function capability for SQLite dialect versions.
    /// PT: Valida a capacidade de funções de janela conhecidas e desconhecidas nas versões do dialeto SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void WindowFunctionCapability_ShouldAllowKnownAndRejectUnknownFunctions(int version)
    {
        var dialect = new SqliteDialect(version);

        Assert.True(dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.True(dialect.SupportsWindowFunction("FIRST_VALUE"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser accepts known window functions and rejects unknown names for SQLite dialect versions.
    /// PT: Garante que o parser aceite funções de janela conhecidas e rejeite nomes desconhecidos nas versões do SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_WindowFunctionName_ShouldAllowKnownAndRejectUnknown(int version)
    {
        var dialect = new SqliteDialect(version);

        var expr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id)", dialect);
        Assert.IsType<WindowFunctionExpr>(expr);
        Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("PERCENTILE_CONT(0.5) OVER (ORDER BY id)", dialect));
    }


    /// <summary>
    /// EN: Ensures window functions that require ordering reject OVER clauses without ORDER BY.
    /// PT: Garante que funções de janela que exigem ordenação rejeitem cláusulas OVER sem ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", dialect));

        Assert.Contains("requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates window function argument arity for supported functions.
    /// PT: Garante que o parser valide a aridade dos argumentos de funções de janela suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new SqliteDialect(version);

        var exRowNumber = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", dialect));
        Assert.Contains("does not accept arguments", exRowNumber.Message, StringComparison.OrdinalIgnoreCase);

        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE() OVER (ORDER BY id)", dialect));
        Assert.Contains("exactly 1 argument", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, 1, 0, 99) OVER (ORDER BY id)", dialect));
        Assert.Contains("between 1 and 3 arguments", exLag.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates literal semantic ranges for window function arguments.
    /// PT: Garante que o parser valide intervalos semânticos literais para argumentos de funções de janela.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new SqliteDialect(version);

        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", dialect));
        Assert.Contains("positive bucket count", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, -1, 0) OVER (ORDER BY id)", dialect));
        Assert.Contains("non-negative offset", exLag.Message, StringComparison.OrdinalIgnoreCase);

        var exNthValue = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTH_VALUE(id, 0) OVER (ORDER BY id)", dialect));
        Assert.Contains("greater than zero", exNthValue.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ORDER BY requirement for window functions is exposed through dialect runtime hook.
    /// PT: Garante que o requisito de ORDER BY para funções de janela seja exposto pelo hook de runtime do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new SqliteDialect(version);

        Assert.True(dialect.RequiresOrderByInWindowFunction("ROW_NUMBER"));
        Assert.True(dialect.RequiresOrderByInWindowFunction("LAG"));

        Assert.False(dialect.RequiresOrderByInWindowFunction("COUNT"));
    }


    /// <summary>
    /// EN: Ensures window function argument arity metadata is exposed through dialect hook.
    /// PT: Garante que os metadados de aridade de argumentos de função de janela sejam expostos pelo hook do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new SqliteDialect(version);

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("ROW_NUMBER", out var rnMin, out var rnMax));
        Assert.Equal(0, rnMin);
        Assert.Equal(0, rnMax);

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("LAG", out var lagMin, out var lagMax));
        Assert.Equal(1, lagMin);
        Assert.Equal(3, lagMax);

        Assert.False(dialect.TryGetWindowFunctionArgumentArity("COUNT", out _, out _));
    }


    /// <summary>
    /// EN: Ensures window frame clause tokens are gated by dialect capability.
    /// PT: Garante que tokens de cláusula de frame de janela sejam controlados pela capability do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_WindowFrameClause_ShouldBeRejectedByDialectCapability(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));

        Assert.Contains("window frame", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
