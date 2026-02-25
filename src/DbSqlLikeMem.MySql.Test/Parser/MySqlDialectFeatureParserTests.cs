namespace DbSqlLikeMem.MySql.Test.Parser;

/// <summary>
/// EN: Covers MySQL-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do MySQL.
/// </summary>
public sealed class MySqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT syntax is rejected for MySQL.
    /// PT: Garante que a sintaxe ON CONFLICT do PostgreSQL seja rejeitada no MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseInsert_OnConflict_ShouldRespectDialectRule(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE support follows the configured MySQL version.
    /// PT: Garante que o suporte a with recursive siga a versão configurada do MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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



    /// <summary>
    /// EN: Verifies unsupported WITH RECURSIVE versions return actionable MySQL guidance.
    /// PT: Verifica que versões sem suporte a WITH RECURSIVE retornam orientação acionável para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion(VersionLowerThan = MySqlDialect.WithCteMinVersion)]
    public void ParseSelect_WithRecursive_UnsupportedVersion_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte", new MySqlDialect(version)));

        Assert.Contains("WITH/CTE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index/keyword hints are parsed.
    /// PT: Garante que hints de índice/palavras-chave do MySQL sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users AS u USE INDEX (idx_users_id) IGNORE KEY FOR ORDER BY (idx_users_name)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.NotNull(parsed.Table);
        Assert.Equal(2, parsed.Table!.MySqlIndexHints?.Count ?? 0);
    }




    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR ORDER BY is captured in AST.
    /// PT: Garante que o escopo FOR ORDER BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForOrderBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u IGNORE INDEX FOR ORDER BY (idx_users_name) ORDER BY u.name";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Ignore, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.OrderBy, hint.Scope);
        Assert.Equal(["idx_users_name"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint scope FOR GROUP BY is captured in AST.
    /// PT: Garante que o escopo FOR GROUP BY de hint de índice MySQL seja capturado na AST.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithIndexHintForGroupBy_ShouldCaptureScope(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR GROUP BY (idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.GroupBy, hint.Scope);
        Assert.Equal(["idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures advanced MySQL index hints with PRIMARY and FOR JOIN are parsed.
    /// PT: Garante que hints avançados de índice MySQL com PRIMARY e FOR JOIN sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithAdvancedIndexHints_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u FORCE INDEX FOR JOIN (PRIMARY, idx_users_id) WHERE u.id > 0";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Force, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Join, hint.Scope);
        Assert.Equal(["PRIMARY", "idx_users_id"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures empty MySQL index hint list is rejected.
    /// PT: Garante que lista vazia em hint de índice MySQL seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintList_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX ()";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("lista de índices vazia", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint list containing empty item is rejected.
    /// PT: Garante que lista de hints MySQL contendo item vazio seja rejeitada.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithEmptyIndexHintItem_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id, )";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("item vazio", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL index hint names with dollar and escaped backtick quoted names are parsed.
    /// PT: Garante que nomes de índice MySQL com cifrão e nomes quoted com escape de backtick sejam interpretados.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithExtendedValidIndexHintNames_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx$users, `idx``quoted`)";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.NotNull(parsed.Table);
        var hint = Assert.Single(parsed.Table!.MySqlIndexHints ?? []);
        Assert.Equal(SqlMySqlIndexHintKind.Use, hint.Kind);
        Assert.Equal(SqlMySqlIndexHintScope.Any, hint.Scope);
        Assert.Equal(["idx$users", "idx`quoted"], hint.IndexNames);
    }

    /// <summary>
    /// EN: Ensures OFFSET/FETCH compatibility syntax is accepted for MySQL parser.
    /// PT: Garante que a sintaxe de compatibilidade OFFSET/FETCH seja aceita pelo parser MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new MySqlDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }


    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new MySqlDialect(version);

        var limitOffset = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
            dialect));
        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));

        var normalizedLimit = Assert.IsType<SqlLimitOffset>(limitOffset.RowLimit);
        var normalizedFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);

        Assert.Equal(normalizedLimit, normalizedFetch);
        Assert.Equal(2, normalizedFetch.Count);
        Assert.Equal(1, normalizedFetch.Offset);
    }



    /// <summary>
    /// EN: Verifies FETCH FIRST syntax returns actionable MySQL pagination guidance.
    /// PT: Verifica que sintaxe FETCH FIRST retorna orientação acionável de paginação para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_FetchFirst_ShouldProvidePaginationHint(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users ORDER BY id FETCH FIRST 5 ROWS ONLY", new MySqlDialect(version)));

        Assert.Contains("FETCH FIRST/NEXT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIMIT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("mysql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new MySqlDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }




    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new MySqlDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("MySQL", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for MySQL.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para MySQL.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MySqlDialect(version)));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("USE/IGNORE/FORCE INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies MERGE in MySQL returns actionable replacement guidance.
    /// PT: Verifica que MERGE no MySQL retorna orientação acionável de substituição.
    /// </summary>
    /// <param name="version">EN: MySQL dialect version under test. PT: Versão do dialeto MySQL em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseMerge_UnsupportedDialect_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'", new MySqlDialect(version)));

        Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by MySQL version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do MySQL e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = new MySqlDialect(version);

        var expected = version >= MySqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("RANK"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against MySQL dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto MySQL por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(supported, dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(supported, dialect);
        Assert.IsType<WindowFunctionExpr>(expr);
        Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(unsupported, dialect));
    }


    /// <summary>
    /// EN: Ensures window functions that require ordering reject OVER clauses without ORDER BY.
    /// PT: Garante que funções de janela que exigem ordenação rejeitem cláusulas OVER sem ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", dialect));
            return;
        }

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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", dialect));
            return;
        }

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
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new MySqlDialect(version);
        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", dialect));
            return;
        }


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
    [MemberDataMySqlVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new MySqlDialect(version);

        var expected = version >= MySqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("LAG"));

        Assert.False(dialect.RequiresOrderByInWindowFunction("COUNT"));
    }


    /// <summary>
    /// EN: Ensures window function argument arity metadata is exposed through dialect hook.
    /// PT: Garante que os metadados de aridade de argumentos de função de janela sejam expostos pelo hook do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.False(dialect.TryGetWindowFunctionArgumentArity("ROW_NUMBER", out _, out _));
            return;
        }

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("ROW_NUMBER", out var rnMin, out var rnMax));
        Assert.Equal(0, rnMin);
        Assert.Equal(0, rnMax);

        Assert.True(dialect.TryGetWindowFunctionArgumentArity("LAG", out var lagMin, out var lagMax));
        Assert.Equal(1, lagMin);
        Assert.Equal(3, lagMax);

        Assert.False(dialect.TryGetWindowFunctionArgumentArity("COUNT", out _, out _));
    }


    /// <summary>
    /// EN: Ensures ROWS window frame clauses parse when supported and RANGE remains gated.
    /// PT: Garante que cláusulas ROWS de frame de janela sejam interpretadas quando suportadas e que RANGE continue bloqueado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(expr);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));
        Assert.Contains("window frame unit", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures invalid window frame bound ordering is rejected by parser semantic validation.
    /// PT: Garante que ordenação inválida de limites de frame de janela seja rejeitada pela validação semântica do parser.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMySqlVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var dialect = new MySqlDialect(version);

        if (version < MySqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
