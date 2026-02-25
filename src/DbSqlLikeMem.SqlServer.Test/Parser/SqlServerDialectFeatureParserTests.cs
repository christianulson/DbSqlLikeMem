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
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_OffsetWithoutOrderBy_ShouldRespectDialectRule(int version)
    {
        var sql = "SELECT id FROM users OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        if (version < SqlServerDialect.OffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("Adicione ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures OFFSET/FETCH accepts command parameters in parser execution mode.
    /// PT: Garante que OFFSET/FETCH aceite parâmetros de comando no modo de execução do parser.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithOffsetFetchParameters_ShouldParse(int version)
    {
        if (version < SqlServerDialect.OffsetFetchMinVersion)
            return;

        var sql = "SELECT id FROM users ORDER BY id OFFSET @p0 ROWS FETCH FIRST @p1 ROWS ONLY";
        var pars = new SqlServerDataParameterCollectionMock
        {
            new Microsoft.Data.SqlClient.SqlParameter("@p0", 1),
            new Microsoft.Data.SqlClient.SqlParameter("@p1", 2)
        };

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version), pars);
        Assert.IsType<SqlSelectQuery>(parsed);
    }


    /// <summary>
    /// EN: Verifies LIMIT syntax in SQL Server returns an actionable pagination hint.
    /// PT: Verifica que sintaxe LIMIT no SQL Server retorna dica acionável de paginação.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_Limit_ShouldProvidePaginationHint(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users ORDER BY id LIMIT 5", new SqlServerDialect(version)));

        Assert.Contains("LIMIT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new SqlServerDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE syntax is rejected.
    /// PT: Garante que a sintaxe with recursive seja rejeitada.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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
    /// EN: Verifies WITH RECURSIVE rejection includes actionable SQL Server guidance.
    /// PT: Verifica que a rejeição de WITH RECURSIVE inclui orientação acionável para SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.WithCteMinVersion)]
    public void ParseSelect_WithRecursive_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte", new SqlServerDialect(version)));

        Assert.Contains("WITH sem RECURSIVE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints in WITH (...) form are parsed.
    /// PT: Garante que hints de tabela SQL Server na forma WITH (...) sejam interpretados.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithLegacySqlServerTableHint_ShouldParse(int version)
    {
        var sql = "SELECT u.id FROM users u (NOLOCK)";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are accepted by parser capability.
    /// PT: Garante que hints de consulta SQL Server OPTION(...) sejam aceitos pela capability do parser.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithOptionQueryHint_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1, RECOMPILE)";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures OPTION(...) is accepted after UNION query tails.
    /// PT: Garante que OPTION(...) seja aceito após o tail de consultas UNION.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUnion_WithOptionQueryHint_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users UNION SELECT id FROM users ORDER BY id OPTION (MAXDOP 1)";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));

        Assert.IsType<SqlUnionQuery>(parsed);
    }


    /// <summary>
    /// EN: Ensures PIVOT clause parsing is available for this dialect.
    /// PT: Garante que o parsing da cláusula pivot esteja disponível para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithPivot_ShouldParse(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version));

        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures invalid PIVOT syntax fails with parser validation error.
    /// PT: Garante que sintaxe inválida de pivot falhe com erro de validação do parser.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithInvalidPivotSyntax_ShouldThrowInvalidOperation(int version)
    {
        var sql = "SELECT t10 FROM users PIVOT (COUNT(id) tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new SqlServerDialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1) SELECT n FROM cte", new SqlServerDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("sqlserver", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by SQL Server version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do SQL Server e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = new SqlServerDialect(version);

        var expected = version >= SqlServerDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("DENSE_RANK"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against SQL Server dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto SQL Server por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new SqlServerDialect(version);
        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new SqlServerDialect(version);

        var expected = version >= SqlServerDialect.WindowFunctionsMinVersion;
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
    [MemberDataSqlServerVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
