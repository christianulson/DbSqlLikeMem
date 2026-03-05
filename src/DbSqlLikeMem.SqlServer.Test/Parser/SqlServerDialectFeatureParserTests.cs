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
    /// EN: Ensures SQL Server OFFSET/FETCH pagination is normalized to the canonical row-limit AST node.
    /// PT: Garante que a paginação OFFSET/FETCH do SQL Server seja normalizada para o nó canônico de AST de limite de linhas.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_OffsetFetch_ShouldNormalizeRowLimitAst(int version)
    {
        if (version < SqlServerDialect.OffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", new SqlServerDialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            new SqlServerDialect(version)));

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(2, rowLimit.Count);
        Assert.Equal(1, rowLimit.Offset);
    }

    /// <summary>
    /// EN: Verifies parsing SELECT with LIMIT returns an actionable hint for SQL Server pagination syntax.
    /// PT: Verifica que o parsing de SELECT com LIMIT retorna uma dica acionável para a sintaxe de paginação do SQL Server.
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
    /// EN: Ensures MERGE parsing follows SQL Server version support and preserves target table metadata.
    /// PT: Garante que o parsing de MERGE siga o suporte por versão do SQL Server e preserve metadados da tabela alvo.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseMerge_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < SqlServerDialect.MergeMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.NotNull(parsed.Table);
        Assert.Equal("users", parsed.Table!.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("target", parsed.Table.Alias, StringComparer.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE accepts the WHEN NOT MATCHED clause form in merge-capable dialect versions.
    /// PT: Garante que MERGE aceite a forma de cláusula WHEN NOT MATCHED em versões de dialeto com suporte.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithWhenNotMatched_ShouldParse(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id)";

        var query = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Equal("users", query.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures MERGE without USING is rejected with actionable parser guidance in SQL Server dialect.
    /// PT: Garante que MERGE sem USING seja rejeitado com orientação acionável do parser no dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithoutUsing_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target ON target.id = 1 WHEN MATCHED THEN UPDATE SET name = 'x'", new SqlServerDialect(version)));

        Assert.Contains("MERGE requer cláusula USING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without ON is rejected with actionable parser guidance in SQL Server dialect.
    /// PT: Garante que MERGE sem ON seja rejeitado com orientação acionável do parser no dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithoutOn_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src WHEN MATCHED THEN UPDATE SET name = 'x'", new SqlServerDialect(version)));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires ON at top-level and does not accept ON tokens nested inside USING subqueries.
    /// PT: Garante que MERGE exija ON em nível top-level e não aceite tokens ON aninhados dentro de subqueries no USING.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithOnOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING (SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE id > 0)) src WHEN MATCHED THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without WHEN is rejected with actionable parser guidance in SQL Server dialect.
    /// PT: Garante que MERGE sem WHEN seja rejeitado com orientação acionável do parser no dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithoutWhen_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src ON target.id = src.id", new SqlServerDialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE does not accept a source alias named WHEN as a replacement for top-level WHEN clauses.
    /// PT: Garante que MERGE não aceite um alias de origem chamado WHEN como substituto para cláusulas WHEN em nível top-level.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithUsingAliasNamedWhen_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING users when ON target.id = when.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires WHEN at top-level and does not accept WHEN tokens nested inside USING subqueries.
    /// PT: Garante que MERGE exija WHEN em nível top-level e não aceite tokens WHEN aninhados dentro de subqueries no USING.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithWhenOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING (SELECT CASE WHEN id > 0 THEN id ELSE 0 END AS id FROM users) src ON target.id = src.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE rejects invalid top-level WHEN forms that are not WHEN MATCHED/WHEN NOT MATCHED.
    /// PT: Garante que MERGE rejeite formas inválidas de WHEN em nível top-level que não sejam WHEN MATCHED/WHEN NOT MATCHED.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion(VersionGraterOrEqual = SqlServerDialect.MergeMinVersion)]
    public void ParseMerge_WithInvalidTopLevelWhenForm_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN src.id > 0 THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Ensures unsupported quoted aliases are rejected with actionable parser diagnostics for this dialect.
    /// PT: Garante que aliases com quoting não suportado sejam rejeitados com diagnóstico acionável do parser para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", new SqlServerDialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server accepts bracket-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o SQL Server aceite aliases com colchetes e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithBracketQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name [User Name] FROM users",
            new SqlServerDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures SQL Server unescapes doubled brackets inside bracket-quoted aliases when normalizing AST alias text.
    /// PT: Garante que o SQL Server faça unescape de colchetes duplicados dentro de aliases com colchetes ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithEscapedBracketQuotedAlias_ShouldNormalizeEscapedBracket(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name [User]]Name] FROM users",
            new SqlServerDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User]Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures SQL Server unescapes doubled double-quotes inside quoted aliases when normalizing AST alias text.
    /// PT: Garante que o SQL Server faça unescape de aspas duplas duplicadas dentro de aliases quoted ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithEscapedDoubleQuotedAlias_ShouldNormalizeEscapedQuote(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            new SqlServerDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User\"Name", item.Alias);
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
    /// EN: Ensures ROWS/RANGE/GROUPS window frame clauses parse when supported.
    /// PT: Garante que cláusulas ROWS/RANGE/GROUPS de frame de janela sejam interpretadas quando suportadas.
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

        var rowsExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(rowsExpr);

        var rangeExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(rangeExpr);

        var groupsExpr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id GROUPS BETWEEN 1 PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(groupsExpr);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts ordered-set WITHIN GROUP for STRING_AGG.
    /// PT: Garante que o parser SQL Server aceite ordered-set WITHIN GROUP para STRING_AGG.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int version)
    {
        var dialect = new SqlServerDialect(version);

        var expr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("STRING_AGG", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy![0].Desc);

        var multiExpr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC, id ASC)", dialect);
        var multiCall = Assert.IsType<CallExpr>(multiExpr);
        Assert.NotNull(multiCall.WithinGroupOrderBy);
        Assert.Equal(2, multiCall.WithinGroupOrderBy!.Count);
        Assert.True(multiCall.WithinGroupOrderBy[0].Desc);
        Assert.False(multiCall.WithinGroupOrderBy[1].Desc);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser blocks non-native ordered-set aggregate names with WITHIN GROUP.
    /// PT: Garante que o parser SQL Server bloqueie nomes não nativos de agregação ordered-set com WITHIN GROUP.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_ListAggWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP clause fails with actionable ORDER BY message.
    /// PT: Garante que cláusula WITHIN GROUP malformada falhe com mensagem acionável de ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_StringAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures trailing commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas finais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByTrailingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ORDER BY lists in WITHIN GROUP are rejected with actionable message.
    /// PT: Garante que listas ORDER BY vazias em WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByEmptyList_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas iniciais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByLeadingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures missing commas between WITHIN GROUP ORDER BY expressions are rejected with actionable message.
    /// PT: Garante que ausência de vírgula entre expressões de ORDER BY no WITHIN GROUP seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server UPDATE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em UPDATE no SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server UPDATE statements even without WHERE.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em UPDATE no SQL Server mesmo sem WHERE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithReturningWithoutWhere_ShouldBeRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no UPDATE do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que UPDATE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em UPDATE SET sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetAssignmentsWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' updated_at = GETDATE() WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("must separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment with malformed expression is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET com expressão malformada seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = (GETDATE() WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET without assignments and terminated by semicolon is rejected with actionable token context.
    /// PT: Garante que UPDATE SET sem atribuições e finalizado por ponto e vírgula seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetWithoutAssignmentsBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET SET name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET leading comma is rejected with actionable token context.
    /// PT: Garante que vírgula inicial em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET , name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma before assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET trailing comma is rejected with actionable token context.
    /// PT: Garante que vírgula final em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b', WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma without assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("UPDATE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server DELETE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em DELETE no SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server DELETE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no DELETE do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no DELETE continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no DELETE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que DELETE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("DELETE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed RETURNING in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado em INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server INSERT remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no INSERT do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no INSERT continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no INSERT do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON DUPLICATE KEY UPDATE remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnDuplicateKeyUpdate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name),";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateReturningClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with unbalanced parentheses in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com parênteses desbalanceados em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with leading comma in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula inicial em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateLeadingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with trailing comma in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula final em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateTrailingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and followed by RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e seguido por RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsReturningClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula WHERE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE SET name = VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que atribuição em ON DUPLICATE KEY UPDATE sem sinal de igual continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE leading comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que vírgula inicial em ON DUPLICATE KEY UPDATE continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE , name = VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula WHERE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with table-source clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula de table-source continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que INSERT com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after INSERT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1),";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuples without comma separator are rejected with actionable message.
    /// PT: Garante que tuplas em INSERT VALUES sem vírgula separadora sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTuplesWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1) (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("separate row tuples with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES , (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES with malformed scalar expression is rejected with actionable message.
    /// PT: Garante que INSERT VALUES com expressão escalar malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1 +, 'a')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 1 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES reports row/position for malformed expression in later rows.
    /// PT: Garante que INSERT VALUES reporte linha/posição para expressão malformada em linhas posteriores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesSecondRowInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a'), (2 +, 'b')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 2 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id,) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT empty column list is rejected with actionable message.
    /// PT: Garante que lista de colunas vazia no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_EmptyColumnList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users () VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("at least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (,id) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT column list unclosed before semicolon is rejected with actionable message.
    /// PT: Garante que lista de colunas do INSERT não fechada antes de ponto e vírgula seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rejects empty expression between commas inside tuple.
    /// PT: Garante que INSERT VALUES rejeite expressão vazia entre vírgulas dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleMissingExpressionBetweenCommas_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,,2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES rejects trailing comma inside tuple.
    /// PT: Garante que INSERT VALUES rejeite vírgula final dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuple with unclosed parenthesis is rejected with actionable message.
    /// PT: Garante que tupla em INSERT VALUES com parêntese não fechado seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleUnclosedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 2";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES row expression count mismatch is rejected with actionable message.
    /// PT: Garante que divergência entre número de colunas e expressões em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesColumnCountMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("column count", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rows with inconsistent expression counts are rejected with actionable message.
    /// PT: Garante que linhas de INSERT VALUES com cardinalidade inconsistente de expressões sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesRowArityMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 'a'), (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 2", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT remains rejected by SQL Server dialect gate even when malformed.
    /// PT: Garante que ON CONFLICT do PostgreSQL continue rejeitado pelo gate de dialeto SQL Server mesmo malformado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnConflictTarget_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (, id) DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target interrupted by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que alvo de ON CONFLICT interrompido por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetUnclosedBeforeSemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed ON CONFLICT DO UPDATE SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnConflictDoUpdateSet_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT without DO branch remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT sem ramo DO continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictMissingDoBranch_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with additional clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET ... RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET ... RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUnexpectedContinuation_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON CONFLICT target expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que expressão malformada no alvo de ON CONFLICT continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id +) DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE; DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id = DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothing_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingUnexpectedContinuationToken_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with additional WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula WHERE adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with table-source clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula de table-source continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by FROM remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetFromWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by USING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetUsingWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON CONFLICT DO UPDATE SET assignment expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que expressão de atribuição malformada em ON CONFLICT DO UPDATE SET continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetInvalidAssignmentExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = (EXCLUDED.name WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que atribuição em ON CONFLICT DO UPDATE SET sem sinal de igual continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without name remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutName_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without name at end-of-statement remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome no fim do statement continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutNameAtEndOfStatement_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without DO branch remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem ramo DO continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutDoBranch_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO with invalid continuation remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO com continuação inválida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoInvalidContinuation_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO SKIP";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE without SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE sem SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithoutSet_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET without assignments remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET sem atribuições continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with leading comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula inicial continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with trailing comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula final continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignments without comma separator remain rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuições sem separador por vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetAssignmentsWithoutCommaSeparator_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuição sem sinal de igual continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with malformed assignment expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com expressão de atribuição malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetInvalidAssignmentExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = (EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by FROM remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetFromWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by USING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetUsingWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothing_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with additional WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula WHERE adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUnexpectedContinuationToken_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE; DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE malformed before DO UPDATE SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE malformado antes do DO UPDATE SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereInvalidPredicateBeforeDoUpdate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO UPDATE SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereOnlySemicolonWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in UPDATE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados em UPDATE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in UPDATE remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no UPDATE continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Verifies UPDATE parsing keeps SET subquery text and WHERE boundary intact when FROM contains joins.
    /// PT: Verifica que o parsing de UPDATE mantém o texto da subquery no SET e o limite do WHERE intacto quando o FROM contém joins.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithSubqueryInSetAndFromJoin_ShouldKeepSetAndWhereBoundaries(int version)
    {
        var sql = @"UPDATE u
SET u.total = (SELECT SUM(o.amount) FROM orders o WHERE o.userid = u.id)
FROM users u
JOIN (SELECT userid FROM orders GROUP BY userid) s ON s.userid = u.id
WHERE u.id > 0";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.NotNull(parsed.UpdateFromSelect);
        Assert.Single(parsed.Set);
        Assert.Contains("SELECT SUM(o.amount) FROM orders", parsed.Set[0].ExprRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("u.id > 0", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser rejects MySQL full-text MATCH ... AGAINST syntax.
    /// PT: Garante que o parser SQL Server rejeite sintaxe full-text MATCH ... AGAINST do MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_MatchAgainst_ShouldBeRejectedByDialectGate(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("MATCH(name) AGAINST ('john' IN BOOLEAN MODE)", dialect));

        Assert.Contains("MATCH ... AGAINST", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
