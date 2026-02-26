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
    /// EN: Verifies LIMIT syntax in SQL Server returns an actionable pagination hint.
    /// PT: Verifica que sintaxe LIMIT no SQL Server retorna dica acionável de paginação.
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

}
