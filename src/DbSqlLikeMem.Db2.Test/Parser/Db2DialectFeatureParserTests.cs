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
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithRecursive_ShouldFollowDb2VersionSupport(int version)
    {
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1 FROM SYSIBM.SYSDUMMY1) SELECT n FROM cte";

        if (version < Db2Dialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new Db2Dialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

    /// <summary>
    /// EN: Tests ParseInsert_OnConflict_ShouldBeRejected behavior.
    /// PT: Testa o comportamento de ParseInsert_OnConflict_ShouldBeRejected.
    /// </summary>
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseInsert_OnConflict_ShouldBeRejected(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE parsing follows DB2 version support and preserves target table metadata.
    /// PT: Garante que o parsing de MERGE siga o suporte por versão do DB2 e preserve metadados da tabela alvo.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseMerge_ShouldFollowDb2VersionSupport(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < Db2Dialect.MergeMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new Db2Dialect(version)));
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
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithWhenNotMatched_ShouldParse(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id)";

        var query = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Equal("users", query.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures MERGE without USING is rejected with actionable parser guidance in DB2 dialect.
    /// PT: Garante que MERGE sem USING seja rejeitado com orientação acionável do parser no dialeto DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithoutUsing_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target ON target.id = 1 WHEN MATCHED THEN UPDATE SET name = 'x'", new Db2Dialect(version)));

        Assert.Contains("MERGE requer cláusula USING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without ON is rejected with actionable parser guidance in DB2 dialect.
    /// PT: Garante que MERGE sem ON seja rejeitado com orientação acionável do parser no dialeto DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithoutOn_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src WHEN MATCHED THEN UPDATE SET name = 'x'", new Db2Dialect(version)));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires ON at top-level and does not accept ON tokens nested inside USING subqueries in DB2 dialect.
    /// PT: Garante que MERGE exija ON em nível top-level e não aceite tokens ON aninhados dentro de subqueries no USING no dialeto DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithOnOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING (SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE id > 0)) src WHEN MATCHED THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without WHEN is rejected with actionable parser guidance in DB2 dialect.
    /// PT: Garante que MERGE sem WHEN seja rejeitado com orientação acionável do parser no dialeto DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithoutWhen_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src ON target.id = src.id", new Db2Dialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures MERGE does not accept a source alias named WHEN as a replacement for top-level WHEN clauses.
    /// PT: Garante que MERGE não aceite um alias de origem chamado WHEN como substituto para cláusulas WHEN em nível top-level.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithUsingAliasNamedWhen_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING users when ON target.id = when.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires WHEN at top-level and does not accept WHEN tokens nested inside USING subqueries.
    /// PT: Garante que MERGE exija WHEN em nível top-level e não aceite tokens WHEN aninhados dentro de subqueries no USING.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithWhenOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING (SELECT CASE WHEN id > 0 THEN id ELSE 0 END AS id FROM users) src ON target.id = src.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE rejects invalid top-level WHEN forms that are not WHEN MATCHED/WHEN NOT MATCHED.
    /// PT: Garante que MERGE rejeite formas inválidas de WHEN em nível top-level que não sejam WHEN MATCHED/WHEN NOT MATCHED.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version(VersionGraterOrEqual = Db2Dialect.MergeMinVersion)]
    public void ParseMerge_WithInvalidTopLevelWhenForm_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN src.id > 0 THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Tests ParseSelect_WithMySqlIndexHints_ShouldBeRejected behavior.
    /// PT: Testa o comportamento de ParseSelect_WithMySqlIndexHints_ShouldBeRejected.
    /// </summary>
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithMySqlIndexHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users USE INDEX (idx_users_id)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for DB2.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para DB2.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Use hints compatíveis", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DB2 rejects unsupported alias quoting style with an actionable message.
    /// PT: Garante que o DB2 rejeite estilo de quoting de alias não suportado com mensagem acionável.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", new Db2Dialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new Db2Dialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DB2 accepts double-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o DB2 aceite aliases com aspas duplas e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithDoubleQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User Name\" FROM users",
            new Db2Dialect(version)));

        var item = Assert.Single(parsed.Items);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures DB2 unescapes doubled double-quotes inside quoted aliases when normalizing AST alias text.
    /// PT: Garante que o DB2 faça unescape de aspas duplas duplicadas dentro de aliases quoted ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithEscapedDoubleQuotedAlias_ShouldNormalizeEscapedQuote(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            new Db2Dialect(version)));

        var item = Assert.Single(parsed.Items);
        Assert.Equal("User\"Name", item.Alias);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users USE INDEX (idx_users_id)", new Db2Dialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("db2", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
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

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
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

    /// <summary>
    /// EN: Ensures OFFSET/FETCH pagination is accepted by DB2 parser.
    /// PT: Garante que paginação OFFSET/FETCH seja aceita pelo parser DB2.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithOffsetFetch_ShouldParse(int version)
    {
        var sql = "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var parsed = SqlQueryParser.Parse(sql, new Db2Dialect(version));
        Assert.IsType<SqlSelectQuery>(parsed);
    }

        /// <summary>
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new Db2Dialect(version);

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
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new Db2Dialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("db2", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies DELETE without FROM returns an actionable error message.
    /// PT: Verifica que DELETE sem FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new Db2Dialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE target alias before FROM returns an actionable error message.
    /// PT: Verifica que alias alvo de DELETE antes de FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: DB2 dialect version under test. PT: Versão do dialeto DB2 em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new Db2Dialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = new Db2Dialect(version);

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.False(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }

    /// <summary>
    /// EN: Validates known and unknown window function capability for DB2 dialect versions.
    /// PT: Valida a capacidade de funções de janela conhecidas e desconhecidas nas versões do dialeto DB2.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void WindowFunctionCapability_ShouldAllowKnownAndRejectUnknownFunctions(int version)
    {
        var dialect = new Db2Dialect(version);

        Assert.True(dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.True(dialect.SupportsWindowFunction("CUME_DIST"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser accepts known window functions and rejects unknown names for DB2 dialect versions.
    /// PT: Garante que o parser aceite funções de janela conhecidas e rejeite nomes desconhecidos nas versões do DB2.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataDb2Version]
    public void ParseScalar_WindowFunctionName_ShouldAllowKnownAndRejectUnknown(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new Db2Dialect(version);

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
    [MemberDataDb2Version]
    public void ParseScalar_WindowFrameClause_ShouldBeRejectedByDialectCapability(int version)
    {
        var dialect = new Db2Dialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));

        Assert.Contains("window frame", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
