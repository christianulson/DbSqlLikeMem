namespace DbSqlLikeMem.Sqlite.Test.Parser;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class SqliteDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures SQLite preserves binary column size metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o SQLite preserve o metadado de tamanho de coluna binaria no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseAlterTableAddBinaryColumn_ShouldPreserveSize(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD payload VARBINARY(16) NULL",
            new SqliteDialect(version)));

        Assert.Equal(DbType.Binary, parsed.ColumnType);
        Assert.Equal(16, parsed.Size);
        Assert.True(parsed.Nullable);
    }

    /// <summary>
    /// EN: Ensures SQLite preserves DECIMAL precision and scale metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o SQLite preserve os metadados de precisao e escala de DECIMAL no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseAlterTableAddDecimalColumn_ShouldPreservePrecisionAndScale(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD amount DECIMAL(10, 4) NOT NULL DEFAULT 0",
            new SqliteDialect(version)));

        Assert.Equal(DbType.Decimal, parsed.ColumnType);
        Assert.Equal(10, parsed.Size);
        Assert.Equal(4, parsed.DecimalPlaces);
        Assert.False(parsed.Nullable);
        Assert.Equal("0", parsed.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures SQLite rejects ALTER TABLE ... ADD when NOT NULL is paired with DEFAULT NULL outside the pragmatic subset.
    /// PT: Garante que o SQLite rejeite ALTER TABLE ... ADD quando NOT NULL e combinado com DEFAULT NULL fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseAlterTableAddColumn_NotNullWithDefaultNull_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users ADD status VARCHAR(20) NOT NULL DEFAULT NULL",
            new SqliteDialect(version)));

        Assert.Contains("default null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQLite rejects ALTER TABLE ... ADD when the table reference uses an alias outside the pragmatic subset.
    /// PT: Garante que o SQLite rejeite ALTER TABLE ... ADD quando a referencia da tabela usa alias fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseAlterTableAddColumn_WithTableAlias_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users u ADD age INT",
            new SqliteDialect(version)));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQLite rejects ALTER TABLE ... ADD when the table reference is a derived source outside the pragmatic subset.
    /// PT: Garante que o SQLite rejeite ALTER TABLE ... ADD quando a referencia da tabela e uma fonte derivada fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseAlterTableAddColumn_WithDerivedTable_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE (SELECT * FROM users) u ADD age INT",
            new SqliteDialect(version)));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQLite keeps scalar FUNCTION DDL blocked because the real provider does not expose SQL-defined function DDL.
    /// PT: Garante que o SQLite mantenha DDL de FUNCTION escalar bloqueado porque o provider real nao expoe DDL SQL para funcao.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalarFunctionDdlSubset_ShouldRespectDialectRule(int version)
    {
        var dialect = new SqliteDialect(version);

        var createEx = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users() RETURNS INT AS BEGIN RETURN 40 + 2 END",
            dialect));
        Assert.Contains("CREATE FUNCTION", createEx.Message, StringComparison.OrdinalIgnoreCase);

        var dropEx = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
            "DROP FUNCTION IF EXISTS fn_users",
            dialect));
        Assert.Contains("DROP FUNCTION", dropEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQLite exposes CHANGES() through the dialect capability used by the executor.
    /// PT: Garante que o SQLite exponha CHANGES() pela capability de dialeto usada pelo executor.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void LastFoundRowsCapability_ShouldExposeSqliteFunction(int version)
    {
        var dialect = new SqliteDialect(version);

        Assert.True(dialect.SupportsLastFoundRowsFunction("CHANGES"));
        Assert.False(dialect.SupportsLastFoundRowsFunction("ROW_COUNT"));
        Assert.False(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
    }

    /// <summary>
    /// EN: Ensures SQLite parser accepts CHANGES() and rejects foreign row-count helper aliases.
    /// PT: Garante que o parser SQLite aceite CHANGES() e rejeite aliases de row-count de outros bancos.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_LastFoundRowsFunctions_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqliteDialect(version);

        Assert.Equal("CHANGES", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CHANGES()", dialect)).Name, StringComparer.OrdinalIgnoreCase);

        var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", dialect));
        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

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
    /// EN: Ensures INSERT ... RETURNING captures projection payload in AST for SQLite dialect.
    /// PT: Garante que INSERT ... RETURNING capture o payload de projeção na AST para o dialeto SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseInsert_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name AS user_name";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("user_name", parsed.Returning[1].Alias);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING captures projection payload in AST for SQLite dialect.
    /// PT: Garante que UPDATE ... RETURNING capture o payload de projeção na AST para o dialeto SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseUpdate_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id, name";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING with qualified wildcard preserves projection item in AST.
    /// PT: Garante que UPDATE ... RETURNING com wildcard qualificado preserve o item de projeção na AST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseUpdate_ReturningQualifiedWildcard_ShouldCaptureReturningItem(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING users.*";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Single(parsed.Returning);
        Assert.Equal("users.*", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING captures projection payload in AST for SQLite dialect.
    /// PT: Garante que DELETE ... RETURNING capture o payload de projeção na AST para o dialeto SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseDelete_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var parsed = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.Parse(sql, new SqliteDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
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
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
[Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new SqliteDialect(version);

        var limitOffset = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
            dialect));
        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));

        var normalizedLimit = Assert.IsType<SqlLimitOffset>(limitOffset.RowLimit);
        var normalizedFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);

        Assert.Equal(normalizedLimit, normalizedFetch);
        Assert.Equal(new LiteralExpr(2), normalizedFetch.Count);
        Assert.Equal(new LiteralExpr(1), normalizedFetch.Offset);
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



    /// <summary>
    /// EN: Verifies DELETE without FROM returns an actionable error message.
    /// PT: Verifica que DELETE sem FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new SqliteDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE target alias before FROM returns an actionable error message.
    /// PT: Verifica que alias alvo de DELETE antes de FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new SqliteDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures SQLite rejects unsupported alias quoting style with an actionable message.
    /// PT: Garante que o SQLite rejeite estilo de quoting de alias não suportado com mensagem acionável.
    /// </summary>
    /// <param name="version">EN: SQLite dialect version under test. PT: Versão do dialeto SQLite em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithBracketQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name [User Name] FROM users", new SqliteDialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'['", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
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
    /// EN: Ensures SQLite accepts backtick-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o SQLite aceite aliases com crase e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: SQLite dialect version under test. PT: Versão do dialeto SQLite em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name `User Name` FROM users",
            new SqliteDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures SQLite unescapes doubled backticks inside backtick-quoted aliases when normalizing AST alias text.
    /// PT: Garante que o SQLite faça unescape de crases duplicadas dentro de aliases com crase ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: SQLite dialect version under test. PT: Versão do dialeto SQLite em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_WithEscapedBacktickQuotedAlias_ShouldNormalizeEscapedBacktick(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name `User``Name` FROM users",
            new SqliteDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User`Name", item.Alias);
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



    /// <summary>
    /// EN: Verifies MERGE in unsupported dialect returns actionable migration guidance.
    /// PT: Verifica que MERGE em dialeto não suportado retorna orientação acionável de migração.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
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

    /// <summary>
    /// EN: Ensures SQLite parser accepts native ORDER BY inside GROUP_CONCAT.
    /// PT: Garante que o parser SQLite aceite ORDER BY nativo dentro de GROUP_CONCAT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_GroupConcatOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new SqliteDialect(version);

        var expr = SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|' ORDER BY amount DESC, id ASC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("GROUP_CONCAT", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Equal(2, call.WithinGroupOrderBy!.Count);
        Assert.True(call.WithinGroupOrderBy[0].Desc);
        Assert.False(call.WithinGroupOrderBy[1].Desc);
    }

    /// <summary>
    /// EN: Ensures SQLite parser preserves DISTINCT when native ORDER BY is used inside GROUP_CONCAT.
    /// PT: Garante que o parser SQLite preserve DISTINCT quando ORDER BY nativo e usado dentro de GROUP_CONCAT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_GroupConcatDistinctOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new SqliteDialect(version);

        var expr = SqlExpressionParser.ParseScalar("GROUP_CONCAT(DISTINCT amount ORDER BY amount DESC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.True(call.Distinct);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy[0].Desc);
    }

    /// <summary>
    /// EN: Ensures malformed native aggregate ORDER BY in SQLite fails with actionable message.
    /// PT: Garante que ORDER BY nativo malformado em agregacao SQLite falhe com mensagem acionavel.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_GroupConcatOrderByInsideCallTrailingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|' ORDER BY amount DESC,)", dialect));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SELECT parsing accepts SQLite native GROUP_CONCAT ordering syntax.
    /// PT: Garante que o parsing de SELECT aceite a sintaxe nativa de ordenacao do GROUP_CONCAT no SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseSelect_GroupConcatOrderByInsideCall_ShouldParse(int version)
    {
        var dialect = new SqliteDialect(version);

        var parsed = Assert.IsType<SqlSelectQuery>(
            SqlQueryParser.Parse("SELECT GROUP_CONCAT(amount, '|' ORDER BY amount DESC) AS joined FROM orders", dialect));

        Assert.Single(parsed.SelectItems);
        Assert.Contains("GROUP_CONCAT", parsed.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITHIN GROUP ordered-set syntax remains unsupported for SQLite aggregates.
    /// PT: Garante que a sintaxe ordered-set WITHIN GROUP continue não suportada para agregações SQLite.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP syntax in SQLite still fails as not-supported (dialect gate precedence).
    /// PT: Garante que sintaxe malformada de WITHIN GROUP no SQLite continue falhando como não suportada (precedência do gate de dialeto).
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroupMalformed_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed trailing comma in WITHIN GROUP remains blocked by dialect gate.
    /// PT: Garante que vírgula final malformada no WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroupTrailingComma_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ORDER BY list in WITHIN GROUP remains blocked by dialect gate.
    /// PT: Garante que lista ORDER BY vazia em WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByEmptyList_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading commas in WITHIN GROUP ORDER BY remain blocked by dialect gate.
    /// PT: Garante que vírgulas iniciais no ORDER BY do WITHIN GROUP continuem bloqueadas pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByLeadingComma_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures missing commas in malformed WITHIN GROUP ORDER BY remain blocked by dialect gate.
    /// PT: Garante que ausência de vírgula em ORDER BY malformado no WITHIN GROUP continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqliteVersion]
    public void ParseScalar_StringAggregateWithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqliteDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("WITHIN GROUP", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
