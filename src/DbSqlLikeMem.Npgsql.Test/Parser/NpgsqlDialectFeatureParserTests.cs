namespace DbSqlLikeMem.Npgsql.Test.Parser;

/// <summary>
/// EN: Covers PostgreSQL/Npgsql-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos de PostgreSQL/Npgsql.
/// </summary>
public sealed class NpgsqlDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures PostgreSQL preserves binary column size metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o PostgreSQL preserve o metadado de tamanho de coluna binaria no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterTableAddBinaryColumn_ShouldPreserveSize(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE public.users ADD payload VARBINARY(16) NULL",
            new NpgsqlDialect(version)));

        Assert.Equal(DbType.Binary, parsed.ColumnType);
        Assert.Equal(16, parsed.Size);
        Assert.True(parsed.Nullable);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL preserves DECIMAL precision and scale metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o PostgreSQL preserve os metadados de precisao e escala de DECIMAL no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterTableAddDecimalColumn_ShouldPreservePrecisionAndScale(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE public.users ADD amount DECIMAL(10, 4) NOT NULL DEFAULT 0",
            new NpgsqlDialect(version)));

        Assert.Equal(DbType.Decimal, parsed.ColumnType);
        Assert.Equal(10, parsed.Size);
        Assert.Equal(4, parsed.DecimalPlaces);
        Assert.False(parsed.Nullable);
        Assert.Equal("0", parsed.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL rejects ALTER TABLE ... ADD when NOT NULL is paired with DEFAULT NULL outside the pragmatic subset.
    /// PT: Garante que o PostgreSQL rejeite ALTER TABLE ... ADD quando NOT NULL e combinado com DEFAULT NULL fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterTableAddColumn_NotNullWithDefaultNull_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE public.users ADD status VARCHAR(20) NOT NULL DEFAULT NULL",
            new NpgsqlDialect(version)));

        Assert.Contains("default null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL rejects ALTER TABLE ... ADD when the table reference uses an alias outside the pragmatic subset.
    /// PT: Garante que o PostgreSQL rejeite ALTER TABLE ... ADD quando a referencia da tabela usa alias fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterTableAddColumn_WithTableAlias_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE public.users u ADD age INT",
            new NpgsqlDialect(version)));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL rejects ALTER TABLE ... ADD when the table reference is a derived source outside the pragmatic subset.
    /// PT: Garante que o PostgreSQL rejeite ALTER TABLE ... ADD quando a referencia da tabela e uma fonte derivada fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseAlterTableAddColumn_WithDerivedTable_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE (SELECT * FROM public.users) u ADD age INT",
            new NpgsqlDialect(version)));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL parses the pragmatic provider-real scalar FUNCTION DDL subset.
    /// PT: Garante que o PostgreSQL interprete o subset pragmatico e realista do provider para DDL de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalarFunctionDdlSubset_ShouldParse(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(base_value integer, increment_value integer) RETURNS integer AS 'SELECT base_value + increment_value' LANGUAGE SQL",
            dialect));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal("integer", create.ReturnTypeSql, ignoreCase: true);
        Assert.Equal(2, create.Parameters.Count);
        Assert.Equal("base_value", create.Parameters[0].Name, ignoreCase: true);
        Assert.Equal("increment_value", create.Parameters[1].Name, ignoreCase: true);
        Assert.IsType<BinaryExpr>(create.Body);

        var drop = Assert.IsType<SqlDropFunctionQuery>(SqlQueryParser.Parse(
            "DROP FUNCTION IF EXISTS fn_users(integer, integer)",
            dialect));

        Assert.True(drop.IfExists);
        Assert.Equal("fn_users", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL parses CREATE OR REPLACE FUNCTION in the supported provider-real subset.
    /// PT: Garante que o PostgreSQL interprete CREATE OR REPLACE FUNCTION no subset realista suportado pelo provider.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseCreateOrReplaceScalarFunctionDdlSubset_ShouldParse(int version)
    {
        var dialect = new NpgsqlDialect(version);
        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE OR REPLACE FUNCTION fn_users(base_value integer, increment_value integer) RETURNS integer AS 'SELECT base_value + increment_value + 1' LANGUAGE SQL",
            dialect));
        Assert.True(create.OrReplace);
        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(2, create.Parameters.Count);
        Assert.IsType<BinaryExpr>(create.Body);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL accepts ILIKE and keeps the case-insensitive flag in the AST.
    /// PT: Garante que o PostgreSQL aceite ILIKE e mantenha a flag case-insensitive na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_Ilike_ShouldParse(int version)
    {
        var expr = SqlExpressionParser.ParseScalar("name ILIKE 'jo%'", new NpgsqlDialect(version));
        var like = Assert.IsType<LikeExpr>(expr);

        Assert.True(like.CaseInsensitive);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL sequence function calls are parsed through dialect-owned capabilities.
    /// PT: Garante que chamadas de funcao de sequence do PostgreSQL sejam interpretadas por capabilities do dialeto.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_SequenceFunctionCalls_ShouldParse(int version)
    {
        var dialect = new NpgsqlDialect(version);

        Assert.Equal("NEXTVAL", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("nextval('sales.seq_orders')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CURRVAL", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("currval('sales.seq_orders')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SETVAL", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("setval('sales.seq_orders', 30, false)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LASTVAL", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("lastval()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL row-count helper stays owned by the dialect capability used by the executor.
    /// PT: Garante que o helper de row-count do PostgreSQL continue pertencendo à capability de dialeto usada pelo executor.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void LastFoundRowsCapability_ShouldExposePostgreSqlFunction(int version)
    {
        var dialect = new NpgsqlDialect(version);

        Assert.True(dialect.SupportsLastFoundRowsFunction("ROW_COUNT"));
        Assert.False(dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"));
        Assert.False(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL exposes its join-based mutation syntax through dialect-owned capabilities.
    /// PT: Garante que o PostgreSQL exponha sua sintaxe de mutacao com join por capabilities do proprio dialeto.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void MutationCapabilities_ShouldExposePostgreSqlContract(int version)
    {
        var dialect = new NpgsqlDialect(version);

        Assert.False(dialect.SupportsUpdateJoinFromSubquerySyntax);
        Assert.True(dialect.SupportsUpdateFromJoinSubquerySyntax);
        Assert.False(dialect.SupportsDeleteTargetFromJoinSubquerySyntax);
        Assert.True(dialect.SupportsDeleteUsingSubquerySyntax);
        Assert.False(dialect.SupportsSqlCalcFoundRowsModifier);
        Assert.Equal(2, dialect.GetInsertUpsertAffectedRowCount(1, 1));
    }

    /// <summary>
    /// EN: Ensures PostgreSQL parser rejects SQL_CALC_FOUND_ROWS because the modifier belongs to MySQL.
    /// PT: Garante que o parser PostgreSQL rejeite SQL_CALC_FOUND_ROWS porque o modificador pertence ao MySQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_SqlCalcFoundRows_ShouldRespectDialectRule(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT SQL_CALC_FOUND_ROWS name FROM users LIMIT 1", new NpgsqlDialect(version)));

        Assert.Contains("SQL_CALC_FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL parser accepts ROW_COUNT() and rejects foreign row-count helper aliases.
    /// PT: Garante que o parser PostgreSQL aceite ROW_COUNT() e rejeite aliases de row-count de outros bancos.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versao do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_LastFoundRowsFunctions_ShouldFollowDialectCapability(int version)
    {
        var dialect = new NpgsqlDialect(version);

        Assert.Equal("ROW_COUNT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROW_COUNT()", dialect)).Name, StringComparer.OrdinalIgnoreCase);

        var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", dialect));
        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING is parsed as duplicate-key handling.
    /// PT: Garante que ON CONFLICT DO NOTHING seja interpretado como tratamento de chave duplicada.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothing_ShouldParse(int version)
    {
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Empty(ins.OnDupAssigns);
        Assert.True(ins.IsOnConflictDoNothing);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT DO NOTHING com RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithReturning_ShouldParse(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET ... RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT DO UPDATE SET ... RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateWithReturning_ShouldParse(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.False(parsed.IsOnConflictDoNothing);
        Assert.Single(parsed.OnDupAssigns);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET ... RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET ... RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MySQL ON DUPLICATE KEY UPDATE syntax is rejected for Npgsql with actionable guidance.
    /// PT: Garante que a sintaxe ON DUPLICATE KEY UPDATE do MySQL seja rejeitada no Npgsql com orientação acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdate_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with RETURNING still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithReturningClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with malformed RETURNING expression still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com expressão malformada em RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithInvalidReturningExpression_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with empty RETURNING list still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com lista vazia em RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithEmptyReturningList_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with unbalanced parentheses in RETURNING still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com parênteses desbalanceados em RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithUnbalancedReturningExpression_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with leading comma in RETURNING still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula inicial em RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithLeadingCommaReturning_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING, id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with trailing comma in RETURNING still provides Npgsql guidance to use ON CONFLICT.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula final em RETURNING continue fornecendo guidance no Npgsql para uso de ON CONFLICT.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithTrailingCommaReturning_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed MySQL ON DUPLICATE KEY UPDATE variant still provides Npgsql guidance.
    /// PT: Garante que variante malformada de ON DUPLICATE KEY UPDATE do MySQL continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithWhereClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with WHERE clause still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula WHERE continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithWhereClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and followed by RETURNING still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e seguido por RETURNING continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithReturningClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with empty RETURNING list still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com lista vazia em RETURNING continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithEmptyReturningList_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with unbalanced RETURNING expression still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com expressão RETURNING desbalanceada continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithUnbalancedReturningExpression_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with FROM clause still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula FROM continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithFromClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with USING clause still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula USING continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithoutAssignmentsWithUsingClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with USING clause still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula USING continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithUsingClause_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with repeated SET keyword still provides Npgsql guidance.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com palavra-chave SET repetida continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateWithRepeatedSetKeyword_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE SET name = VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignment without equals still provides Npgsql guidance.
    /// PT: Garante que atribuição em ON DUPLICATE KEY UPDATE sem sinal de igual continue fornecendo guidance no Npgsql.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnDuplicateKeyUpdateAssignmentWithoutEquals_ShouldProvideNpgsqlGuidance(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name VALUES(name)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL keeps DROP INDEX ... ON &lt;table&gt; blocked because the ON table clause is not part of its shared subset.
    /// PT: Garante que o PostgreSQL mantenha DROP INDEX ... ON &lt;table&gt; bloqueado porque a clausula ON table nao faz parte do subset compartilhado dele.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDropIndex_WithOnTableClause_ShouldBeRejectedByDialectGate(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("DROP INDEX ix_users_name ON public.users", new NpgsqlDialect(version)));

        Assert.Contains("DROP INDEX", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ON <table>", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MATERIALIZED CTE syntax is accepted.
    /// PT: Garante que a sintaxe de CTE MATERIALIZED seja aceita.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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
    [Trait("Category", "Parser")]
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
        Assert.Single(ins.OnDupAssignsParsed);
        Assert.NotNull(ins.OnDupAssignsParsed[0].ValueExpr);
        Assert.Equal("name", ins.OnDupAssigns[0].Col);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE rejects FROM clause with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE rejeite cláusula FROM com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithFromClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE rejects USING clause with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE rejeite cláusula USING com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithUsingClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by FROM is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateSetFromWithoutAssignments_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by USING is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateSetUsingWithoutAssignments_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name
RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.False(parsed.IsOnConflictDoNothing);
        Assert.Single(parsed.OnDupAssigns);
        Assert.Single(parsed.OnDupAssignsParsed);
        Assert.NotNull(parsed.OnDupAssignsParsed[0].ValueExpr);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoUpdateWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO UPDATE SET name = EXCLUDED.name
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING remains valid.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING permaneça válido.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_DoNothing_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO NOTHING";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Empty(parsed.Returning);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_DoNothingWithReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO NOTHING
RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_DoNothingWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO NOTHING
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_DoNothingWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO NOTHING
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_DoNothingWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO NOTHING
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with FROM clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithFromClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with USING clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithUsingClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with SET clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithSetClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with UPDATE clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula UPDATE seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithUpdateClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
UPDATE SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'UPDATE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with additional WHERE clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula WHERE adicional seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithWhereClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with unexpected continuation token is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com token de continuação inesperado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_DoNothingWithUnexpectedContinuationToken_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey
DO NOTHING
EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'EXTRA'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING are parsed together.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING sejam interpretados em conjunto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_UpdateWhere_Returning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Single(ins.OnDupAssignsParsed);
        Assert.NotNull(ins.OnDupAssignsParsed[0].ValueExpr);
        Assert.Contains("users.id", ins.OnConflictUpdateWhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ins.OnConflictUpdateWhereExpr);
        Assert.Single(ins.Returning);
        Assert.Equal("id", ins.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_UpdateWhere_InvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_UpdateWhere_UnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_UpdateWhere_EmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE are parsed together even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE sejam interpretados em conjunto mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_OnConstraint_TargetWhere_UpdateWhere_WithoutReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Single(ins.OnDupAssignsParsed);
        Assert.NotNull(ins.OnDupAssignsParsed[0].ValueExpr);
        Assert.Contains("users.id", ins.OnConflictUpdateWhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ins.OnConflictUpdateWhereExpr);
        Assert.Empty(ins.Returning);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE, update WHERE, and RETURNING are parsed together.
    /// PT: Garante que WHERE no alvo do conflito, WHERE do update e RETURNING sejam interpretados em conjunto.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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
        Assert.Single(ins.OnDupAssignsParsed);
        Assert.NotNull(ins.OnDupAssignsParsed[0].ValueExpr);
        Assert.Contains("users.id", ins.OnConflictUpdateWhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ins.OnConflictUpdateWhereExpr);
        Assert.Single(ins.Returning);
        Assert.Equal("id", ins.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE + update WHERE + RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que target WHERE + update WHERE + RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_InvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE + update WHERE + RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que target WHERE + update WHERE + RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_UnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE + update WHERE + RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que target WHERE + update WHERE + RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_EmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures conflict target WHERE and update WHERE are parsed together even without RETURNING.
    /// PT: Garante que WHERE no alvo do conflito e WHERE do update sejam interpretados em conjunto mesmo sem RETURNING.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_UpdateWhere_WithoutReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));

        var ins = Assert.IsType<SqlInsertQuery>(parsed);
        Assert.True(ins.HasOnDuplicateKeyUpdate);
        Assert.Single(ins.OnDupAssigns);
        Assert.Single(ins.OnDupAssignsParsed);
        Assert.NotNull(ins.OnDupAssignsParsed[0].ValueExpr);
        Assert.Contains("users.id", ins.OnConflictUpdateWhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.NotNull(ins.OnConflictUpdateWhereExpr);
        Assert.Empty(ins.Returning);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING remains valid.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING permaneça válido.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothing_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Empty(parsed.Returning);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING remains valid and captures projection.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING permaneça válido e capture a projeção.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithReturning_ShouldParse(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.OnDupAssigns);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with unexpected continuation token is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com token de continuação inesperado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithUnexpectedContinuationToken_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'EXTRA'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with FROM clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithFromClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with USING clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithUsingClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with SET clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithSetClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with UPDATE clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula UPDATE seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithUpdateClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
UPDATE SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'UPDATE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with additional WHERE clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula WHERE adicional seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetWhere_DoNothingWithWhereClause_ShouldThrowActionableError(int version)
    {
        var sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO NOTHING
WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ON CONFLICT target list is rejected with actionable message.
    /// PT: Garante que lista vazia no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_EmptyTarget_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT () DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ')'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (, id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final no alvo de ON CONFLICT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id,) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ')'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target interrupted by semicolon is rejected with actionable message.
    /// PT: Garante que alvo de ON CONFLICT interrompido por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_TargetUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET with empty assignment list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET com lista vazia de atribuições seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetEmptyAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by RETURNING is rejected with actionable token context.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por RETURNING seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetWithoutAssignmentsBeforeReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment without expression is rejected with actionable message.
    /// PT: Garante que atribuição sem expressão em ON CONFLICT DO UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetAssignmentWithoutExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires an expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em ON CONFLICT DO UPDATE SET sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetMissingCommaBetweenAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET accepts semicolon statement boundary after assignment list.
    /// PT: Garante que ON CONFLICT DO UPDATE SET aceite fronteira de statement por ponto e vírgula após lista de atribuições.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateSetWithSemicolonBoundary_ShouldParse(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Single(parsed.OnDupAssigns);
        Assert.Equal("name", parsed.OnDupAssigns[0].Col);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT without DO branch is rejected with actionable message.
    /// PT: Garante que ON CONFLICT sem ramo DO seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_WithoutDoBranch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires DO NOTHING or DO UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO with invalid continuation is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO com continuação inválida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoInvalidContinuation_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO SKIP";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must be followed by NOTHING or UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SKIP'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with additional clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula adicional seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithWhereClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with FROM clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithFromClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with USING clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithUsingClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with SET clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithSetClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with UPDATE clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula UPDATE seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithUpdateClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'UPDATE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with unexpected continuation token is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO NOTHING com token de continuação inesperado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoNothingWithUnexpectedContinuation_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO NOTHING does not support additional clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'EXTRA'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE without SET is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE sem SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflict_DoUpdateWithoutSet_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires SET assignments", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE without predicate is rejected with actionable message.
    /// PT: Garante que WHERE no alvo de ON CONFLICT sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'DO'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que WHERE no alvo de ON CONFLICT finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE; DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que WHERE no alvo de ON CONFLICT com predicado malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetWhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id = DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target with malformed expression is rejected with actionable message.
    /// PT: Garante que alvo de ON CONFLICT com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictTargetInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id +) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without constraint name is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome da constraint seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutName_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires a constraint name", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'DO'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without name and at end-of-statement is rejected with actionable token context.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome no fim do statement seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutNameAtEndOfStatement_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires a constraint name", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without DO branch is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem ramo DO seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintWithoutDoBranch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires DO NOTHING or DO UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO with invalid continuation is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO com continuação inválida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoInvalidContinuation_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO SKIP";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must be followed by NOTHING or UPDATE SET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SKIP'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE without SET is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE sem SET seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWithoutSet_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires SET assignments", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET without assignments is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET sem atribuições seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetWithoutAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by RETURNING is rejected with actionable token context.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por RETURNING seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetWithoutAssignmentsBeforeReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with leading comma is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula inicial seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma before assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with trailing comma is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma without assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuições sem separador por vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetAssignmentsWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuição sem sinal de igual seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with malformed assignment expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com expressão de atribuição malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateSetInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = (EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE without predicate is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'DO'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE; DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE with malformed predicate is rejected before DO UPDATE SET branch.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo malformado seja rejeitado antes do ramo DO UPDATE SET.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintTargetWhereInvalidPredicateBeforeDoUpdate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO UPDATE SET name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("target WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon is rejected even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereOnlySemicolonWithoutReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and malformed RETURNING expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão malformada em RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and unbalanced RETURNING expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and empty RETURNING list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e lista vazia em RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictOnConstraintDoUpdateWhereEmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE terminated only by semicolon is rejected even without RETURNING.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE finalizado apenas por ponto e vírgula seja rejeitado mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereOnlySemicolonWithoutReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que WHERE em ON CONFLICT DO UPDATE com predicado malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DO UPDATE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and malformed RETURNING expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão malformada em RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and unbalanced RETURNING expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and empty RETURNING list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e lista vazia em RETURNING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWhereEmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with malformed expression is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com expressão malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithInvalidReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com parênteses desbalanceados seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithUnbalancedReturningExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with empty projection list is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com lista de projeção vazia seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithEmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with table-source clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula de table-source seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithFromClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by FROM is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por FROM seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateSetFromWithoutAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET FROM users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'FROM'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by USING is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateSetUsingWithoutAssignments_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with USING clause is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula USING seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateWithUsingClause_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("does not support table-source clauses", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'USING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment with malformed expression is rejected with actionable message.
    /// PT: Garante que atribuição em ON CONFLICT DO UPDATE SET com expressão malformada seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateSetInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = (EXCLUDED.name WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que ON CONFLICT DO UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateSetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em ON CONFLICT DO UPDATE SET sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_OnConflictDoUpdateSetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT ... RETURNING captures projection payload in AST for PostgreSQL dialect.
    /// PT: Garante que INSERT ... RETURNING capture o payload de projeção na AST para o dialeto PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name AS user_name";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("user_name", parsed.Returning[1].Alias);
    }


    /// <summary>
    /// EN: Ensures INSERT with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que INSERT com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after INSERT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1),";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuples without comma separator are rejected with actionable message.
    /// PT: Garante que tuplas em INSERT VALUES sem vírgula separadora sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTuplesWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1) (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("separate row tuples with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES , (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES with malformed scalar expression is rejected with actionable message.
    /// PT: Garante que INSERT VALUES com expressão escalar malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1 +, 'a')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("row 1 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES reports row/position for malformed expression in later rows.
    /// PT: Garante que INSERT VALUES reporte linha/posição para expressão malformada em linhas posteriores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesSecondRowInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a'), (2 +, 'b')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("row 2 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id,) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT empty column list is rejected with actionable message.
    /// PT: Garante que lista de colunas vazia no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_EmptyColumnList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users () VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("at least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (,id) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT column list unclosed before semicolon is rejected with actionable message.
    /// PT: Garante que lista de colunas do INSERT não fechada antes de ponto e vírgula seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ColumnListUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES empty row tuple is rejected with actionable message.
    /// PT: Garante que linha vazia em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesEmptyRowTuple_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES ()";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rejects empty expression between commas inside tuple.
    /// PT: Garante que INSERT VALUES rejeite expressão vazia entre vírgulas dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleMissingExpressionBetweenCommas_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,,2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES rejects trailing comma inside tuple.
    /// PT: Garante que INSERT VALUES rejeite vírgula final dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuple with unclosed parenthesis is rejected with actionable message.
    /// PT: Garante que tupla em INSERT VALUES com parêntese não fechado seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesTupleUnclosedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 2";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES row expression count matches target column count.
    /// PT: Garante que a quantidade de expressões em INSERT VALUES corresponda à quantidade de colunas alvo.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesColumnCountMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("column count", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rows with inconsistent expression counts are rejected with actionable message.
    /// PT: Garante que linhas de INSERT VALUES com cardinalidade inconsistente de expressões sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ValuesRowArityMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 'a'), (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("row 2", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT ... RETURNING is parsed without consuming RETURNING as SELECT tail.
    /// PT: Garante que INSERT ... SELECT ... RETURNING seja interpretado sem consumir RETURNING como cauda do SELECT.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_SelectReturning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) SELECT id, name FROM users RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT ... WHERE ... RETURNING preserves WHERE boundary and captures RETURNING projection.
    /// PT: Garante que INSERT ... SELECT ... WHERE ... RETURNING preserve o limite do WHERE e capture a projeção de RETURNING.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_SelectWhereReturning_ShouldPreserveWhereBoundary(int version)
    {
        const string sql = "INSERT INTO users (id, name) SELECT id, name FROM users WHERE id IN (1, 2) RETURNING id";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.NotNull(parsed.InsertSelect!.Where);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING keeps WHERE boundary and captures returning projection.
    /// PT: Garante que UPDATE ... RETURNING preserve o limite do WHERE e capture a projeção de retorno.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id, name";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }


    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING without WHERE keeps SET boundary and captures RETURNING projection.
    /// PT: Garante que UPDATE ... RETURNING sem WHERE preserve o limite do SET e capture a projeção de RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningWithoutWhere_ShouldCaptureReturningItems(int version)
    {
        const string sql = "UPDATE users SET name = 'b' RETURNING id";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Null(parsed.WhereRaw);
        Assert.Single(parsed.Set);
        Assert.Equal("'b'", parsed.Set[0].ExprRaw);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures valid UPDATE SET assignments materialize parsed scalar expressions in AST.
    /// PT: Garante que atribuições válidas de UPDATE SET materializem expressões escalares parseadas na AST.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetValidAssignments_ShouldMaterializeParsedExpressions(int version)
    {
        const string sql = "UPDATE users SET name = upper('b'), updated_at = now() WHERE id = 1";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Equal(2, parsed.SetParsed.Count);
        Assert.All(parsed.SetParsed, a => Assert.NotNull(a.ValueExpr));
    }

    /// <summary>
    /// EN: Ensures UPDATE SET without assignments and followed by RETURNING is rejected with actionable token context.
    /// PT: Garante que UPDATE SET sem atribuições e seguido por RETURNING seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetWithoutAssignmentsBeforeReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em UPDATE SET seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b', RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET leading comma is rejected with actionable token context.
    /// PT: Garante que vírgula inicial em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET , name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma before assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em UPDATE SET sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetAssignmentsWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' updated_at = NOW() WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment with malformed expression is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET com expressão malformada seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = (upper('a') WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_SetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET SET name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que UPDATE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE followed directly by RETURNING is rejected with actionable token context.
    /// PT: Garante que UPDATE com WHERE seguido diretamente por RETURNING seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereWithoutPredicateBeforeReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("UPDATE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING with qualified wildcard preserves projection item in AST.
    /// PT: Garante que UPDATE ... RETURNING com wildcard qualificado preserve o item de projeção na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningQualifiedWildcard_ShouldCaptureReturningItem(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING users.*";

        var parsed = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Single(parsed.Returning);
        Assert.Equal("users.*", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING captures projection payload in AST for PostgreSQL dialect.
    /// PT: Garante que DELETE ... RETURNING capture o payload de projeção na AST para o dialeto PostgreSQL.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_Returning_ShouldCaptureReturningItems(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var parsed = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("id = 1", parsed.WhereRaw, StringComparison.OrdinalIgnoreCase);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }


    /// <summary>
    /// EN: Ensures DELETE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que DELETE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("Unexpected token after DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE followed directly by RETURNING is rejected with actionable token context.
    /// PT: Garante que DELETE com WHERE seguido diretamente por RETURNING seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereWithoutPredicateBeforeReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE RETURNING id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'RETURNING'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("DELETE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures empty RETURNING clause is rejected with actionable message.
    /// PT: Garante que cláusula RETURNING vazia seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_EmptyReturning_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression is rejected with actionable token context.
    /// PT: Garante que alias em RETURNING sem expressão seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningAliasWithoutExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING AS user_id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'AS'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in INSERT is rejected with actionable message.
    /// PT: Garante que lista vazia em RETURNING no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_EmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in UPDATE is rejected with actionable message.
    /// PT: Garante que lista vazia em RETURNING no UPDATE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_EmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in DELETE is rejected with actionable message.
    /// PT: Garante que lista vazia em RETURNING no DELETE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_EmptyReturningList_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in UPDATE is rejected with actionable token context.
    /// PT: Garante que alias em RETURNING sem expressão no UPDATE seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningAliasWithoutExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'AS'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in DELETE is rejected with actionable token context.
    /// PT: Garante que alias em RETURNING sem expressão no DELETE seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningAliasWithoutExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'AS'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial no RETURNING seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING leading comma in INSERT is rejected with actionable message.
    /// PT: Garante que vírgula inicial no RETURNING de INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING, id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING trailing comma in INSERT is rejected with actionable message.
    /// PT: Garante que vírgula final no RETURNING de INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING trailing comma in UPDATE is rejected with actionable message.
    /// PT: Garante que vírgula final no RETURNING de UPDATE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final no RETURNING seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING leading comma in DELETE is rejected with actionable message.
    /// PT: Garante que vírgula inicial no RETURNING de DELETE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING supports nested expressions with commas and semicolon statement boundary.
    /// PT: Garante que RETURNING suporte expressões aninhadas com vírgulas e limite de statement por ponto e vírgula.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningNestedExpressionsWithSemicolon_ShouldCaptureItems(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING COALESCE((SELECT max(id) FROM users), 0) AS next_id, concat(name, ',x') AS decorated;";

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("COALESCE((SELECT max(id) FROM users), 0)", parsed.Returning[0].Raw);
        Assert.Equal("next_id", parsed.Returning[0].Alias);
        Assert.Equal("concat(name, ',x')", parsed.Returning[1].Raw);
        Assert.Equal("decorated", parsed.Returning[1].Alias);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in INSERT with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING no INSERT com parênteses desbalanceados seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningUnbalancedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING com parênteses desbalanceados seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningUnbalancedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUpdate_ReturningInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in INSERT is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseInsert_ReturningInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in DELETE is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING no DELETE seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("RETURNING expression is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in DELETE with unbalanced parentheses is rejected with actionable message.
    /// PT: Garante que expressão malformada em RETURNING no DELETE com parênteses desbalanceados seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_ReturningUnbalancedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("unbalanced parentheses", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints are rejected for Npgsql.
    /// PT: Garante que hints de tabela do SQL Server sejam rejeitados para Npgsql.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
    }

    /// <summary>
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var limitOffset = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id LIMIT 2 OFFSET 1",
            dialect));
        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));
        var fetchFirst = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id FETCH FIRST 2 ROWS ONLY",
            dialect));

        var normalizedLimit = Assert.IsType<SqlLimitOffset>(limitOffset.RowLimit);
        var normalizedOffsetFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);
        var normalizedFetchFirst = Assert.IsType<SqlLimitOffset>(fetchFirst.RowLimit);

        Assert.Equal(normalizedLimit, normalizedOffsetFetch);
        Assert.Equal(2, normalizedFetchFirst.Count);
        Assert.Null(normalizedFetchFirst.Offset);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for Npgsql.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para Npgsql.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Use hints compatíveis", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures unsupported quoted aliases are rejected with actionable parser diagnostics for this dialect.
    /// PT: Garante que aliases com quoting não suportado sejam rejeitados com diagnóstico acionável do parser para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", new NpgsqlDialect(version)));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Npgsql accepts double-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o Npgsql aceite aliases com aspas duplas e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithDoubleQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User Name\" FROM users",
            new NpgsqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures Npgsql unescapes doubled double-quotes inside quoted aliases when normalizing AST alias text.
    /// PT: Garante que o Npgsql faça unescape de aspas duplas duplicadas dentro de aliases quoted ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithEscapedDoubleQuotedAlias_ShouldNormalizeEscapedQuote(int version)
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            new NpgsqlDialect(version)));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User\"Name", item.Alias);
    }


    /// <summary>
    /// EN: Ensures PIVOT clause is rejected when the dialect capability flag is disabled.
    /// PT: Garante que a cláusula pivot seja rejeitada quando a flag de capacidade do dialeto está desabilitada.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseSelect_WithPivot_ShouldBeRejectedWithDialectMessage(int version)
    {
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

        Assert.Contains("PIVOT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies DELETE without FROM returns an actionable error message.
    /// PT: Verifica que DELETE sem FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new NpgsqlDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE target alias before FROM returns an actionable error message.
    /// PT: Verifica que alias alvo de DELETE antes de FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new NpgsqlDialect(version)));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new NpgsqlDialect(version)));

        Assert.Contains("token inicial", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("SELECT/INSERT/UPDATE/DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE is rejected for Npgsql dialect with an actionable not-supported message.
    /// PT: Garante que MERGE seja rejeitado para o dialeto Npgsql com mensagem acionável de não suportado.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseMerge_UnsupportedDialect_ShouldProvideActionableMessage(int version)
    {
        const string sql = "MERGE INTO users u USING users s ON u.id = s.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < NpgsqlDialect.MergeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new NpgsqlDialect(version)));

            Assert.Contains("MERGE", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("não suportado", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = SqlQueryParser.Parse(sql, new NpgsqlDialect(version));
        Assert.IsType<SqlMergeQuery>(parsed);
    }

    /// <summary>
    /// EN: Ensures runtime dialect hooks used by executor remain stable across supported versions.
    /// PT: Garante que os hooks de runtime do dialeto usados pelo executor permaneçam estáveis nas versões suportadas.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
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
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: Npgsql dialect version under test. PT: Versão do dialeto Npgsql em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new NpgsqlDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("npgsql", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by PostgreSQL version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do PostgreSQL e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expected = version >= NpgsqlDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("LAG"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against PostgreSQL dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto PostgreSQL por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new NpgsqlDialect(version);
        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expected = version >= NpgsqlDialect.WindowFunctionsMinVersion;
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
    [MemberDataNpgsqlVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var dialect = new NpgsqlDialect(version);

        if (version < NpgsqlDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Npgsql parser accepts ordered-set WITHIN GROUP for STRING_AGG.
    /// PT: Garante que o parser Npgsql aceite ordered-set WITHIN GROUP para STRING_AGG.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var expr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("STRING_AGG", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy![0].Desc);
    }

    /// <summary>
    /// EN: Ensures Npgsql parser blocks non-native ordered-set aggregate names with WITHIN GROUP.
    /// PT: Garante que o parser Npgsql bloqueie nomes não nativos de agregação ordered-set com WITHIN GROUP.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_ListAggWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("LISTAGG", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP clause fails with actionable ORDER BY message.
    /// PT: Garante que cláusula WITHIN GROUP malformada falhe com mensagem acionável de ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataNpgsqlVersion]
    public void ParseScalar_StringAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByTrailingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByEmptyList_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByLeadingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

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
    [MemberDataNpgsqlVersion]
    public void ParseScalar_WithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowActionableError(int version)
    {
        var dialect = new NpgsqlDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}

