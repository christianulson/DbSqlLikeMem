using System.Collections;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers the first parser slice of the automatic SQL dialect mode.
/// PT: Cobre a primeira fatia de parser do modo automatico de dialeto SQL.
/// </summary>
public sealed class SqlDialectAutoParserTests
{
    /// <summary>
    /// EN: Verifies Auto dialect exposes the pagination capabilities required for TOP, LIMIT and FETCH parsing.
    /// PT: Verifica se o dialeto Auto expoe as capabilities de paginacao necessarias para parsing de TOP, LIMIT e FETCH.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposePaginationCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsTop);
        Assert.True(dialect.SupportsLimitOffset);
        Assert.True(dialect.SupportsFetchFirst);
        Assert.True(dialect.SupportsOffsetFetch);
    }

    /// <summary>
    /// EN: Verifies TOP, LIMIT and FETCH FIRST normalize to the same canonical row-limit AST shape in Auto dialect.
    /// PT: Verifica se TOP, LIMIT e FETCH FIRST sao normalizados para o mesmo formato canonico de AST de limite de linhas no dialeto Auto.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNormalizeTopLimitAndFetchFirst_ToCanonicalRowLimit()
    {
        var dialect = new AutoSqlDialect();

        var top = ParseRowLimit("SELECT TOP 5 id FROM users", dialect);
        var limit = ParseRowLimit("SELECT id FROM users LIMIT 5", dialect);
        var fetch = ParseRowLimit("SELECT id FROM users FETCH FIRST 5 ROWS ONLY", dialect);

        Assert.Equal(5, top.Count);
        Assert.Null(top.Offset);
        Assert.Equal(top.Count, limit.Count);
        Assert.Equal(top.Offset, limit.Offset);
        Assert.Equal(top.Count, fetch.Count);
        Assert.Equal(top.Offset, fetch.Offset);
    }

    /// <summary>
    /// EN: Verifies OFFSET/FETCH keeps using the canonical LIMIT/OFFSET node in Auto dialect.
    /// PT: Verifica se OFFSET/FETCH continua usando o no canonico de LIMIT/OFFSET no dialeto Auto.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNormalizeOffsetFetch_ToCanonicalRowLimit()
    {
        var rowLimit = ParseRowLimit(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            new AutoSqlDialect());

        Assert.Equal(2, rowLimit.Count);
        Assert.Equal(1, rowLimit.Offset);
    }

    /// <summary>
    /// EN: Verifies Oracle-style ROWNUM predicates normalize to the canonical row-limit node in Auto dialect.
    /// PT: Verifica se predicados no estilo Oracle com ROWNUM sao normalizados para o no canonico de limite de linhas no dialeto Auto.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNormalizeRownumPredicate_ToCanonicalRowLimit()
    {
        var parsed = ParseSelect(
            "SELECT id FROM users WHERE ROWNUM <= 5",
            new AutoSqlDialect());

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(5, rowLimit.Count);
        Assert.Null(rowLimit.Offset);
        Assert.Null(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies ROWNUM normalization removes only the limit predicate and preserves the remaining WHERE filter.
    /// PT: Verifica se a normalizacao de ROWNUM remove apenas o predicado de limite e preserva o filtro restante do WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldKeepRemainingWherePredicate_WhenRownumUsesAnd()
    {
        var parsed = ParseSelect(
            "SELECT id FROM users WHERE status = 1 AND ROWNUM <= 5",
            new AutoSqlDialect());

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(5, rowLimit.Count);
        Assert.Null(rowLimit.Offset);
        Assert.IsType<BinaryExpr>(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies Auto normalization tightens existing pagination when TOP and strict ROWNUM bounds are combined.
    /// PT: Verifica se a normalizacao Auto restringe a paginacao existente quando TOP e limites estritos com ROWNUM sao combinados.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldTightenExistingLimit_WhenRownumAndTopAreCombined()
    {
        var parsed = ParseSelect(
            "SELECT TOP 10 id FROM users WHERE ROWNUM < 4",
            new AutoSqlDialect());

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(3, rowLimit.Count);
        Assert.Null(rowLimit.Offset);
        Assert.Null(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies Auto dialect keeps unsafe ROWNUM predicates in WHERE when they are combined with OR.
    /// PT: Verifica se o dialeto Auto mantem predicados inseguros com ROWNUM no WHERE quando eles sao combinados com OR.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNotRewriteUnsafeRownumPredicate_WhenCombinedWithOr()
    {
        var parsed = ParseSelect(
            "SELECT id FROM users WHERE ROWNUM <= 5 OR status = 1",
            new AutoSqlDialect());

        Assert.Null(parsed.RowLimit);
        Assert.IsType<BinaryExpr>(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies Auto dialect does not rewrite ROWNUM when the query already uses offset-based pagination.
    /// PT: Verifica se o dialeto Auto nao reescreve ROWNUM quando a consulta ja usa paginacao baseada em offset.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNotRewriteRownum_WhenOffsetPaginationAlreadyExists()
    {
        var parsed = ParseSelect(
            "SELECT id FROM users WHERE ROWNUM <= 5 ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            new AutoSqlDialect());

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(2, rowLimit.Count);
        Assert.Equal(1, rowLimit.Offset);
        Assert.IsType<BinaryExpr>(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies Auto dialect resolves integer parameters before normalizing a safe ROWNUM predicate.
    /// PT: Verifica se o dialeto Auto resolve parametros inteiros antes de normalizar um predicado seguro com ROWNUM.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldNormalizeRownumPredicate_WithParameter()
    {
        var parameters = new TestParameterCollection
        {
            new TestParameter("@take", 4)
        };

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users WHERE ROWNUM <= @take",
            new AutoSqlDialect(),
            parameters));

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(4, rowLimit.Count);
        Assert.Null(rowLimit.Offset);
        Assert.Null(parsed.Where);
    }

    /// <summary>
    /// EN: Verifies the parser exposes dedicated Auto helpers for single statements, batches and UNION chains.
    /// PT: Verifica se o parser expoe helpers dedicados de Auto para statements unicos, lotes e cadeias UNION.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeDedicatedParseHelpers()
    {
        var single = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto("SELECT TOP 2 id FROM users"));
        var batch = SqlQueryParser.ParseMultiAuto("SELECT TOP 1 id FROM users; SELECT id FROM users LIMIT 2").ToList();
        var union = SqlQueryParser.ParseUnionChainAuto("SELECT TOP 1 id FROM users UNION ALL SELECT id FROM users LIMIT 2");
        var statements = SqlQueryParser.SplitStatementsAuto("SELECT `id` FROM users; SELECT [name] FROM users").ToList();

        Assert.IsType<SqlLimitOffset>(single.RowLimit);
        Assert.Equal(2, batch.Count);
        Assert.Equal(2, union.Parts.Count);
        Assert.Equal(2, statements.Count);
    }

    /// <summary>
    /// EN: Verifies the expression parser exposes dedicated Auto helpers for scalar and WHERE expressions.
    /// PT: Verifica se o parser de expressoes expoe helpers dedicados de Auto para expressoes escalares e WHERE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeDedicatedExpressionParseHelpers()
    {
        var parameters = new TestParameterCollection
        {
            new TestParameter("@take", 4)
        };

        var scalar = SqlExpressionParser.ParseScalarAuto("`users`.`id`");
        var where = SqlExpressionParser.ParseWhereAuto("ROWNUM <= @take AND [status] = 1", parameters);

        Assert.IsType<ColumnExpr>(scalar);
        var binary = Assert.IsType<BinaryExpr>(where);
        Assert.Equal(SqlBinaryOp.And, binary.Op);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection also recognizes high-value identity and concatenation markers in one token scan.
    /// PT: Verifica se a deteccao de sintaxe Auto tambem reconhece marcadores de identidade e concatenacao de alto retorno em uma unica varredura de tokens.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectIdentityAndConcatenationMarkers()
    {
        var features = DetectSyntaxFeatures(
            """
            CREATE TABLE users (
                id INT IDENTITY(1,1),
                legacy_id BIGSERIAL,
                external_id INT AUTO_INCREMENT,
                full_name VARCHAR(100)
            );
            SELECT CONCAT(first_name, ' ', last_name), first_name || ' ' || last_name FROM users;
            """);

        Assert.True((features & AutoSqlSyntaxFeatures.Identity) != 0);
        Assert.True((features & AutoSqlSyntaxFeatures.Concat) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection ignores markers that appear only inside string literals.
    /// PT: Verifica se a deteccao de sintaxe Auto ignora marcadores que aparecem apenas dentro de literais string.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldIgnoreMarkersInsideStringLiterals()
    {
        var features = DetectSyntaxFeatures(
            "SELECT 'TOP LIMIT FETCH OFFSET ROWNUM IDENTITY AUTO_INCREMENT SERIAL CONCAT ||' AS sample");

        Assert.Equal(AutoSqlSyntaxFeatures.None, features);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection ignores quoted identifiers that happen to match dialect marker names.
    /// PT: Verifica se a deteccao de sintaxe Auto ignora identificadores quoted que por acaso coincidem com nomes de marcadores de dialeto.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldIgnoreQuotedIdentifiersWithMarkerNames()
    {
        var features = DetectSyntaxFeatures(
            "SELECT `serial`, [identity], \"concat\" FROM users");

        Assert.Equal(AutoSqlSyntaxFeatures.None, features);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared sequence capabilities already supported by the parser and runtime contracts.
    /// PT: Verifica se o dialeto Auto expoe as capabilities compartilhadas de sequence ja suportadas pelos contratos de parser e runtime.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeSequenceCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsSequenceDdl);
        Assert.True(dialect.SupportsNextValueForSequenceExpression);
        Assert.True(dialect.SupportsPreviousValueForSequenceExpression);
        Assert.True(dialect.SupportsSequenceDotValueExpression("NEXTVAL"));
        Assert.True(dialect.SupportsSequenceDotValueExpression("CURRVAL"));
        Assert.True(dialect.SupportsSequenceFunctionCall("NEXTVAL"));
        Assert.True(dialect.SupportsSequenceFunctionCall("CURRVAL"));
        Assert.True(dialect.SupportsSequenceFunctionCall("SETVAL"));
        Assert.True(dialect.SupportsSequenceFunctionCall("LASTVAL"));
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared sequence DDL without requiring a provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta DDL compartilhado de sequence sem exigir selecao de dialeto especifico por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSequenceDdl()
    {
        var create = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.ParseAuto(
            "CREATE SEQUENCE sales.seq_orders START WITH 10 INCREMENT BY 5"));
        var drop = Assert.IsType<SqlDropSequenceQuery>(SqlQueryParser.ParseAuto(
            "DROP SEQUENCE IF EXISTS sales.seq_orders"));

        Assert.Equal("seq_orders", create.Table?.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("sales", create.Table?.DbName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(10, create.StartValue);
        Assert.Equal(5, create.IncrementBy);
        Assert.True(drop.IfExists);
        Assert.Equal("seq_orders", drop.Table?.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses the pragmatic shared subset of CREATE/DROP INDEX DDL.
    /// PT: Verifica se o modo Auto interpreta o subset pragmático compartilhado de DDL CREATE/DROP INDEX.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseIndexDdl()
    {
        var create = Assert.IsType<SqlCreateIndexQuery>(SqlQueryParser.ParseAuto(
            "CREATE UNIQUE INDEX ix_users_name ON sales.users (name, email)"));
        var drop = Assert.IsType<SqlDropIndexQuery>(SqlQueryParser.ParseAuto(
            "DROP INDEX IF EXISTS ix_users_name"));

        Assert.True(create.Unique);
        Assert.Equal("ix_users_name", create.IndexName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("sales", create.Table?.DbName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("users", create.Table?.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(new[] { "name", "email" }, create.KeyColumns);
        Assert.True(drop.IfExists);
        Assert.Equal("ix_users_name", drop.IndexName, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses the shared sequence expression families into the existing canonical call nodes.
    /// PT: Verifica se o modo Auto interpreta as familias compartilhadas de expressoes de sequence para os nos canonicos de chamada ja existentes.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseEquivalentSequenceExpressionFamilies()
    {
        var nextValueFor = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("NEXT VALUE FOR sales.seq_orders"));
        var previousValueFor = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("PREVIOUS VALUE FOR sales.seq_orders"));
        var dotNextVal = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("sales.seq_orders.NEXTVAL"));
        var dotCurrVal = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("sales.seq_orders.CURRVAL"));
        var nextVal = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("NEXTVAL('sales.seq_orders')"));
        var currVal = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("CURRVAL('sales.seq_orders')"));
        var lastVal = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("LASTVAL()"));

        Assert.Equal("NEXT_VALUE_FOR", nextValueFor.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("PREVIOUS_VALUE_FOR", previousValueFor.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("NEXTVAL", dotNextVal.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CURRVAL", dotCurrVal.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("NEXTVAL", nextVal.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CURRVAL", currVal.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LASTVAL", lastVal.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared JSON arrow capability used by the parser and executor.
    /// PT: Verifica se o dialeto Auto expoe a capability compartilhada de operadores JSON usada pelo parser e pelo executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeJsonArrowCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsJsonArrowOperators);
        Assert.True(dialect.AllowsParserCrossDialectJsonOperators);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared JSON arrow operators into the neutral JSON access AST node.
    /// PT: Verifica se o modo Auto interpreta operadores JSON compartilhados para o no neutro de AST de acesso JSON.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseJsonArrowOperators()
    {
        var extract = Assert.IsType<JsonAccessExpr>(SqlExpressionParser.ParseScalarAuto("payload->'$.tenant'"));
        var unquote = Assert.IsType<JsonAccessExpr>(SqlExpressionParser.ParseScalarAuto("payload->>'$.tenant'"));

        Assert.False(extract.Unquote);
        Assert.True(unquote.Unquote);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared JSON function capabilities already implemented by the parser and executor.
    /// PT: Verifica se o dialeto Auto expoe as capabilities compartilhadas de funcoes JSON ja implementadas pelo parser e pelo executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeJsonFunctionCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsJsonExtractFunction);
        Assert.True(dialect.SupportsJsonValueFunction);
        Assert.True(dialect.SupportsJsonValueReturningClause);
        Assert.True(dialect.SupportsOpenJsonFunction);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared JSON_EXTRACT and JSON_VALUE calls without provider-specific selection.
    /// PT: Verifica se o modo Auto interpreta chamadas compartilhadas de JSON_EXTRACT e JSON_VALUE sem selecao especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedJsonFunctions()
    {
        var extract = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("JSON_EXTRACT(payload, '$.tenant')"));
        var value = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("JSON_VALUE(payload, '$.tenant')"));
        var valueReturning = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("JSON_VALUE(payload, '$.tenant' RETURNING NUMBER)"));
        var openJson = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("OPENJSON(payload)"));

        Assert.Equal("JSON_EXTRACT", extract.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("JSON_VALUE", value.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("JSON_VALUE", valueReturning.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OPENJSON", openJson.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("RETURNING NUMBER", Assert.IsType<RawSqlExpr>(valueReturning.Args[2]).Sql, ignoreCase: true);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared temporal aliases already supported by the evaluator.
    /// PT: Verifica se o dialeto Auto expoe os aliases temporais compartilhados ja suportados pelo evaluator.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeTemporalAliases()
    {
        var dialect = new AutoSqlDialect();

        Assert.Contains("CURRENT_DATE", dialect.TemporalFunctionIdentifierNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("CURRENT_TIME", dialect.TemporalFunctionIdentifierNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("CURRENT_TIMESTAMP", dialect.TemporalFunctionIdentifierNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("SYSTEMDATE", dialect.TemporalFunctionIdentifierNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("SYSDATE", dialect.TemporalFunctionIdentifierNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("NOW", dialect.TemporalFunctionCallNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("GETDATE", dialect.TemporalFunctionCallNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("SYSDATETIME", dialect.TemporalFunctionCallNames, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared temporal identifiers and calls without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta identificadores e chamadas temporais compartilhadas sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedTemporalAliases()
    {
        var currentDate = SqlExpressionParser.ParseScalarAuto("CURRENT_DATE");
        var systemDate = SqlExpressionParser.ParseScalarAuto("SYSTEMDATE");
        var sysDate = SqlExpressionParser.ParseScalarAuto("SYSDATE");
        var now = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("NOW()"));
        var getDate = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("GETDATE()"));

        Assert.IsType<IdentifierExpr>(currentDate);
        Assert.IsType<IdentifierExpr>(systemDate);
        Assert.IsType<IdentifierExpr>(sysDate);
        Assert.Equal("NOW", now.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("GETDATE", getDate.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared date-add function families without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta familias compartilhadas de funcoes de adicao temporal sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedDateAddFamilies()
    {
        var dateAdd = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("DATE_ADD(created_at, INTERVAL 1 DAY)"));
        var sqlServerDateAdd = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("DATEADD(DAY, 1, created_at)"));
        var timestampAdd = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("TIMESTAMPADD(DAY, 1, created_at)"));

        Assert.Equal("DATE_ADD", dateAdd.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATEADD", sqlServerDateAdd.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TIMESTAMPADD", timestampAdd.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared string-aggregate capabilities already supported by parser and executor.
    /// PT: Verifica se o dialeto Auto expoe as capabilities compartilhadas de agregacao textual ja suportadas por parser e executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeStringAggregateCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsStringAggregateFunction("GROUP_CONCAT"));
        Assert.True(dialect.SupportsStringAggregateFunction("STRING_AGG"));
        Assert.True(dialect.SupportsStringAggregateFunction("LISTAGG"));
        Assert.True(dialect.SupportsWithinGroupStringAggregateFunction("GROUP_CONCAT"));
        Assert.True(dialect.SupportsWithinGroupStringAggregateFunction("STRING_AGG"));
        Assert.True(dialect.SupportsWithinGroupStringAggregateFunction("LISTAGG"));
        Assert.True(dialect.SupportsAggregateOrderByStringAggregateFunction("GROUP_CONCAT"));
        Assert.True(dialect.SupportsAggregateOrderByStringAggregateFunction("STRING_AGG"));
        Assert.True(dialect.SupportsAggregateOrderByStringAggregateFunction("LISTAGG"));
        Assert.True(dialect.SupportsAggregateSeparatorKeywordStringAggregateFunction("GROUP_CONCAT"));
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared rowcount helper capabilities already supported by parser and executor.
    /// PT: Verifica se o dialeto Auto expoe as capabilities compartilhadas de helpers de rowcount ja suportadas por parser e executor.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeRowCountCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"));
        Assert.True(dialect.SupportsLastFoundRowsFunction("ROW_COUNT"));
        Assert.True(dialect.SupportsLastFoundRowsFunction("CHANGES"));
        Assert.True(dialect.SupportsLastFoundRowsFunction("ROWCOUNT"));
        Assert.True(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
        Assert.True(dialect.SupportsDoubleAtIdentifierSyntax);
        Assert.True(dialect.SupportsSqlCalcFoundRowsModifier);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared null-safe equality capability.
    /// PT: Verifica se o dialeto Auto expoe a capability compartilhada de igualdade null-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeNullSafeEqualityCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsNullSafeEq);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared ILIKE capability.
    /// PT: Verifica se o dialeto Auto expoe a capability compartilhada de ILIKE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeIlikeCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsIlikeOperator);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared MATCH ... AGAINST capability.
    /// PT: Verifica se o dialeto Auto expoe a capability compartilhada de MATCH ... AGAINST.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeMatchAgainstCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsMatchAgainstPredicate);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared conditional and null-substitute capabilities.
    /// PT: Verifica se o dialeto Auto expoe as capabilities compartilhadas de condicionais e substituicao de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeConditionalAndNullSubstituteCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsIfFunction);
        Assert.True(dialect.SupportsIifFunction);
        Assert.Contains("IFNULL", dialect.NullSubstituteFunctionNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("ISNULL", dialect.NullSubstituteFunctionNames, StringComparer.OrdinalIgnoreCase);
        Assert.Contains("NVL", dialect.NullSubstituteFunctionNames, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared window-function capability surface.
    /// PT: Verifica se o dialeto Auto expoe a superficie compartilhada de capabilities de funcoes de janela.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeWindowFunctionCapabilities()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsWindowFunctions);
        Assert.True(dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.True(dialect.SupportsWindowFunction("LAG"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes the shared PIVOT capability already available in the shared parser and executor.
    /// PT: Verifica se o dialeto Auto expoe a capability compartilhada de PIVOT ja disponivel no parser e no executor compartilhados.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposePivotCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsPivotClause);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes shared WITH/CTE support.
    /// PT: Verifica se o dialeto Auto expoe suporte compartilhado a WITH/CTE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeWithCteCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsWithCte);
        Assert.True(dialect.SupportsWithRecursive);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes shared DML RETURNING support.
    /// PT: Verifica se o dialeto Auto expoe suporte compartilhado a RETURNING em DML.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeReturningCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsReturning);
    }

    /// <summary>
    /// EN: Verifies Auto dialect exposes shared ORDER BY NULLS FIRST/LAST support.
    /// PT: Verifica se o dialeto Auto expoe suporte compartilhado a ORDER BY NULLS FIRST/LAST.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldExposeOrderByNullsCapability()
    {
        var dialect = new AutoSqlDialect();

        Assert.True(dialect.SupportsOrderByNullsModifier);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared string-aggregate families, including ordered-set syntax.
    /// PT: Verifica se o modo Auto interpreta familias compartilhadas de agregacao textual, incluindo sintaxe ordered-set.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedStringAggregateFamilies()
    {
        var groupConcat = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("GROUP_CONCAT(amount, '|') WITHIN GROUP (ORDER BY amount DESC)"));
        var stringAgg = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)"));
        var listAgg = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)"));

        Assert.Equal("GROUP_CONCAT", groupConcat.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("STRING_AGG", stringAgg.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LISTAGG", listAgg.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Single(groupConcat.WithinGroupOrderBy!);
        Assert.Single(stringAgg.WithinGroupOrderBy!);
        Assert.Single(listAgg.WithinGroupOrderBy!);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared rowcount helpers and the SQL Server-style identifier alias.
    /// PT: Verifica se o modo Auto interpreta helpers compartilhados de rowcount e o alias identificador no estilo SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedRowCountHelpers()
    {
        var foundRows = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("FOUND_ROWS()"));
        var rowCount = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("ROW_COUNT()"));
        var changes = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("CHANGES()"));
        var rowcount = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("ROWCOUNT()"));
        var atAtRowcount = Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalarAuto("@@ROWCOUNT"));

        Assert.Equal("FOUND_ROWS", foundRows.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ROW_COUNT", rowCount.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CHANGES", changes.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ROWCOUNT", rowcount.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("@@ROWCOUNT", atAtRowcount.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared null-safe equality into the canonical comparison node.
    /// PT: Verifica se o modo Auto interpreta a igualdade null-safe compartilhada para o no canonico de comparacao.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseNullSafeEquality()
    {
        var where = Assert.IsType<BinaryExpr>(SqlExpressionParser.ParseWhereAuto("a <=> b"));

        Assert.Equal(SqlBinaryOp.NullSafeEq, where.Op);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses ILIKE into a case-insensitive LIKE expression.
    /// PT: Verifica se o modo Auto interpreta ILIKE para uma expressão LIKE case-insensitive.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseIlike()
    {
        var where = Assert.IsType<LikeExpr>(SqlExpressionParser.ParseWhereAuto("name ILIKE 'jo%'"));

        Assert.True(where.CaseInsensitive);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses MATCH ... AGAINST into the shared internal call form.
    /// PT: Verifica se o modo Auto interpreta MATCH ... AGAINST para a forma interna compartilhada de chamada.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseMatchAgainst()
    {
        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto(
            "MATCH(title, body) AGAINST ('+john -maria' IN BOOLEAN MODE)"));

        Assert.Equal("MATCH_AGAINST", expr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(3, expr.Args.Count);
        Assert.Equal("IN BOOLEAN MODE", Assert.IsType<RawSqlExpr>(expr.Args[2]).Sql, ignoreCase: true);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared conditional and null-substitute helpers.
    /// PT: Verifica se o modo Auto interpreta helpers compartilhados condicionais e de substituicao de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseConditionalAndNullSubstituteHelpers()
    {
        var ifExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("IF(score > 0, 'yes', 'no')"));
        var iifExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("IIF(score > 0, 'yes', 'no')"));
        var ifNullExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("IFNULL(name, 'n/a')"));
        var isNullExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("ISNULL(name, 'n/a')"));
        var nvlExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("NVL(name, 'n/a')"));
        var coalesceExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("COALESCE(name, 'n/a')"));
        var nullIfExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalarAuto("NULLIF(name, 'n/a')"));

        Assert.Equal("IF", ifExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("IIF", iifExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("IFNULL", ifNullExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ISNULL", isNullExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("NVL", nvlExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("COALESCE", coalesceExpr.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("NULLIF", nullIfExpr.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared window functions without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta funcoes de janela compartilhadas sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSharedWindowFunctions()
    {
        var rowNumber = Assert.IsType<WindowFunctionExpr>(SqlExpressionParser.ParseScalarAuto(
            "ROW_NUMBER() OVER (ORDER BY id)"));
        var lag = Assert.IsType<WindowFunctionExpr>(SqlExpressionParser.ParseScalarAuto(
            "LAG(amount, 1, 0) OVER (PARTITION BY userid ORDER BY amount)"));

        Assert.Equal("ROW_NUMBER", rowNumber.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Empty(rowNumber.Args);
        Assert.Single(rowNumber.Spec.OrderBy);
        Assert.Equal("LAG", lag.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(3, lag.Args.Count);
        Assert.Single(lag.Spec.PartitionBy);
        Assert.Single(lag.Spec.OrderBy);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared PIVOT syntax without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de PIVOT sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParsePivot()
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT t10, t20 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10, 20 AS t20)) p"));

        var source = parsed.Table;
        Assert.NotNull(source);
        var pivot = source!.Pivot;
        Assert.NotNull(pivot);

        Assert.Equal("COUNT", pivot!.AggregateFunction, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("tenantid", pivot.ForColumnRaw, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, pivot.InItems.Count);
        Assert.Equal("t10", pivot.InItems[0].Alias, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("t20", pivot.InItems[1].Alias, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared UNPIVOT syntax without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de UNPIVOT sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseUnpivot()
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT up.id, up.FieldName, up.FieldValue FROM (SELECT id, name, email FROM users) src UNPIVOT (FieldValue FOR FieldName IN (name, email)) up"));

        var source = parsed.Table;
        Assert.NotNull(source);
        var unpivot = source!.Unpivot;
        Assert.NotNull(unpivot);

        Assert.Equal("FieldValue", unpivot!.ValueColumnName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("FieldName", unpivot.NameColumnName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, unpivot.InItems.Count);
        Assert.Equal("name", unpivot.InItems[0].SourceColumnName, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("email", unpivot.InItems[1].SourceColumnName, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared FOR JSON PATH syntax without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de FOR JSON PATH sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseForJsonPath()
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT id AS [User.Id], name AS [User.Name] FROM users ORDER BY id FOR JSON PATH, ROOT('users')"));

        var forJson = parsed.ForJson;
        Assert.NotNull(forJson);
        Assert.Equal(SqlForJsonMode.Path, forJson!.Mode);
        Assert.Equal("users", forJson.RootName, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared WITH/CTE syntax without provider-specific dialect selection.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de WITH/CTE sem selecao de dialeto especifica por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseWithCte()
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            """
            WITH active_users AS (
                SELECT Id, Name
                FROM users
                WHERE Id <= 2
            )
            SELECT Name
            FROM active_users
            ORDER BY Id
            """));

        var cte = Assert.Single(parsed.Ctes);
        Assert.Equal("active_users", cte.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("active_users", parsed.Table?.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared RETURNING syntax for DML statements.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de RETURNING para comandos DML.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseReturning()
    {
        var insert = Assert.IsType<SqlInsertQuery>(SqlQueryParser.ParseAuto(
            "INSERT INTO users (id, name) VALUES (1, 'Ana') RETURNING id, name AS user_name"));
        var update = Assert.IsType<SqlUpdateQuery>(SqlQueryParser.ParseAuto(
            "UPDATE users SET name = 'Bia' WHERE id = 1 RETURNING id, name"));
        var delete = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.ParseAuto(
            "DELETE FROM users WHERE id = 1 RETURNING id"));

        Assert.Equal(2, insert.Returning.Count);
        Assert.Equal("user_name", insert.Returning[1].Alias, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(2, update.Returning.Count);
        Assert.Single(delete.Returning);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses shared ORDER BY NULLS FIRST/LAST syntax.
    /// PT: Verifica se o modo Auto interpreta sintaxe compartilhada de ORDER BY NULLS FIRST/LAST.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseOrderByNulls()
    {
        var first = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT Name FROM users ORDER BY Email NULLS FIRST"));
        var last = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT Name FROM users ORDER BY Email DESC NULLS LAST"));

        Assert.Single(first.OrderBy);
        Assert.True(first.OrderBy[0].NullsFirst);
        Assert.Single(last.OrderBy);
        Assert.False(last.OrderBy[0].NullsFirst);
        Assert.True(last.OrderBy[0].Desc);
    }

    /// <summary>
    /// EN: Verifies Auto mode parses the shared SQL_CALC_FOUND_ROWS select modifier.
    /// PT: Verifica se o modo Auto interpreta o modificador compartilhado SQL_CALC_FOUND_ROWS em SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldParseSqlCalcFoundRowsModifier()
    {
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.ParseAuto(
            "SELECT SQL_CALC_FOUND_ROWS Name FROM users ORDER BY Id LIMIT 1"));

        Assert.Contains("SQL_CALC_FOUND_ROWS", parsed.RawSql ?? string.Empty, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared sequence markers without requiring provider-specific parsing.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de sequence sem exigir parsing especifico por provider.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectSequenceMarkers()
    {
        var features = DetectSyntaxFeatures(
            """
            CREATE SEQUENCE sales.seq_orders START WITH 10 INCREMENT BY 5;
            SELECT NEXT VALUE FOR sales.seq_orders, PREVIOUS VALUE FOR sales.seq_orders, sales.seq_orders.NEXTVAL, CURRVAL('sales.seq_orders'), LASTVAL();
            DROP SEQUENCE IF EXISTS sales.seq_orders;
            """);

        Assert.True((features & AutoSqlSyntaxFeatures.Sequence) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared JSON arrow markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de operadores JSON.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectJsonArrowMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT payload->'$.tenant', payload->>'$.region' FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.JsonArrow) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared JSON function markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de funcoes JSON.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectJsonFunctionMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT JSON_EXTRACT(payload, '$.tenant'), JSON_VALUE(payload, '$.region'), OPENJSON(payload) FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.JsonFunction) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared temporal identifier and call markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de identificadores e chamadas temporais.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectTemporalMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT CURRENT_DATE, SYSTEMDATE, SYSDATE, NOW(), GETDATE(), SYSDATETIME()");

        Assert.True((features & AutoSqlSyntaxFeatures.Temporal) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared date-add markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de adicao temporal.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectDateAddMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT DATE_ADD(created_at, INTERVAL 1 DAY), DATEADD(DAY, 1, created_at), TIMESTAMPADD(DAY, 1, created_at)");

        Assert.True((features & AutoSqlSyntaxFeatures.DateAdd) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared string-aggregate markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de agregacao textual.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectStringAggregateMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT GROUP_CONCAT(name, '|'), STRING_AGG(name, '|'), LISTAGG(name, '|') WITHIN GROUP (ORDER BY name) FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.StringAggregate) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared rowcount helper markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de helpers de rowcount.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectRowCountMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT SQL_CALC_FOUND_ROWS Name FROM users LIMIT 1; SELECT FOUND_ROWS(), ROW_COUNT(), CHANGES(), ROWCOUNT(), @@ROWCOUNT");

        Assert.True((features & AutoSqlSyntaxFeatures.RowCount) != 0);
        Assert.True((features & AutoSqlSyntaxFeatures.SqlCalcFoundRows) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes the shared null-safe equality marker.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece o marcador compartilhado de igualdade null-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectNullSafeEqualityMarker()
    {
        var features = DetectSyntaxFeatures("SELECT * FROM users WHERE a <=> b");

        Assert.True((features & AutoSqlSyntaxFeatures.NullSafeEq) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes the shared ILIKE marker.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece o marcador compartilhado de ILIKE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectIlikeMarker()
    {
        var features = DetectSyntaxFeatures("SELECT * FROM users WHERE name ILIKE 'jo%'");

        Assert.True((features & AutoSqlSyntaxFeatures.Ilike) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes MATCH ... AGAINST markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores de MATCH ... AGAINST.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectMatchAgainstMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT MATCH(title, body) AGAINST ('john' IN BOOLEAN MODE) FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.MatchAgainst) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared conditional and null-substitute markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados condicionais e de substituicao de nulos.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectConditionalAndNullSubstituteMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT IF(a > 0, 1, 0), IIF(a > 0, 1, 0), IFNULL(name, 'n/a'), ISNULL(name, 'n/a'), NVL(name, 'n/a'), COALESCE(name, 'n/a'), NULLIF(name, 'n/a') FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.ConditionalNullFunctions) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared window-function markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de funcoes de janela.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectWindowFunctionMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT ROW_NUMBER() OVER (ORDER BY id), LAG(amount, 1, 0) OVER (PARTITION BY userid ORDER BY amount) FROM users");

        Assert.True((features & AutoSqlSyntaxFeatures.WindowFunctions) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared PIVOT markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de PIVOT.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectPivotMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT t10, t20 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10, 20 AS t20)) p");

        Assert.True((features & AutoSqlSyntaxFeatures.Pivot) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared UNPIVOT markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de UNPIVOT.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectUnpivotMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT up.id, up.FieldName, up.FieldValue FROM (SELECT id, name, email FROM users) src UNPIVOT (FieldValue FOR FieldName IN (name, email)) up");

        Assert.True((features & AutoSqlSyntaxFeatures.Pivot) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared WITH/CTE markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de WITH/CTE.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectWithCteMarkers()
    {
        var features = DetectSyntaxFeatures("WITH active_users AS (SELECT Id FROM users) SELECT Id FROM active_users");

        Assert.True((features & AutoSqlSyntaxFeatures.WithCte) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared RETURNING markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de RETURNING.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectReturningMarkers()
    {
        var features = DetectSyntaxFeatures("INSERT INTO users (id, name) VALUES (1, 'Ana') RETURNING id, name");

        Assert.True((features & AutoSqlSyntaxFeatures.Returning) != 0);
    }

    /// <summary>
    /// EN: Verifies Auto syntax detection recognizes shared ORDER BY NULLS FIRST/LAST markers.
    /// PT: Verifica se a deteccao de sintaxe Auto reconhece marcadores compartilhados de ORDER BY NULLS FIRST/LAST.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void AutoDialect_ShouldDetectOrderByNullsMarkers()
    {
        var features = DetectSyntaxFeatures("SELECT Name FROM users ORDER BY Email NULLS FIRST, Name DESC NULLS LAST");

        Assert.True((features & AutoSqlSyntaxFeatures.OrderByNulls) != 0);
    }

    private static SqlLimitOffset ParseRowLimit(string sql, AutoSqlDialect dialect)
    {
        var parsed = ParseSelect(sql, dialect);
        return Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
    }

    private static AutoSqlSyntaxFeatures DetectSyntaxFeatures(string sql)
    {
        var tokens = new SqlTokenizer(sql, new AutoSqlDialect()).Tokenize();
        return SqlSyntaxDetector.Detect(sql, tokens);
    }

    private static SqlSelectQuery ParseSelect(string sql, AutoSqlDialect dialect)
        => Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));

    private sealed class TestParameterCollection : IDataParameterCollection
    {
        private readonly List<object> _items = [];

        public object this[string parameterName]
        {
            get => _items
                .OfType<IDataParameter>()
                .First(parameter => string.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));
            set => throw new NotSupportedException();
        }

        public object? this[int index]
        {
            get => _items[index];
            set
            {
                ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
                _items[index] = value!;
            }
        }

        public bool IsFixedSize => false;

        public bool IsReadOnly => false;

        public int Count => _items.Count;

        public bool IsSynchronized => false;

        public object SyncRoot => this;

        public int Add(object? value)
        {
            ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
            _items.Add(value!);
            return _items.Count - 1;
        }

        public void Clear() => _items.Clear();

        public bool Contains(string parameterName)
            => _items
                .OfType<IDataParameter>()
                .Any(parameter => string.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase));

        public bool Contains(object? value)
            => value is not null && _items.Contains(value);

        public void CopyTo(Array array, int index)
            => _items.ToArray().CopyTo(array, index);

        public IEnumerator GetEnumerator() => _items.GetEnumerator();

        public int IndexOf(string parameterName)
        {
            for (var i = 0; i < _items.Count; i++)
            {
                if (_items[i] is IDataParameter parameter
                    && string.Equals(parameter.ParameterName, parameterName, StringComparison.OrdinalIgnoreCase))
                    return i;
            }

            return -1;
        }

        public int IndexOf(object? value)
            => value is null ? -1 : _items.IndexOf(value);

        public void Insert(int index, object? value)
        {
            ArgumentNullExceptionCompatible.ThrowIfNull(value, nameof(value));
            _items.Insert(index, value!);
        }

        public void Remove(object? value)
        {
            if (value is not null)
                _items.Remove(value);
        }

        public void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                _items.RemoveAt(index);
        }

        public void RemoveAt(int index) => _items.RemoveAt(index);
    }

#nullable disable
    private sealed class TestParameter(string name, object value) : IDataParameter
    {
        private string _parameterName = name;
        private string _sourceColumn = string.Empty;

        public DbType DbType { get; set; }

        public ParameterDirection Direction { get; set; } = ParameterDirection.Input;

        public bool IsNullable => true;

        public DataRowVersion SourceVersion { get; set; } = DataRowVersion.Current;

        public object Value { get; set; } = value;

        string IDataParameter.ParameterName
        {
            get => _parameterName;
            set => _parameterName = value ?? string.Empty;
        }

        string IDataParameter.SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value ?? string.Empty;
        }
    }
#nullable restore
}
