namespace DbSqlLikeMem.Oracle.Test.Parser;

/// <summary>
/// EN: Covers Oracle-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do Oracle.
/// </summary>
public sealed class OracleDialectFeatureParserTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures Oracle preserves binary column size metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o Oracle preserve o metadado de tamanho de coluna binaria no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseAlterTableAddBinaryColumn_ShouldPreserveSize(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD payload VARBINARY(16) NULL",
            db, d));

        Assert.Equal(DbType.Binary, parsed.ColumnType);
        Assert.Equal(16, parsed.Size);
        Assert.True(parsed.Nullable);
    }

    /// <summary>
    /// EN: Ensures Oracle preserves DECIMAL precision and scale metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o Oracle preserve os metadados de precisao e escala de DECIMAL no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseAlterTableAddDecimalColumn_ShouldPreservePrecisionAndScale(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD amount DECIMAL(10, 4) NOT NULL DEFAULT 0",
            db, d));

        Assert.Equal(DbType.Decimal, parsed.ColumnType);
        Assert.Equal(10, parsed.Size);
        Assert.Equal(4, parsed.DecimalPlaces);
        Assert.False(parsed.Nullable);
        Assert.Equal("0", parsed.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects ALTER TABLE ... ADD when NOT NULL is paired with DEFAULT NULL outside the pragmatic subset.
    /// PT: Garante que o Oracle rejeite ALTER TABLE ... ADD quando NOT NULL e combinado com DEFAULT NULL fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseAlterTableAddColumn_NotNullWithDefaultNull_ShouldReject(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users ADD status VARCHAR(20) NOT NULL DEFAULT NULL",
            db, d));

        Assert.Contains("default null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects ALTER TABLE ... ADD when the table reference uses an alias outside the pragmatic subset.
    /// PT: Garante que o Oracle rejeite ALTER TABLE ... ADD quando a referencia da tabela usa alias fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseAlterTableAddColumn_WithTableAlias_ShouldReject(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users u ADD age INT",
            db, d));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects ALTER TABLE ... ADD when the table reference is a derived source outside the pragmatic subset.
    /// PT: Garante que o Oracle rejeite ALTER TABLE ... ADD quando a referencia da tabela e uma fonte derivada fora do subset pragmatico.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseAlterTableAddColumn_WithDerivedTable_ShouldReject(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE (SELECT * FROM users) u ADD age INT",
            db, d));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle parses the pragmatic provider-real scalar FUNCTION DDL subset.
    /// PT: Garante que o Oracle interprete o subset pragmatico e realista do provider para DDL de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalarFunctionDdlSubset_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue; END",
            db, d));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal("NUMBER", create.Definition.ReturnTypeSql, ignoreCase: true);
        Assert.Equal(2, create.Definition.Parameters.Count);
        Assert.Equal("baseValue", create.Definition.Parameters[0].Name, ignoreCase: true);
        Assert.Equal("incrementValue", create.Definition.Parameters[1].Name, ignoreCase: true);
        Assert.IsType<BinaryExpr>(create.Definition.Body);

        var drop = Assert.IsType<SqlDropFunctionQuery>(SqlQueryParser.Parse(
            "DROP FUNCTION fn_users",
            db, d));

        Assert.False(drop.IfExists);
        Assert.Equal("fn_users", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Oracle parses CREATE OR REPLACE FUNCTION in the supported provider-real subset.
    /// PT: Garante que o Oracle interprete CREATE OR REPLACE FUNCTION no subset realista suportado pelo provider.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Vers+�o do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseCreateOrReplaceScalarFunctionDdlSubset_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE OR REPLACE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue + 1; END",
            db, d));
        Assert.True(create.OrReplace);
        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(2, create.Definition.Parameters.Count);
        Assert.IsType<BinaryExpr>(create.Definition.Body);
    }

    /// <summary>
    /// EN: Ensures Oracle exposes ROW_COUNT() through the dialect capability used by the executor.
    /// PT: Garante que o Oracle exponha ROW_COUNT() pela capability de dialeto usada pelo executor.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void LastFoundRowsCapability_ShouldExposeOracleFunction(int version)
    {
        var dialect = Get(version, v => new OracleDialect(v));

        Assert.True(dialect.SupportsLastFoundRowsFunction("ROW_COUNT"));
        Assert.False(dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"));
        Assert.False(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
    }

    /// <summary>
    /// EN: Ensures Oracle parser accepts ROW_COUNT() and rejects foreign row-count helper aliases.
    /// PT: Garante que o parser Oracle aceite ROW_COUNT() e rejeite aliases de row-count de outros bancos.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_LastFoundRowsFunctions_ShouldFollowDialectCapability(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        Assert.Equal("ROW_COUNT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROW_COUNT()", db, d)).Name, StringComparer.OrdinalIgnoreCase);

        var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", db, d));
        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures WITH RECURSIVE syntax is rejected for Oracle.
    /// PT: Garante que a sintaxe with recursive seja rejeitada no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithRecursive_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1 FROM dual) SELECT n FROM cte";

        if (version < OracleDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
            return;
        }

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Verifies WITH RECURSIVE rejection includes actionable guidance for Oracle syntax.
    /// PT: Verifica que a rejeição de WITH RECURSIVE inclui orientação acionável para sintaxe Oracle.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.WithCteMinVersion)]
    public void ParseSelect_WithRecursive_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1 FROM dual) SELECT n FROM cte", db, d));

        Assert.Contains("WITH sem RECURSIVE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects MATERIALIZED and NOT MATERIALIZED CTE hints.
    /// PT: Garante que o Oracle rejeite hints MATERIALIZED e NOT MATERIALIZED em CTE.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.WithCteMinVersion)]
    public void ParseSelect_WithMaterializedHint_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        var materializedEx = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH x AS MATERIALIZED (SELECT 1 FROM dual) SELECT 1 FROM x", db, d));
        Assert.Contains("WITH ... AS MATERIALIZED", materializedEx.Message, StringComparison.OrdinalIgnoreCase);

        var notMaterializedEx = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH x AS NOT MATERIALIZED (SELECT 1 FROM dual) SELECT 1 FROM x", db, d));
        Assert.Contains("WITH ... AS NOT MATERIALIZED", notMaterializedEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT syntax is rejected for Oracle.
    /// PT: Garante que a sintaxe ON CONFLICT seja rejeitada no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseInsert_OnConflict_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));

        Assert.Contains(SqlConst.MERGE, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE parsing follows Oracle version support and preserves target table metadata.
    /// PT: Garante que o parsing de MERGE siga o suporte por versão do Oracle e preserve metadados da tabela alvo.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseMerge_ShouldFollowOracleVersionSupport(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < OracleDialect.MergeMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
            return;
        }

        var parsed = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, db, d));
        Assert.NotNull(parsed.Table);
        Assert.Equal("users", parsed.Table!.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("target", parsed.Table.Alias, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle accepts OUTER APPLY in versions that support APPLY semantics.
    /// PT: Garante que o Oracle aceite OUTER APPLY nas versoes que suportam semantica APPLY.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = 12)]
    public void ParseSelect_OuterApply_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "SELECT COUNT(*) FROM users u OUTER APPLY (SELECT o.Note FROM orders o WHERE o.UserId = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, db, d));

        Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.OuterApply, parsed.Joins[0].Type);
    }


    /// <summary>
    /// EN: Ensures MERGE accepts the WHEN NOT MATCHED clause form in merge-capable dialect versions.
    /// PT: Garante que MERGE aceite a forma de cláusula WHEN NOT MATCHED em versões de dialeto com suporte.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithWhenNotMatched_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id)";

        var query = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, db, d));

        Assert.Equal("users", query.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures MERGE without USING is rejected with actionable parser guidance in Oracle dialect.
    /// PT: Garante que MERGE sem USING seja rejeitado com orientação acionável do parser no dialeto Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithoutUsing_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target ON target.id = 1 WHEN MATCHED THEN UPDATE SET name = 'x'", db, d));

        Assert.Contains("MERGE requer cláusula USING", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without ON is rejected with actionable parser guidance in Oracle dialect.
    /// PT: Garante que MERGE sem ON seja rejeitado com orientação acionável do parser no dialeto Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithoutOn_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src WHEN MATCHED THEN UPDATE SET name = 'x'", db, d));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires ON at top-level and does not accept ON tokens nested inside USING subqueries in Oracle dialect.
    /// PT: Garante que MERGE exija ON em nível top-level e não aceite tokens ON aninhados dentro de subqueries no USING no dialeto Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithOnOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING (SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE id > 0)) src WHEN MATCHED THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));

        Assert.Contains("MERGE requer cláusula ON", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE without WHEN is rejected with actionable parser guidance in Oracle dialect.
    /// PT: Garante que MERGE sem WHEN seja rejeitado com orientação acionável do parser no dialeto Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithoutWhen_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src ON target.id = src.id", db, d));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures pagination syntaxes normalize to the same row-limit AST shape for this dialect.
    /// PT: Garante que as sintaxes de paginação sejam normalizadas para o mesmo formato de AST de limite de linhas neste dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_PaginationSyntaxes_ShouldNormalizeRowLimitAst(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.OffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", db, d));
            return;
        }

        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            db, d));
        var fetchFirst = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id FETCH FIRST 2 ROWS ONLY",
            db, d));

        var normalizedOffsetFetch = Assert.IsType<SqlLimitOffset>(offsetFetch.RowLimit);
        var normalizedFetchFirst = Assert.IsType<SqlLimitOffset>(fetchFirst.RowLimit);

        Assert.Equal(new LiteralExpr(2), normalizedOffsetFetch.Count);
        Assert.Equal(new LiteralExpr(1), normalizedOffsetFetch.Offset);
        Assert.Equal(new LiteralExpr(2), normalizedFetchFirst.Count);
        Assert.Null(normalizedFetchFirst.Offset);
    }

    /// <summary>
    /// EN: Ensures Oracle accepts JSON_VALUE with RETURNING clause and preserves the payload in the scalar AST.
    /// PT: Garante que o Oracle aceite JSON_VALUE com cláusula RETURNING e preserve o payload na AST escalar.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_JsonValueWithReturningClause_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        if (version < OracleDialect.OracleJsonSqlFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("JSON_VALUE(payload, '$.a.b' RETURNING NUMBER)", db, d));
            Assert.Contains("JSON_VALUE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<CallExpr>(
            SqlExpressionParser.ParseScalar("JSON_VALUE(payload, '$.a.b' RETURNING NUMBER)", db, d));

        Assert.Equal("JSON_VALUE", parsed.Name, ignoreCase: true);
        Assert.Equal(3, parsed.Args.Count);
        Assert.Equal("RETURNING NUMBER", Assert.IsType<RawSqlExpr>(parsed.Args[2]).Sql, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures Oracle approximate aggregate helpers follow the explicit dialect capability by version.
    /// PT: Garante que os helpers Oracle de agregacao aproximada sigam a capability explicita do dialeto por versao.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_ApproximateAggregateHelpers_ShouldFollowDialectCapability(int version)
    {
        var dialect = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        Assert.Equal(
            version >= OracleDialect.ApproxCountDistinctMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT_AGG"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT_DETAIL"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_MEDIAN"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_PERCENTILE"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_PERCENTILE_AGG"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateAggregateFunction("APPROX_PERCENTILE_DETAIL"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateScalarFunction("TO_APPROX_COUNT_DISTINCT"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsApproximateScalarFunction("TO_APPROX_PERCENTILE"));
        Assert.Equal(
            version >= OracleDialect.OracleBinaryConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_BINARY_DOUBLE"));
        Assert.Equal(
            version >= OracleDialect.OracleBinaryConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_BINARY_FLOAT"));
        Assert.Equal(
            version >= OracleDialect.OracleBlobConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_BLOB"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_CLOB"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_DSINTERVAL"));
        Assert.True(dialect.SupportsOracleSpecificConversionFunction("TO_LOB"));
        Assert.True(dialect.SupportsOracleSpecificConversionFunction("TO_MULTI_BYTE"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_NCHAR"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_NCLOB"));
        Assert.True(dialect.SupportsOracleSpecificConversionFunction("TO_SINGLE_BYTE"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_TIMESTAMP_TZ"));
        Assert.Equal(
            version >= OracleDialect.OracleTextConversionMinVersion,
            dialect.SupportsOracleSpecificConversionFunction("TO_YMINTERVAL"));
        Assert.Equal(
            version >= OracleDialect.OracleScnFunctionMinVersion,
            dialect.SupportsOracleScnFunction("SCN_TO_TIMESTAMP"));
        Assert.Equal(
            version >= OracleDialect.OracleScnFunctionMinVersion,
            dialect.SupportsOracleScnFunction("TIMESTAMP_TO_SCN"));
        Assert.Equal(
            version >= 18,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_COMPARE"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_DETAILS"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_ID"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_SET"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_VALUE"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("NCGR"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("POWERMULTISET"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("POWERMULTISET_BY_CARDINALITY"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_BOUNDS"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_COST"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_DETAILS"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_PROBABILITY"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_SET"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PRESENTNNV"));
        Assert.Equal(
            version >= OracleDialect.ApproximateAnalyticsMinVersion,
            dialect.SupportsOracleAnalyticsFunction("PRESENTV"));
        Assert.Equal(
            version >= 8,
            dialect.SupportsOracleAnalyticsFunction("RATIO_TO_REPORT"));
        Assert.Equal(
            version >= OracleDialect.OracleAdvancedClusterFunctionMinVersion,
            dialect.SupportsOracleClusterFunction("CLUSTER_DETAILS"));
        Assert.Equal(
            version >= OracleDialect.OracleAdvancedClusterFunctionMinVersion,
            dialect.SupportsOracleClusterFunction("CLUSTER_DISTANCE"));
        Assert.Equal(
            version >= OracleDialect.OracleClusterFunctionMinVersion,
            dialect.SupportsOracleClusterFunction("CLUSTER_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleClusterFunctionMinVersion,
            dialect.SupportsOracleClusterFunction("CLUSTER_PROBABILITY"));
        Assert.Equal(
            version >= OracleDialect.OracleClusterFunctionMinVersion,
            dialect.SupportsOracleClusterFunction("CLUSTER_SET"));
        Assert.Equal(
            version >= OracleDialect.OracleContainerFunctionMinVersion,
            dialect.SupportsOracleContainerFunction("CON_DBID_TO_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleContainerFunctionMinVersion,
            dialect.SupportsOracleContainerFunction("CON_GUID_TO_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleContainerFunctionMinVersion,
            dialect.SupportsOracleContainerFunction("CON_NAME_TO_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleContainerFunctionMinVersion,
            dialect.SupportsOracleContainerFunction("CON_UID_TO_ID"));
        Assert.True(dialect.SupportsOracleRowIdFunction("ROWIDTOCHAR"));
        Assert.True(dialect.SupportsOracleRowIdFunction("ROWTONCHAR"));
        Assert.True(dialect.SupportsOracleUserEnvFunction("USERENV"));
        Assert.Equal(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_INVOKING_USER"));
        Assert.Equal(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_INVOKING_USERID"));
        Assert.Equal(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_DST_AFFECTED"));
        Assert.Equal(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_DST_CONVERT"));
        Assert.Equal(
            version >= OracleDialect.OracleUserEnvMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_DST_ERROR"));
        Assert.Equal(
            version >= OracleDialect.OraclePartitionMetadataMinVersion,
            dialect.SupportsOracleUserEnvFunction("ORA_DM_PARTITION_NAME"));
        Assert.Equal(
            version >= OracleDialect.OracleValidateConversionMinVersion,
            dialect.SupportsOracleValidationFunction("VALIDATE_CONVERSION"));
        Assert.Equal(
            version >= OracleDialect.OracleJsonTransformMinVersion,
            dialect.SupportsOracleJsonTransformFunction("JSON_TRANSFORM"));
        Assert.Equal(
            version >= OracleDialect.OracleJsonSqlFunctionMinVersion,
            dialect.SupportsJsonValueFunction);
        Assert.Equal(
            version >= OracleDialect.OracleJsonSqlFunctionMinVersion,
            dialect.SupportsJsonQueryFunction);
        Assert.Equal(
            version >= OracleDialect.OracleJsonSqlFunctionMinVersion,
            dialect.SupportsJsonTableFunction);
        Assert.Equal(
            version >= OracleDialect.OracleJsonSqlFunctionMinVersion,
            dialect.SupportsJsonValueReturningClause);
        Assert.Equal(
            version >= OracleDialect.OracleCollationFunctionMinVersion,
            dialect.SupportsOracleCollationFunction("COLLATION"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_CHARSET_DECL_LEN"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_CHARSET_ID"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_CHARSET_NAME"));
        Assert.Equal(
            version >= OracleDialect.OracleCollationFunctionMinVersion,
            dialect.SupportsOracleNlsFunction("NLS_COLLATION_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleCollationFunctionMinVersion,
            dialect.SupportsOracleNlsFunction("NLS_COLLATION_NAME"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_INITCAP"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_LOWER"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLS_UPPER"));
        Assert.True(dialect.SupportsOracleNlsFunction("NLSSORT"));
        Assert.Equal(
            version >= OracleDialect.OracleOraHashMinVersion,
            dialect.SupportsOracleHashFunction("ORA_HASH"));
        Assert.Equal(
            version >= OracleDialect.OracleStandardHashMinVersion,
            dialect.SupportsOracleHashFunction("STANDARD_HASH"));
        Assert.True(dialect.SupportsOracleSysFunction("SYS_GUID"));
        Assert.True(dialect.SupportsOracleSysFunction("SYS_CONTEXT"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_CONNECT_BY_PATH"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_DBURIGEN"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_EXTRACT_UTC"));
        Assert.Equal(
            version >= OracleDialect.OracleSysZoneIdMinVersion,
            dialect.SupportsOracleSysFunction("SYS_OP_ZONE_ID"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_TYPEID"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_XMLAGG"));
        Assert.Equal(
            version >= OracleDialect.OracleSysFamilyMinVersion,
            dialect.SupportsOracleSysFunction("SYS_XMLGEN"));

        if (version < OracleDialect.ApproxCountDistinctMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(amount)", db, dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal(
                "APPROX_COUNT_DISTINCT",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
        }

        if (version < OracleDialect.ApproximateAnalyticsMinVersion)
        {
            var exAgg = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_AGG(amount)", db, dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT_AGG", exAgg.Message, StringComparison.OrdinalIgnoreCase);

            var exDetail = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_DETAIL(amount)", db, dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT_DETAIL", exDetail.Message, StringComparison.OrdinalIgnoreCase);

            var exMedian = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_MEDIAN(amount)", db, dialect));
            Assert.Contains("APPROX_MEDIAN", exMedian.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentile = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE(amount, 0.5)", db, dialect));
            Assert.Contains("APPROX_PERCENTILE", exPercentile.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentileAgg = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_AGG(amount, 0.5)", db, dialect));
            Assert.Contains("APPROX_PERCENTILE_AGG", exPercentileAgg.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentileDetail = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_DETAIL(amount, 0.5)", db, dialect));
            Assert.Contains("APPROX_PERCENTILE_DETAIL", exPercentileDetail.Message, StringComparison.OrdinalIgnoreCase);

            var exToApproxCount = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("TO_APPROX_COUNT_DISTINCT(amount)", db, dialect));
            Assert.Contains("TO_APPROX_COUNT_DISTINCT", exToApproxCount.Message, StringComparison.OrdinalIgnoreCase);

            var exToApproxPercentile = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("TO_APPROX_PERCENTILE(amount)", db, dialect));
            Assert.Contains("TO_APPROX_PERCENTILE", exToApproxPercentile.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal(
                "APPROX_COUNT_DISTINCT_AGG",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_AGG(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_COUNT_DISTINCT_DETAIL",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_DETAIL(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_MEDIAN",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_MEDIAN(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE(amount, 0.5)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE_AGG",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_AGG(amount, 0.5)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE_DETAIL",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_DETAIL(amount, 0.5)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "TO_APPROX_COUNT_DISTINCT",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TO_APPROX_COUNT_DISTINCT(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "TO_APPROX_PERCENTILE",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TO_APPROX_PERCENTILE(amount)", db, dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
        }

        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_BINARY_DOUBLE(amount)", "TO_BINARY_DOUBLE", OracleDialect.OracleBinaryConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_BINARY_FLOAT(amount)", "TO_BINARY_FLOAT", OracleDialect.OracleBinaryConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_BLOB(amount)", "TO_BLOB", OracleDialect.OracleBlobConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_CLOB(amount)", "TO_CLOB", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_DSINTERVAL(amount)", "TO_DSINTERVAL", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_LOB(amount)", "TO_LOB", 7);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_MULTI_BYTE(amount)", "TO_MULTI_BYTE", 7);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_NCHAR(amount)", "TO_NCHAR", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_NCLOB(amount)", "TO_NCLOB", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_SINGLE_BYTE(amount)", "TO_SINGLE_BYTE", 7);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_TIMESTAMP_TZ(amount)", "TO_TIMESTAMP_TZ", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, db, dialect, "TO_YMINTERVAL(amount)", "TO_YMINTERVAL", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleScnParsing(version, db, dialect, "SCN_TO_TIMESTAMP(amount)", "SCN_TO_TIMESTAMP");
        AssertOracleScnParsing(version, db, dialect, "TIMESTAMP_TO_SCN(amount)", "TIMESTAMP_TO_SCN");
        AssertOracleAnalyticsParsing(version, db, dialect, "FEATURE_COMPARE(amount)", "FEATURE_COMPARE", 18);
        AssertOracleAnalyticsParsing(version, db, dialect, "FEATURE_DETAILS(amount)", "FEATURE_DETAILS", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "FEATURE_ID(amount)", "FEATURE_ID", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "FEATURE_SET(amount)", "FEATURE_SET", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "FEATURE_VALUE(amount)", "FEATURE_VALUE", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "NCGR(amount)", "NCGR", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "POWERMULTISET(amount, amount)", "POWERMULTISET", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "POWERMULTISET_BY_CARDINALITY(amount, amount)", "POWERMULTISET_BY_CARDINALITY", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION(amount)", "PREDICTION", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION_BOUNDS(amount)", "PREDICTION_BOUNDS", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION_COST(amount)", "PREDICTION_COST", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION_DETAILS(amount)", "PREDICTION_DETAILS", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION_PROBABILITY(amount)", "PREDICTION_PROBABILITY", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PREDICTION_SET(amount)", "PREDICTION_SET", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PRESENTNNV(amount)", "PRESENTNNV", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "PRESENTV(amount)", "PRESENTV", OracleDialect.ApproximateAnalyticsMinVersion);
        AssertOracleAnalyticsParsing(version, db, dialect, "RATIO_TO_REPORT(amount)", "RATIO_TO_REPORT", 8);
        AssertOracleVersionedParsing(version, db, dialect, "CLUSTER_DETAILS(amount, amount, amount)", "CLUSTER_DETAILS", OracleDialect.OracleAdvancedClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CLUSTER_DISTANCE(amount, amount, amount)", "CLUSTER_DISTANCE", OracleDialect.OracleAdvancedClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CLUSTER_ID(amount, amount, amount)", "CLUSTER_ID", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CLUSTER_PROBABILITY(amount, amount, amount)", "CLUSTER_PROBABILITY", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CLUSTER_SET(amount, amount, amount)", "CLUSTER_SET", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CON_DBID_TO_ID(amount)", "CON_DBID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CON_GUID_TO_ID(amount)", "CON_GUID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CON_NAME_TO_ID(amount)", "CON_NAME_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "CON_UID_TO_ID(amount)", "CON_UID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ROWIDTOCHAR(amount)", "ROWIDTOCHAR", 7);
        AssertOracleVersionedParsing(version, db, dialect, "ROWTONCHAR(amount)", "ROWTONCHAR", 0);
        AssertOracleVersionedParsing(version, db, dialect, "USERENV(amount)", "USERENV", 7);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_INVOKING_USER()", "ORA_INVOKING_USER", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_INVOKING_USERID()", "ORA_INVOKING_USERID", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_DST_AFFECTED(amount)", "ORA_DST_AFFECTED", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_DST_CONVERT(amount)", "ORA_DST_CONVERT", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_DST_ERROR(amount)", "ORA_DST_ERROR", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_DM_PARTITION_NAME()", "ORA_DM_PARTITION_NAME", OracleDialect.OraclePartitionMetadataMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "VALIDATE_CONVERSION(amount, 'NUMBER')", "VALIDATE_CONVERSION", OracleDialect.OracleValidateConversionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "JSON_TRANSFORM(amount)", "JSON_TRANSFORM", OracleDialect.OracleJsonTransformMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "JSON_VALUE(amount, '$.a')", "JSON_VALUE", OracleDialect.OracleJsonSqlFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "JSON_QUERY(amount, '$.a')", "JSON_QUERY", OracleDialect.OracleJsonSqlFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "COLLATION(amount)", "COLLATION", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_CHARSET_DECL_LEN(amount)", "NLS_CHARSET_DECL_LEN", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_CHARSET_ID(amount)", "NLS_CHARSET_ID", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_CHARSET_NAME(amount)", "NLS_CHARSET_NAME", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_COLLATION_ID(amount)", "NLS_COLLATION_ID", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_COLLATION_NAME(amount)", "NLS_COLLATION_NAME", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_INITCAP(amount)", "NLS_INITCAP", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_LOWER(amount)", "NLS_LOWER", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLS_UPPER(amount)", "NLS_UPPER", 7);
        AssertOracleVersionedParsing(version, db, dialect, "NLSSORT(amount)", "NLSSORT", 7);
        AssertOracleVersionedParsing(version, db, dialect, "ORA_HASH(amount)", "ORA_HASH", OracleDialect.OracleOraHashMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "STANDARD_HASH(amount)", "STANDARD_HASH", OracleDialect.OracleStandardHashMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_GUID()", "SYS_GUID", 7);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')", "SYS_CONTEXT", 7);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_CONNECT_BY_PATH(amount, '/')", "SYS_CONNECT_BY_PATH", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_DBURIGEN(amount, amount)", "SYS_DBURIGEN", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_EXTRACT_UTC(amount)", "SYS_EXTRACT_UTC", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_OP_ZONE_ID(amount)", "SYS_OP_ZONE_ID", OracleDialect.OracleSysZoneIdMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_TYPEID(amount)", "SYS_TYPEID", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_XMLAGG(amount)", "SYS_XMLAGG", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, db, dialect, "SYS_XMLGEN(amount)", "SYS_XMLGEN", OracleDialect.OracleSysFamilyMinVersion);
    }

    private static void AssertOracleSpecificConversionParsing(int version, OracleDbMock db, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, db, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, db, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleScnParsing(int version, OracleDbMock db, OracleDialect dialect, string sql, string functionName)
    {
        if (version < OracleDialect.OracleScnFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, db, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, db, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleAnalyticsParsing(int version, OracleDbMock db, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, db, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, db, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleVersionedParsing(int version, OracleDbMock db, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, db, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, db, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures MERGE does not accept a source alias named WHEN as a replacement for top-level WHEN clauses.
    /// PT: Garante que MERGE não aceite um alias de origem chamado WHEN como substituto para cláusulas WHEN em nível top-level.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithUsingAliasNamedWhen_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING users when ON target.id = when.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MERGE requires WHEN at top-level and does not accept WHEN tokens nested inside USING subqueries.
    /// PT: Garante que MERGE exija WHEN em nível top-level e não aceite tokens WHEN aninhados dentro de subqueries no USING.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithWhenOnlyInsideUsingSubquery_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING (SELECT CASE WHEN id > 0 THEN id ELSE 0 END AS id FROM users) src ON target.id = src.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures MERGE rejects invalid top-level WHEN forms that are not WHEN MATCHED/WHEN NOT MATCHED.
    /// PT: Garante que MERGE rejeite formas inválidas de WHEN em nível top-level que não sejam WHEN MATCHED/WHEN NOT MATCHED.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion(VersionGraterOrEqual = OracleDialect.MergeMinVersion)]
    public void ParseMerge_WithInvalidTopLevelWhenForm_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN src.id > 0 THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, db, d));

        Assert.Contains("MERGE requer ao menos uma cláusula WHEN", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects unsupported alias quoting style with an actionable message.
    /// PT: Garante que o Oracle rejeite estilo de quoting de alias não suportado com mensagem acionável.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithBacktickQuotedAlias_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", db, d));

        Assert.Contains("alias/identificadores", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("'`'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle accepts double-quoted aliases and preserves the normalized alias text in AST.
    /// PT: Garante que o Oracle aceite aliases com aspas duplas e preserve o texto normalizado do alias na AST.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithDoubleQuotedAlias_ShouldParseAndNormalizeAlias(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User Name\" FROM users",
            db, d));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures Oracle unescapes doubled double-quotes inside quoted aliases when normalizing AST alias text.
    /// PT: Garante que o Oracle faça unescape de aspas duplas duplicadas dentro de aliases quoted ao normalizar o texto do alias na AST.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithEscapedDoubleQuotedAlias_ShouldNormalizeEscapedQuote(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            db, d));

        var item = Assert.Single(parsed.SelectItems);
        Assert.Equal("User\"Name", item.Alias);
    }

    /// <summary>
    /// EN: Ensures SQL Server table hints are rejected for Oracle.
    /// PT: Garante que hints de tabela do SQL Server sejam rejeitados no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithSqlServerTableHints_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) query hints are rejected for Oracle.
    /// PT: Garante que hints SQL Server OPTION(...) sejam rejeitados para Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Use hints compatíveis", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server OPTION(...) is also rejected after UNION tails for Oracle.
    /// PT: Garante que OPTION(...) de SQL Server também seja rejeitado após tail de UNION no Oracle.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseUnion_WithSqlServerOptionHints_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "SELECT id FROM users UNION SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, db, d));
        Assert.Contains("OPTION(query hints)", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Ensures PIVOT clause parsing is available for this dialect.
    /// PT: Garante que o parsing da cláusula pivot esteja disponível para este dialeto.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseSelect_WithPivot_ShouldParse(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var parsed = SqlQueryParser.Parse(sql, db, d);

        Assert.IsType<SqlSelectQuery>(parsed);
    }



    /// <summary>
    /// EN: Verifies DELETE without FROM returns an actionable error message.
    /// PT: Verifica que DELETE sem FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseDelete_WithoutFrom_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", db, d));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies DELETE target alias before FROM returns an actionable error message.
    /// PT: Verifica que alias alvo de DELETE antes de FROM retorna mensagem de erro acionável.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseDelete_TargetAliasBeforeFrom_ShouldProvideActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", db, d));

        Assert.Contains("DELETE FROM", ex.Message, StringComparison.OrdinalIgnoreCase);
    }



    /// <summary>
    /// EN: Verifies unsupported top-level statements return guidance-focused errors.
    /// PT: Verifica que comandos de topo não suportados retornam erros com orientação.
    /// </summary>
    /// <param name="version">EN: Dialect version under test. PT: Versão do dialeto em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseUnsupportedTopLevelStatement_ShouldUseActionableMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", db, d));

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
    [MemberDataOracleVersion]
    public void RuntimeDialectRules_ShouldRemainStable(int version)
    {
        var d = Get(version, v => new OracleDialect(v));

        Assert.True(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.Decimal));
        Assert.True(d.AreUnionColumnTypesCompatible(DbType.String, DbType.AnsiString));
        Assert.False(d.AreUnionColumnTypesCompatible(DbType.Int32, DbType.String));

        Assert.True(d.IsIntegerCastTypeName("INT"));
        Assert.True(d.IsIntegerCastTypeName("NUMBER"));

        Assert.False(d.RegexInvalidPatternEvaluatesToFalse);
        Assert.True(d.SupportsTriggers);
    }


    /// <summary>
    /// EN: Ensures unsupported SQL uses the standard not-supported message.
    /// PT: Garante que SQL não suportado use a mensagem padrão de não suportado.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseUnsupportedSql_ShouldUseStandardNotSupportedMessage(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", db, d));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("oracle", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle rejects OWNED BY for sequence DDL even when CREATE SEQUENCE is supported.
    /// PT: Garante que o Oracle rejeite OWNED BY em DDL de sequence mesmo quando CREATE SEQUENCE estiver suportado.
    /// </summary>
    /// <param name="version">EN: Oracle dialect version under test. PT: Versão do dialeto Oracle em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseCreateSequence_OwnedBy_ShouldThrowNotSupported(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
            "CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1 OWNED BY sales.users.id",
            db, d));

        Assert.Contains("OWNED BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Validates window function capability by Oracle version and function name.
    /// PT: Valida a capacidade de funções de janela por versão do Oracle e nome da função.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void WindowFunctionCapability_ShouldRespectVersionAndKnownFunctions(int version)
    {
        var dialect = Get(version, v => new OracleDialect(v));

        var expected = version >= OracleDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.SupportsWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.SupportsWindowFunction("NTILE"));
        Assert.False(dialect.SupportsWindowFunction("PERCENTILE_CONT"));
    }

    /// <summary>
    /// EN: Ensures parser validates window function names against Oracle dialect capabilities by version.
    /// PT: Garante que o parser valide nomes de função de janela contra as capacidades do dialeto Oracle por versão.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionName_ShouldRespectDialectCapability(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(supported, db, d));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(supported, db, d);
        Assert.IsType<WindowFunctionExpr>(expr);
        Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(unsupported, db, d));
    }


    /// <summary>
    /// EN: Ensures window functions that require ordering reject OVER clauses without ORDER BY.
    /// PT: Garante que funções de janela que exigem ordenação rejeitem cláusulas OVER sem ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER ()", db, d));

        Assert.Contains("requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates window function argument arity for supported functions.
    /// PT: Garante que o parser valide a aridade dos argumentos de funções de janela suportadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", db, d));
            return;
        }

        var exRowNumber = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER(1) OVER (ORDER BY id)", db, d));
        Assert.Contains("does not accept arguments", exRowNumber.Message, StringComparison.OrdinalIgnoreCase);

        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE() OVER (ORDER BY id)", db, d));
        Assert.Contains("exactly 1 argument", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, 1, 0, 99) OVER (ORDER BY id)", db, d));
        Assert.Contains("between 1 and 3 arguments", exLag.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures parser validates literal semantic ranges for window function arguments.
    /// PT: Garante que o parser valide intervalos semânticos literais para argumentos de funções de janela.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));
        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", db, d));
            return;
        }


        var exNtile = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTILE(0) OVER (ORDER BY id)", db, d));
        Assert.Contains("positive integer literal", exNtile.Message, StringComparison.OrdinalIgnoreCase);

        var exLag = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LAG(id, -1, 0) OVER (ORDER BY id)", db, d));
        Assert.Contains("offset must be non-negative", exLag.Message, StringComparison.OrdinalIgnoreCase);

        var exNthValue = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("NTH_VALUE(id, 0) OVER (ORDER BY id)", db, d));
        Assert.Contains("greater than zero", exNthValue.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ORDER BY requirement for window functions is exposed through dialect runtime hook.
    /// PT: Garante que o requisito de ORDER BY para funções de janela seja exposto pelo hook de runtime do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = Get(version, v => new OracleDialect(v));

        var expected = version >= OracleDialect.WindowFunctionsMinVersion;
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("ROW_NUMBER"));
        Assert.Equal(expected, dialect.RequiresOrderByInWindowFunction("LAG"));

        Assert.False(dialect.RequiresOrderByInWindowFunction(SqlConst.COUNT));
    }


    /// <summary>
    /// EN: Ensures window function argument arity metadata is exposed through dialect hook.
    /// PT: Garante que os metadados de aridade de argumentos de função de janela sejam expostos pelo hook do dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = Get(version, v => new OracleDialect(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
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

        Assert.True(dialect.TryGetWindowFunctionArgumentArity(SqlConst.COUNT, out var countMin, out var countMax));
        Assert.Equal(1, countMin);
        Assert.Equal(1, countMax);
    }


    /// <summary>
    /// EN: Ensures supported window frame clauses parse on aggregate window functions.
    /// PT: Garante que cláusulas de frame suportadas sejam interpretadas em funções de janela agregadas.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", db, d));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", db, d);
        Assert.IsType<WindowFunctionExpr>(expr);

        expr = SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", db, d);
        Assert.IsType<WindowFunctionExpr>(expr);

        expr = SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)", db, d);
        Assert.IsType<WindowFunctionExpr>(expr);
    }



    /// <summary>
    /// EN: Ensures invalid window frame bound ordering is rejected by parser semantic validation.
    /// PT: Garante que ordenação inválida de limites de frame de janela seja rejeitada pela validação semântica do parser.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFrameClauseInvalidBounds_ShouldBeRejected(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("COUNT(*) OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", db, d));

        Assert.Contains("start bound cannot be greater", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures Oracle parser blocks non-native ordered-set aggregate names with WITHIN GROUP.
    /// PT: Garante que o parser Oracle bloqueie nomes não nativos de agregação ordered-set com WITHIN GROUP.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_StringAggWithinGroup_ShouldThrowNotSupported(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", db, d));

        Assert.Contains(SqlConst.STRING_AGG, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP clause fails with actionable ORDER BY message.
    /// PT: Garante que cláusula WITHIN GROUP malformada falhe com mensagem acionável de ORDER BY.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_ListAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (amount DESC)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (amount DESC)", db, d));

        Assert.Contains("WITHIN GROUP requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures trailing commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas finais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WithinGroupOrderByTrailingComma_ShouldThrowActionableError(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", db, d));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty ORDER BY lists in WITHIN GROUP are rejected with actionable message.
    /// PT: Garante que listas ORDER BY vazias em WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WithinGroupOrderByEmptyList_ShouldThrowActionableError(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY)", db, d));

        Assert.Contains("requires at least one expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures leading commas in WITHIN GROUP ORDER BY are rejected with actionable message.
    /// PT: Garante que vírgulas iniciais no ORDER BY do WITHIN GROUP sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WithinGroupOrderByLeadingComma_ShouldThrowActionableError(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", db, d));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures missing commas between WITHIN GROUP ORDER BY expressions are rejected with actionable message.
    /// PT: Garante que ausência de vírgula entre expressões de ORDER BY no WITHIN GROUP seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowActionableError(int version)
    {
        var d = Get(version, v => new OracleDialect(v));
        var db = Get(version, v => new OracleDbMock(v));

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", db, d));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", db, d));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
