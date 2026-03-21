namespace DbSqlLikeMem.Oracle.Test.Parser;

/// <summary>
/// EN: Covers Oracle-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do Oracle.
/// </summary>
public sealed class OracleDialectFeatureParserTests
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
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD payload VARBINARY(16) NULL",
            new OracleDialect(version)));

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
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE users ADD amount DECIMAL(10, 4) NOT NULL DEFAULT 0",
            new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users ADD status VARCHAR(20) NOT NULL DEFAULT NULL",
            new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE users u ADD age INT",
            new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE (SELECT * FROM users) u ADD age INT",
            new OracleDialect(version)));

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
        var dialect = new OracleDialect(version);

        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue; END",
            dialect));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal("NUMBER", create.ReturnTypeSql, ignoreCase: true);
        Assert.Equal(2, create.Parameters.Count);
        Assert.Equal("baseValue", create.Parameters[0].Name, ignoreCase: true);
        Assert.Equal("incrementValue", create.Parameters[1].Name, ignoreCase: true);
        Assert.IsType<BinaryExpr>(create.Body);

        var drop = Assert.IsType<SqlDropFunctionQuery>(SqlQueryParser.Parse(
            "DROP FUNCTION fn_users",
            dialect));

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
        var dialect = new OracleDialect(version);
        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE OR REPLACE FUNCTION fn_users(baseValue NUMBER, incrementValue NUMBER) RETURN NUMBER IS BEGIN RETURN baseValue + incrementValue + 1; END",
            dialect));
        Assert.True(create.OrReplace);
        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal(2, create.Parameters.Count);
        Assert.IsType<BinaryExpr>(create.Body);
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
        var dialect = new OracleDialect(version);

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
        var dialect = new OracleDialect(version);

        Assert.Equal("ROW_COUNT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROW_COUNT()", dialect)).Name, StringComparer.OrdinalIgnoreCase);

        var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", dialect));
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
        var sql = "WITH RECURSIVE cte(n) AS (SELECT 1 FROM dual) SELECT n FROM cte";

        if (version < OracleDialect.WithCteMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
            return;
        }

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
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
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("WITH RECURSIVE cte(n) AS (SELECT 1 FROM dual) SELECT n FROM cte", new OracleDialect(version)));

        Assert.Contains("WITH sem RECURSIVE", ex.Message, StringComparison.OrdinalIgnoreCase);
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
        var sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        if (version < OracleDialect.MergeMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new OracleDialect(version)));
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
        const string sql = "SELECT COUNT(*) FROM users u OUTER APPLY (SELECT o.Note FROM orders o WHERE o.UserId = u.Id ORDER BY o.Id DESC FETCH FIRST 1 ROW ONLY) x";

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN NOT MATCHED THEN INSERT (id) VALUES (src.id)";

        var query = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target ON target.id = 1 WHEN MATCHED THEN UPDATE SET name = 'x'", new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src WHEN MATCHED THEN UPDATE SET name = 'x'", new OracleDialect(version)));

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
        const string sql = "MERGE INTO users target USING (SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE id > 0)) src WHEN MATCHED THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("MERGE INTO users target USING users src ON target.id = src.id", new OracleDialect(version)));

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
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.OffsetFetchMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", dialect));
            return;
        }

        var offsetFetch = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));
        var fetchFirst = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id FETCH FIRST 2 ROWS ONLY",
            dialect));

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
        var dialect = new OracleDialect(version);
        if (version < OracleDialect.OracleJsonSqlFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("JSON_VALUE(payload, '$.a.b' RETURNING NUMBER)", dialect));
            Assert.Contains("JSON_VALUE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<CallExpr>(
            SqlExpressionParser.ParseScalar("JSON_VALUE(payload, '$.a.b' RETURNING NUMBER)", dialect));

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
        var dialect = new OracleDialect(version);

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
            version >= 12,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_DETAILS"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_ID"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_SET"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("FEATURE_VALUE"));
        Assert.Equal(
            version >= 18,
            dialect.SupportsOracleAnalyticsFunction("NCGR"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("POWERMULTISET"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("POWERMULTISET_BY_CARDINALITY"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION"));
        Assert.Equal(
            version >= 11,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_BOUNDS"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_COST"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_DETAILS"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_PROBABILITY"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PREDICTION_SET"));
        Assert.Equal(
            version >= 10,
            dialect.SupportsOracleAnalyticsFunction("PRESENTNNV"));
        Assert.Equal(
            version >= 10,
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
        Assert.Equal(
            version >= OracleDialect.OracleRowToNCharFunctionMinVersion,
            dialect.SupportsOracleRowIdFunction("ROWTONCHAR"));
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
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(amount)", dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal(
                "APPROX_COUNT_DISTINCT",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
        }

        if (version < OracleDialect.ApproximateAnalyticsMinVersion)
        {
            var exAgg = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_AGG(amount)", dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT_AGG", exAgg.Message, StringComparison.OrdinalIgnoreCase);

            var exDetail = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_DETAIL(amount)", dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT_DETAIL", exDetail.Message, StringComparison.OrdinalIgnoreCase);

            var exMedian = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_MEDIAN(amount)", dialect));
            Assert.Contains("APPROX_MEDIAN", exMedian.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentile = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE(amount, 0.5)", dialect));
            Assert.Contains("APPROX_PERCENTILE", exPercentile.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentileAgg = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_AGG(amount, 0.5)", dialect));
            Assert.Contains("APPROX_PERCENTILE_AGG", exPercentileAgg.Message, StringComparison.OrdinalIgnoreCase);

            var exPercentileDetail = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_DETAIL(amount, 0.5)", dialect));
            Assert.Contains("APPROX_PERCENTILE_DETAIL", exPercentileDetail.Message, StringComparison.OrdinalIgnoreCase);

            var exToApproxCount = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("TO_APPROX_COUNT_DISTINCT(amount)", dialect));
            Assert.Contains("TO_APPROX_COUNT_DISTINCT", exToApproxCount.Message, StringComparison.OrdinalIgnoreCase);

            var exToApproxPercentile = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("TO_APPROX_PERCENTILE(amount)", dialect));
            Assert.Contains("TO_APPROX_PERCENTILE", exToApproxPercentile.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal(
                "APPROX_COUNT_DISTINCT_AGG",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_AGG(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_COUNT_DISTINCT_DETAIL",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT_DETAIL(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_MEDIAN",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_MEDIAN(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE(amount, 0.5)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE_AGG",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_AGG(amount, 0.5)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "APPROX_PERCENTILE_DETAIL",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_PERCENTILE_DETAIL(amount, 0.5)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "TO_APPROX_COUNT_DISTINCT",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TO_APPROX_COUNT_DISTINCT(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
            Assert.Equal(
                "TO_APPROX_PERCENTILE",
                Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TO_APPROX_PERCENTILE(amount)", dialect)).Name,
                StringComparer.OrdinalIgnoreCase);
        }

        AssertOracleSpecificConversionParsing(version, dialect, "TO_BINARY_DOUBLE(amount)", "TO_BINARY_DOUBLE", OracleDialect.OracleBinaryConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_BINARY_FLOAT(amount)", "TO_BINARY_FLOAT", OracleDialect.OracleBinaryConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_BLOB(amount)", "TO_BLOB", OracleDialect.OracleBlobConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_CLOB(amount)", "TO_CLOB", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_DSINTERVAL(amount)", "TO_DSINTERVAL", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_LOB(amount)", "TO_LOB", 7);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_MULTI_BYTE(amount)", "TO_MULTI_BYTE", 7);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_NCHAR(amount)", "TO_NCHAR", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_NCLOB(amount)", "TO_NCLOB", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_SINGLE_BYTE(amount)", "TO_SINGLE_BYTE", 7);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_TIMESTAMP_TZ(amount)", "TO_TIMESTAMP_TZ", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleSpecificConversionParsing(version, dialect, "TO_YMINTERVAL(amount)", "TO_YMINTERVAL", OracleDialect.OracleTextConversionMinVersion);
        AssertOracleScnParsing(version, dialect, "SCN_TO_TIMESTAMP(amount)", "SCN_TO_TIMESTAMP");
        AssertOracleScnParsing(version, dialect, "TIMESTAMP_TO_SCN(amount)", "TIMESTAMP_TO_SCN");
        AssertOracleAnalyticsParsing(version, dialect, "FEATURE_COMPARE(amount)", "FEATURE_COMPARE", 18);
        AssertOracleAnalyticsParsing(version, dialect, "FEATURE_DETAILS(amount)", "FEATURE_DETAILS", 12);
        AssertOracleAnalyticsParsing(version, dialect, "FEATURE_ID(amount)", "FEATURE_ID", 10);
        AssertOracleAnalyticsParsing(version, dialect, "FEATURE_SET(amount)", "FEATURE_SET", 10);
        AssertOracleAnalyticsParsing(version, dialect, "FEATURE_VALUE(amount)", "FEATURE_VALUE", 10);
        AssertOracleAnalyticsParsing(version, dialect, "NCGR(amount)", "NCGR", 18);
        AssertOracleAnalyticsParsing(version, dialect, "POWERMULTISET(amount, amount)", "POWERMULTISET", 10);
        AssertOracleAnalyticsParsing(version, dialect, "POWERMULTISET_BY_CARDINALITY(amount, amount)", "POWERMULTISET_BY_CARDINALITY", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION(amount)", "PREDICTION", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION_BOUNDS(amount)", "PREDICTION_BOUNDS", 11);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION_COST(amount)", "PREDICTION_COST", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION_DETAILS(amount)", "PREDICTION_DETAILS", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION_PROBABILITY(amount)", "PREDICTION_PROBABILITY", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PREDICTION_SET(amount)", "PREDICTION_SET", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PRESENTNNV(amount)", "PRESENTNNV", 10);
        AssertOracleAnalyticsParsing(version, dialect, "PRESENTV(amount)", "PRESENTV", 10);
        AssertOracleAnalyticsParsing(version, dialect, "RATIO_TO_REPORT(amount)", "RATIO_TO_REPORT", 8);
        AssertOracleVersionedParsing(version, dialect, "CLUSTER_DETAILS(amount, amount, amount)", "CLUSTER_DETAILS", OracleDialect.OracleAdvancedClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CLUSTER_DISTANCE(amount, amount, amount)", "CLUSTER_DISTANCE", OracleDialect.OracleAdvancedClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CLUSTER_ID(amount, amount, amount)", "CLUSTER_ID", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CLUSTER_PROBABILITY(amount, amount, amount)", "CLUSTER_PROBABILITY", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CLUSTER_SET(amount, amount, amount)", "CLUSTER_SET", OracleDialect.OracleClusterFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CON_DBID_TO_ID(amount)", "CON_DBID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CON_GUID_TO_ID(amount)", "CON_GUID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CON_NAME_TO_ID(amount)", "CON_NAME_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "CON_UID_TO_ID(amount)", "CON_UID_TO_ID", OracleDialect.OracleContainerFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ROWIDTOCHAR(amount)", "ROWIDTOCHAR", 7);
        AssertOracleVersionedParsing(version, dialect, "ROWTONCHAR(amount)", "ROWTONCHAR", OracleDialect.OracleRowToNCharFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "USERENV(amount)", "USERENV", 7);
        AssertOracleVersionedParsing(version, dialect, "ORA_INVOKING_USER()", "ORA_INVOKING_USER", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ORA_INVOKING_USERID()", "ORA_INVOKING_USERID", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ORA_DST_AFFECTED(amount)", "ORA_DST_AFFECTED", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ORA_DST_CONVERT(amount)", "ORA_DST_CONVERT", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ORA_DST_ERROR(amount)", "ORA_DST_ERROR", OracleDialect.OracleUserEnvMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "ORA_DM_PARTITION_NAME()", "ORA_DM_PARTITION_NAME", OracleDialect.OraclePartitionMetadataMinVersion);
        AssertOracleVersionedParsing(version, dialect, "VALIDATE_CONVERSION(amount, 'NUMBER')", "VALIDATE_CONVERSION", OracleDialect.OracleValidateConversionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "JSON_TRANSFORM(amount)", "JSON_TRANSFORM", OracleDialect.OracleJsonTransformMinVersion);
        AssertOracleVersionedParsing(version, dialect, "JSON_VALUE(amount, '$.a')", "JSON_VALUE", OracleDialect.OracleJsonSqlFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "JSON_QUERY(amount, '$.a')", "JSON_QUERY", OracleDialect.OracleJsonSqlFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "COLLATION(amount)", "COLLATION", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "NLS_CHARSET_DECL_LEN(amount)", "NLS_CHARSET_DECL_LEN", 7);
        AssertOracleVersionedParsing(version, dialect, "NLS_CHARSET_ID(amount)", "NLS_CHARSET_ID", 7);
        AssertOracleVersionedParsing(version, dialect, "NLS_CHARSET_NAME(amount)", "NLS_CHARSET_NAME", 7);
        AssertOracleVersionedParsing(version, dialect, "NLS_COLLATION_ID(amount)", "NLS_COLLATION_ID", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "NLS_COLLATION_NAME(amount)", "NLS_COLLATION_NAME", OracleDialect.OracleCollationFunctionMinVersion);
        AssertOracleVersionedParsing(version, dialect, "NLS_INITCAP(amount)", "NLS_INITCAP", 7);
        AssertOracleVersionedParsing(version, dialect, "NLS_LOWER(amount)", "NLS_LOWER", 7);
        AssertOracleVersionedParsing(version, dialect, "NLS_UPPER(amount)", "NLS_UPPER", 7);
        AssertOracleVersionedParsing(version, dialect, "NLSSORT(amount)", "NLSSORT", 7);
        AssertOracleVersionedParsing(version, dialect, "ORA_HASH(amount)", "ORA_HASH", OracleDialect.OracleOraHashMinVersion);
        AssertOracleVersionedParsing(version, dialect, "STANDARD_HASH(amount)", "STANDARD_HASH", OracleDialect.OracleStandardHashMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_GUID()", "SYS_GUID", 7);
        AssertOracleVersionedParsing(version, dialect, "SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA')", "SYS_CONTEXT", 7);
        AssertOracleVersionedParsing(version, dialect, "SYS_CONNECT_BY_PATH(amount, '/')", "SYS_CONNECT_BY_PATH", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_DBURIGEN(amount, amount)", "SYS_DBURIGEN", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_EXTRACT_UTC(amount)", "SYS_EXTRACT_UTC", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_OP_ZONE_ID(amount)", "SYS_OP_ZONE_ID", OracleDialect.OracleSysZoneIdMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_TYPEID(amount)", "SYS_TYPEID", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_XMLAGG(amount)", "SYS_XMLAGG", OracleDialect.OracleSysFamilyMinVersion);
        AssertOracleVersionedParsing(version, dialect, "SYS_XMLGEN(amount)", "SYS_XMLGEN", OracleDialect.OracleSysFamilyMinVersion);
    }

    private static void AssertOracleSpecificConversionParsing(int version, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleScnParsing(int version, OracleDialect dialect, string sql, string functionName)
    {
        if (version < OracleDialect.OracleScnFunctionMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleAnalyticsParsing(int version, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect)).Name,
            StringComparer.OrdinalIgnoreCase);
    }

    private static void AssertOracleVersionedParsing(int version, OracleDialect dialect, string sql, string functionName, int minVersion)
    {
        if (version < minVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains(functionName, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal(
            functionName,
            Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect)).Name,
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
        const string sql = "MERGE INTO users target USING users when ON target.id = when.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        const string sql = "MERGE INTO users target USING (SELECT CASE WHEN id > 0 THEN id ELSE 0 END AS id FROM users) src ON target.id = src.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN src.id > 0 THEN UPDATE SET name = 'x'";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));

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
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT name `User Name` FROM users", new OracleDialect(version)));

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
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User Name\" FROM users",
            new OracleDialect(version)));

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
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT name \"User\"\"Name\" FROM users",
            new OracleDialect(version)));

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
        var sql = "SELECT id FROM users WITH (NOLOCK)";

        Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
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
        var sql = "SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
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
        var sql = "SELECT id FROM users UNION SELECT id FROM users OPTION (MAXDOP 1)";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new OracleDialect(version)));
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
        var sql = "SELECT t10 FROM (SELECT tenantid, id FROM users) src PIVOT (COUNT(id) FOR tenantid IN (10 AS t10)) p";

        var parsed = SqlQueryParser.Parse(sql, new OracleDialect(version));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE users WHERE id = 1", new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("DELETE u FROM users u", new OracleDialect(version)));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse("UPSERT INTO users VALUES (1)", new OracleDialect(version)));

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
        var d = new OracleDialect(version);

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
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users WITH (NOLOCK)", new OracleDialect(version)));

        Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.Ordinal);
        Assert.Contains("oracle", ex.Message, StringComparison.OrdinalIgnoreCase);
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
        var dialect = new OracleDialect(version);

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
        var supported = "ROW_NUMBER() OVER (ORDER BY id)";
        var unsupported = "PERCENTILE_CONT(0.5) OVER (ORDER BY id)";
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.WindowFunctionsMinVersion)
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
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionWithoutOrderBy_ShouldRespectDialectRules(int version)
    {
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.WindowFunctionsMinVersion)
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
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionArguments_ShouldValidateArity(int version)
    {
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.WindowFunctionsMinVersion)
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
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFunctionLiteralArguments_ShouldValidateSemanticRange(int version)
    {
        var dialect = new OracleDialect(version);
        if (version < OracleDialect.WindowFunctionsMinVersion)
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
    [MemberDataOracleVersion]
    public void WindowFunctionOrderByRequirementHook_ShouldRespectVersion(int version)
    {
        var dialect = new OracleDialect(version);

        var expected = version >= OracleDialect.WindowFunctionsMinVersion;
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
    [MemberDataOracleVersion]
    public void WindowFunctionArgumentArityHook_ShouldRespectVersion(int version)
    {
        var dialect = new OracleDialect(version);

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

        Assert.False(dialect.TryGetWindowFunctionArgumentArity("COUNT", out _, out _));
    }


    /// <summary>
    /// EN: Ensures ROWS window frame clauses parse when supported and RANGE remains gated.
    /// PT: Garante que cláusulas ROWS de frame de janela sejam interpretadas quando suportadas e que RANGE continue bloqueado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataOracleVersion]
    public void ParseScalar_WindowFrameClause_ShouldRespectDialectCapabilities(int version)
    {
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
        Assert.IsType<WindowFunctionExpr>(expr);

        expr = SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", dialect);
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
        var dialect = new OracleDialect(version);

        if (version < OracleDialect.WindowFunctionsMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("ROW_NUMBER() OVER (ORDER BY id ROWS BETWEEN CURRENT ROW AND 1 PRECEDING)", dialect));

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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));

        Assert.Contains("STRING_AGG", ex.Message, StringComparison.OrdinalIgnoreCase);
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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (amount DESC)", dialect));

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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

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
        var dialect = new OracleDialect(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("LISTAGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
