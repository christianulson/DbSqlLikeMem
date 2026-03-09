namespace DbSqlLikeMem.SqlAzure.Test.Parser;

/// <summary>
/// EN: Covers SQL Azure parser behavior that reuses SQL Server syntax gates through compatibility levels.
/// PT: Cobre o comportamento de parser do SQL Azure que reutiliza gates de sintaxe do SQL Server por nivel de compatibilidade.
/// </summary>
public sealed class SqlAzureDialectFeatureParserTests
{
    private static SqlDialectBase CreateDialect(int compatibilityLevel)
        => new SqlAzureDbMock(compatibilityLevel).Dialect;

    /// <summary>
    /// EN: Ensures CREATE SEQUENCE follows SQL Azure compatibility levels mapped to SQL Server 2012 semantics.
    /// PT: Garante que CREATE SEQUENCE siga os niveis de compatibilidade do SQL Azure mapeados para a semantica do SQL Server 2012.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseCreateSequence_ShouldFollowCompatibilityLevelSupport(int compatibilityLevel)
    {
        const string sql = "CREATE SEQUENCE sales.seq_orders START WITH 20 INCREMENT BY 10";
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2012)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            return;
        }

        var parsed = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Equal("sales", parsed.Table?.DbName, ignoreCase: true);
        Assert.Equal("seq_orders", parsed.Table?.Name, ignoreCase: true);
        Assert.Equal(20L, parsed.StartValue);
        Assert.Equal(10L, parsed.IncrementBy);
    }

    /// <summary>
    /// EN: Ensures OFFSET/FETCH without ORDER BY follows SQL Azure compatibility-level rules.
    /// PT: Garante que OFFSET/FETCH sem ORDER BY siga as regras por nivel de compatibilidade do SQL Azure.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_OffsetWithoutOrderBy_ShouldRespectCompatibilityLevelRule(int compatibilityLevel)
    {
        const string sql = "SELECT id FROM users OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2012)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, dialect));
        Assert.Contains("Adicione ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures OFFSET/FETCH pagination is normalized in SQL Azure parser paths once compatibility allows it.
    /// PT: Garante que a paginacao OFFSET/FETCH seja normalizada nos caminhos de parser do SQL Azure quando a compatibilidade permitir.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_OffsetFetch_ShouldNormalizeRowLimitAst(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2012)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlQueryParser.Parse("SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY", dialect));
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT id FROM users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY",
            dialect));

        var rowLimit = Assert.IsType<SqlLimitOffset>(parsed.RowLimit);
        Assert.Equal(2, rowLimit.Count);
        Assert.Equal(1, rowLimit.Offset);
    }

    /// <summary>
    /// EN: Ensures SQL Azure keeps the SQL Server pagination hint for unsupported LIMIT syntax.
    /// PT: Garante que o SQL Azure mantenha a dica de paginacao do SQL Server para sintaxe LIMIT nao suportada.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_Limit_ShouldProvidePaginationHint(int compatibilityLevel)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users ORDER BY id LIMIT 5", CreateDialect(compatibilityLevel)));

        Assert.Contains("LIMIT", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("FETCH", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure compatibility levels preserve MERGE parser support inherited from SQL Server 2008+.
    /// PT: Garante que os niveis de compatibilidade do SQL Azure preservem o suporte de parser a MERGE herdado do SQL Server 2008+.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseMerge_ShouldFollowCompatibilityLevelSupport(int compatibilityLevel)
    {
        const string sql = "MERGE INTO users target USING users src ON target.id = src.id WHEN MATCHED THEN UPDATE SET name = 'x'";

        var parsed = Assert.IsType<SqlMergeQuery>(SqlQueryParser.Parse(sql, CreateDialect(compatibilityLevel)));
        Assert.Equal("users", parsed.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures JSON_VALUE follows SQL Azure compatibility thresholds mapped to SQL Server 2016 semantics.
    /// PT: Garante que JSON_VALUE siga os limiares de compatibilidade do SQL Azure mapeados para a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_JsonValue_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = "SELECT JSON_VALUE(data, '$.name') AS name FROM users";
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Single(parsed.SelectItems);
        Assert.Contains("JSON_VALUE", parsed.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure inherits SQL Server row-count capabilities through the compatibility-mapped dialect.
    /// PT: Garante que o SQL Azure herde as capabilities de row-count do SQL Server pelo dialeto mapeado por compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void LastFoundRowsCapability_ShouldFollowCompatibilityMappedDialect(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        Assert.True(dialect.SupportsLastFoundRowsFunction("ROWCOUNT"));
        Assert.True(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
        Assert.False(dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"));
    }

    /// <summary>
    /// EN: Ensures SQL Azure inherits SQL Server join-mutation capabilities through the compatibility-mapped dialect.
    /// PT: Garante que o SQL Azure herde as capabilities de mutacao com join do SQL Server pelo dialeto mapeado por compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void MutationCapabilities_ShouldFollowCompatibilityMappedDialect(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        Assert.False(dialect.SupportsUpdateJoinFromSubquerySyntax);
        Assert.True(dialect.SupportsUpdateFromJoinSubquerySyntax);
        Assert.True(dialect.SupportsDeleteTargetFromJoinSubquerySyntax);
        Assert.False(dialect.SupportsDeleteUsingSubquerySyntax);
        Assert.False(dialect.SupportsSqlCalcFoundRowsModifier);
        Assert.Equal(2, dialect.GetInsertUpsertAffectedRowCount(1, 1));
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser inherits SQL Server row-count function gating through compatibility mapping.
    /// PT: Garante que o parser SQL Azure herde o gate de função row-count do SQL Server pelo mapeamento de compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_LastFoundRowsFunctions_ShouldFollowCompatibilityMappedDialect(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        Assert.Equal("ROWCOUNT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROWCOUNT()", dialect)).Name, StringComparer.OrdinalIgnoreCase);

        var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", dialect));
        Assert.Contains("FOUND_ROWS", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser/tokenizer inherits @@ROWCOUNT syntax support from the compatibility-mapped SQL Server dialect.
    /// PT: Garante que o parser/tokenizer SQL Azure herde o suporte de sintaxe a @@ROWCOUNT do dialeto SQL Server mapeado por compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_SystemRowCountIdentifier_ShouldFollowCompatibilityMappedDialect(int compatibilityLevel)
    {
        var expr = SqlExpressionParser.ParseScalar("@@ROWCOUNT", CreateDialect(compatibilityLevel));
        var identifier = Assert.IsType<IdentifierExpr>(expr);

        Assert.Equal("@@ROWCOUNT", identifier.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures OPENJSON follows SQL Azure compatibility thresholds mapped to SQL Server 2016 semantics.
    /// PT: Garante que OPENJSON siga os limiares de compatibilidade do SQL Azure mapeados para a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_OpenJson_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = "OPENJSON(payload)";
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar(sql, dialect));

            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(sql, dialect);
        var call = Assert.IsType<CallExpr>(expr);
        Assert.Equal("OPENJSON", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures NEXT/PREVIOUS VALUE FOR follow SQL Azure compatibility thresholds mapped to SQL Server sequence-expression semantics.
    /// PT: Garante que NEXT/PREVIOUS VALUE FOR sigam os limiares de compatibilidade do SQL Azure mapeados para a semantica de expressoes de sequence do SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_SequenceValueFunctions_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2012)
        {
            var nextEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("NEXT VALUE FOR sales.seq_orders", dialect));
            Assert.Contains("NEXT VALUE FOR", nextEx.Message, StringComparison.OrdinalIgnoreCase);

            var previousEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("PREVIOUS VALUE FOR sales.seq_orders", dialect));
            Assert.Contains("PREVIOUS VALUE FOR", previousEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var nextExpr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("NEXT VALUE FOR sales.seq_orders", dialect));
        Assert.Equal("NEXT_VALUE_FOR", nextExpr.Name, StringComparer.OrdinalIgnoreCase);

        var previousExSupported = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("PREVIOUS VALUE FOR sales.seq_orders", dialect));
        Assert.Contains("PREVIOUS VALUE FOR", previousExSupported.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts ordered-set WITHIN GROUP for STRING_AGG through the shared SQL Server dialect path.
    /// PT: Garante que o parser SQL Azure aceite ordered-set WITHIN GROUP para STRING_AGG pelo caminho compartilhado do dialeto SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2017)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar(
                    "STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC, id ASC)",
                    dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(
            "STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC, id ASC)",
            dialect);

        var call = Assert.IsType<CallExpr>(expr);
        Assert.Equal("STRING_AGG", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Equal(2, call.WithinGroupOrderBy!.Count);
        Assert.True(call.WithinGroupOrderBy[0].Desc);
        Assert.False(call.WithinGroupOrderBy[1].Desc);
    }

    /// <summary>
    /// EN: Ensures malformed WITHIN GROUP in SQL Azure keeps the actionable ORDER BY diagnostic from the shared parser path.
    /// PT: Garante que WITHIN GROUP malformado no SQL Azure preserve o diagnostico acionavel de ORDER BY do caminho compartilhado do parser.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_StringAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2017)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar(
                    "STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)",
                    dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar(
                "STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)",
                dialect));

        Assert.Contains("WITHIN GROUP requires ORDER BY", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures full SELECT parsing in SQL Azure accepts STRING_AGG ordered-set syntax inherited from SQL Server semantics.
    /// PT: Garante que o parsing de SELECT completo no SQL Azure aceite a sintaxe ordered-set de STRING_AGG herdada da semantica do SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_StringAggWithinGroup_ShouldParse(int compatibilityLevel)
    {
        var dialect = CreateDialect(compatibilityLevel);

        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2017)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(
                "SELECT STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders",
                dialect));
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders",
            dialect));

        Assert.Single(parsed.SelectItems);
        Assert.Contains("STRING_AGG", parsed.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }
}
