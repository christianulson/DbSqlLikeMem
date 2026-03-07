using DbSqlLikeMem.SqlServer;

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
    /// EN: Ensures SQL Azure parser accepts ordered-set WITHIN GROUP for STRING_AGG through the shared SQL Server dialect path.
    /// PT: Garante que o parser SQL Azure aceite ordered-set WITHIN GROUP para STRING_AGG pelo caminho compartilhado do dialeto SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int compatibilityLevel)
    {
        var expr = SqlExpressionParser.ParseScalar(
            "STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC, id ASC)",
            CreateDialect(compatibilityLevel));

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
        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar(
                "STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)",
                CreateDialect(compatibilityLevel)));

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
        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(
            "SELECT STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC) AS joined FROM orders",
            CreateDialect(compatibilityLevel)));

        Assert.Single(parsed.SelectItems);
        Assert.Contains("STRING_AGG", parsed.SelectItems[0].Raw, StringComparison.OrdinalIgnoreCase);
    }
}
