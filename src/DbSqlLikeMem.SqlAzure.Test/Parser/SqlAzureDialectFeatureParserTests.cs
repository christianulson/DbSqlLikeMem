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
    /// EN: Ensures SQL Azure inherits APPLY support from the compatibility-mapped SQL Server dialect.
    /// PT: Garante que o SQL Azure herde o suporte a APPLY do dialeto SQL Server mapeado por compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ApplyCapability_ShouldFollowCompatibilityMappedDialect(int compatibilityLevel)
    {
        CreateDialect(compatibilityLevel).SupportsApplyClause.Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts CROSS APPLY with correlated derived subqueries through the shared SQL Server parser path.
    /// PT: Garante que o parser SQL Azure aceite CROSS APPLY com subqueries derivadas correlacionadas pelo caminho compartilhado do SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_CrossApplyDerivedSubquery_ShouldUseSharedSqlServerPath(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, latest.OrderId
            FROM Users u
            CROSS APPLY (
                SELECT TOP 1 o.OrderId
                FROM Orders o
                WHERE o.UserId = u.Id
                ORDER BY o.OrderId DESC
            ) latest
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, CreateDialect(compatibilityLevel)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.CrossApply, join.Type);
        Assert.NotNull(join.Table.Derived);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts CROSS APPLY OPENJSON only when compatibility reaches SQL Server 2016 semantics.
    /// PT: Garante que o parser SQL Azure aceite CROSS APPLY OPENJSON apenas quando a compatibilidade atingir a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_CrossApplyOpenJson_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, j.[value]
            FROM Users u
            CROSS APPLY OPENJSON(u.Email) j
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var join = Assert.Single(parsed.Joins);
        Assert.NotNull(join.Table.TableFunction);
        Assert.Equal("OPENJSON", join.Table.TableFunction!.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts OPENJSON WITH explicit schema only when compatibility reaches SQL Server 2016 semantics.
    /// PT: Garante que o parser SQL Azure aceite OPENJSON WITH com schema explicito apenas quando a compatibilidade atingir a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_CrossApplyOpenJsonWithSchema_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, data.Name, data.Qty
            FROM Users u
            CROSS APPLY OPENJSON(u.Email) WITH (
                Name NVARCHAR(20) '$.Name',
                Qty INT '$.Qty',
                PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON
            ) data
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var join = Assert.Single(parsed.Joins);
        var withClause = Assert.IsType<SqlOpenJsonWithClause>(join.Table.OpenJsonWithClause);
        Assert.Equal(3, withClause.Columns.Count);
        Assert.True(withClause.Columns[2].AsJson);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser preserves OPENJSON strict/lax path modifiers and quoted-key JSON paths in the explicit schema subset.
    /// PT: Garante que o parser SQL Azure preserve modificadores strict/lax do OPENJSON e paths JSON com chave entre aspas no subset de schema explicito.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_CrossApplyOpenJsonWithStrictQuotedPaths_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT data.Color
            FROM Users u
            CROSS APPLY OPENJSON(u.Email, 'strict $.items[1]') WITH (
                Color NVARCHAR(20) 'lax $."Name.With.Dot"'
            ) data
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("OPENJSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var join = Assert.Single(parsed.Joins);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal("strict $.items[1]", Assert.IsType<LiteralExpr>(function.Args[1]).Value);
        Assert.Equal("lax $.\"Name.With.Dot\"", join.Table.OpenJsonWithClause!.Columns[0].Path);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts OUTER APPLY STRING_SPLIT only when compatibility reaches SQL Server 2016 semantics.
    /// PT: Garante que o parser SQL Azure aceite OUTER APPLY STRING_SPLIT apenas quando a compatibilidade atingir a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_OuterApplyStringSplit_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, part.value
            FROM Users u
            OUTER APPLY STRING_SPLIT(u.Email, ',') part
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("STRING_SPLIT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var join = Assert.Single(parsed.Joins);
        Assert.NotNull(join.Table.TableFunction);
        Assert.Equal("STRING_SPLIT", join.Table.TableFunction!.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts STRING_SPLIT enable_ordinal only when compatibility reaches SQL Server 2022 semantics.
    /// PT: Garante que o parser SQL Azure aceite STRING_SPLIT com enable_ordinal apenas quando a compatibilidade atingir a semantica do SQL Server 2022.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_CrossApplyStringSplitWithOrdinal_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, part.value, part.ordinal
            FROM Users u
            CROSS APPLY STRING_SPLIT(u.Email, ',', 1) part
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2022)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var join = Assert.Single(parsed.Joins);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal("STRING_SPLIT", function.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(3, function.Args.Count);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts UNPIVOT through the shared SQL Server table-transform path.
    /// PT: Garante que o parser SQL Azure aceite UNPIVOT pelo caminho compartilhado de transformacao tabular do SQL Server.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_WithUnpivot_ShouldPopulateTableTransform(int compatibilityLevel)
    {
        const string sql = """
            SELECT up.Id, up.FieldName, up.FieldValue
            FROM (SELECT Id, Name, Email FROM Users) src
            UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, CreateDialect(compatibilityLevel)));
        var source = Assert.IsType<SqlTableSource>(parsed.Table);
        var unpivot = Assert.IsType<SqlUnpivotSpec>(source.Unpivot);

        Assert.Equal("up", source.Alias, ignoreCase: true);
        Assert.Equal("FieldValue", unpivot.ValueColumnName, ignoreCase: true);
        Assert.Equal("FieldName", unpivot.NameColumnName, ignoreCase: true);
        Assert.Equal(2, unpivot.InItems.Count);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts FOR JSON PATH only when compatibility reaches SQL Server 2016 semantics.
    /// PT: Garante que o parser SQL Azure aceite FOR JSON PATH apenas quando a compatibilidade atingir a semantica do SQL Server 2016.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_ForJsonPath_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id AS [User.Id], u.Name AS [User.Name]
            FROM Users u
            ORDER BY u.Id
            FOR JSON PATH, ROOT('users')
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("FOR JSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var forJson = Assert.IsType<SqlForJsonClause>(parsed.ForJson);
        Assert.Equal(SqlForJsonMode.Path, forJson.Mode);
        Assert.Equal("users", forJson.RootName, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Azure parser accepts FOR JSON AUTO options through compatibility-mapped SQL Server semantics.
    /// PT: Garante que o parser SQL Azure aceite opcoes de FOR JSON AUTO pela semantica do SQL Server mapeada por compatibilidade.
    /// </summary>
    /// <param name="compatibilityLevel">EN: SQL Azure compatibility level under test. PT: Nivel de compatibilidade SQL Azure em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlAzureCompatibilityLevel]
    public void ParseSelect_ForJsonAutoWithOptions_ShouldRespectCompatibilityLevel(int compatibilityLevel)
    {
        const string sql = """
            SELECT u.Id, u.Name, u.Email
            FROM Users u
            WHERE u.Id = 1
            FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
            """;

        var dialect = CreateDialect(compatibilityLevel);
        if (compatibilityLevel < SqlAzureDbCompatibilityLevels.SqlServer2016)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("FOR JSON", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, dialect));
        var forJson = Assert.IsType<SqlForJsonClause>(parsed.ForJson);
        Assert.Equal(SqlForJsonMode.Auto, forJson.Mode);
        Assert.True(forJson.IncludeNullValues);
        Assert.True(forJson.WithoutArrayWrapper);
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
