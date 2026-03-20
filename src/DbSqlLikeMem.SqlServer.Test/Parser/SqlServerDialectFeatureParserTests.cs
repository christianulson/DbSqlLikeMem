namespace DbSqlLikeMem.SqlServer.Test.Parser;

/// <summary>
/// EN: Covers SQL Server-specific parser feature behavior.
/// PT: Cobre o comportamento de recursos de parser específicos do SQL Server.
/// </summary>
public sealed class SqlServerDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures JSON_TABLE is rejected in SQL Server dialect with an explicit dialect gate.
    /// PT: Garante que JSON_TABLE seja rejeitado no dialeto SQL Server com gate explicito de dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_JsonTable_ShouldBeRejected(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("JSON_TABLE(payload, '$[*]' COLUMNS(x INT PATH '$'))", new SqlServerDialect(version)));

        Assert.Contains(SqlConst.JSON_TABLE, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server dialect keeps MATCH ... AGAINST disabled in its explicit capability hook.
    /// PT: Garante que o dialeto SQL Server mantenha MATCH ... AGAINST desabilitado em seu hook explicito de capability.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MatchAgainstCapability_ShouldBeDisabled(int version)
    {
        Assert.False(new SqlServerDialect(version).SupportsMatchAgainstPredicate);
    }

    /// <summary>
    /// EN: Ensures SQL Server row-count helpers are exposed through dialect-owned capabilities.
    /// PT: Garante que os helpers de row-count do SQL Server sejam expostos por capabilities do próprio dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void LastFoundRowsCapability_ShouldExposeSqlServerFunctionAndIdentifier(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsLastFoundRowsFunction("ROWCOUNT"));
        Assert.True(dialect.SupportsLastFoundRowsIdentifier("@@ROWCOUNT"));
        Assert.False(dialect.SupportsLastFoundRowsFunction("FOUND_ROWS"));
    }

    /// <summary>
    /// EN: Ensures SQL Server exposes its join-based mutation syntax through dialect-owned capabilities.
    /// PT: Garante que o SQL Server exponha sua sintaxe de mutacao com join por capabilities do proprio dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MutationCapabilities_ShouldExposeSqlServerContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.False(dialect.SupportsUpdateJoinFromSubquerySyntax);
        Assert.True(dialect.SupportsUpdateFromJoinSubquerySyntax);
        Assert.True(dialect.SupportsDeleteTargetFromJoinSubquerySyntax);
        Assert.False(dialect.SupportsDeleteUsingSubquerySyntax);
        Assert.False(dialect.SupportsSqlCalcFoundRowsModifier);
        Assert.Equal(2, dialect.GetInsertUpsertAffectedRowCount(1, 1));
    }

    /// <summary>
    /// EN: Ensures SQL Server exposes APPLY support only for versions that already include the native clause.
    /// PT: Garante que o SQL Server exponha suporte a APPLY apenas para versoes que ja incluem a clausula nativa.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ApplyCapability_ShouldFollowSqlServerVersionSupport(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.WithCteMinVersion, dialect.SupportsApplyClause);
    }

    /// <summary>
    /// EN: Ensures SQL Server metadata functions added to the mock remain enabled for every supported version.
    /// PT: Garante que as funcoes de metadados do SQL Server adicionadas ao mock permaneçam habilitadas para todas as versoes suportadas.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MetadataFunctions_ShouldBeEnabledForAllVersions(int version)
    {
        var dialect = new SqlServerDialect(version);
        var functions = new[]
        {
            "APPLOCK_MODE",
            "APPLOCK_TEST",
            "ASSEMBLYPROPERTY",
            "CERTENCODED",
            "CERTPRIVATEKEY",
            "CURSOR_STATUS",
            "FILE_ID",
            "FILE_IDEX",
            "FILE_NAME",
            "FILEGROUP_ID",
            "FILEGROUP_NAME",
            "FILEGROUPPROPERTY",
            "FILEPROPERTY",
            "FULLTEXTCATALOGPROPERTY",
            "FULLTEXTSERVICEPROPERTY",
            "GET_FILESTREAM_TRANSACTION_CONTEXT",
            "HAS_PERMS_BY_NAME",
            "INDEX_COL",
            "INDEXKEY_PROPERTY",
            "INDEXPROPERTY",
            "MIN_ACTIVE_ROWVERSION",
            "OBJECT_DEFINITION",
            "PWDCOMPARE",
            "PWDENCRYPT",
            "STATS_DATE",
        };

        foreach (var name in functions)
            Assert.True(dialect.SupportsSqlServerMetadataFunction(name), name);
    }

    /// <summary>
    /// EN: Ensures SQL Server metadata identifiers include @@TEXTSIZE for every supported version.
    /// PT: Garante que os identificadores de metadados do SQL Server incluam @@TEXTSIZE para todas as versoes suportadas.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MetadataIdentifiers_ShouldIncludeTextSize(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("@@TEXTSIZE"));
    }

    /// <summary>
    /// EN: Ensures SQL Server scalar functions include NEWSEQUENTIALID for every supported version.
    /// PT: Garante que funcoes escalares do SQL Server incluam NEWSEQUENTIALID para todas as versoes suportadas.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ScalarFunctions_ShouldIncludeNewSequentialId(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerScalarFunction("NEWSEQUENTIALID"));
    }

    /// <summary>
    /// EN: Ensures SQL Server parses CROSS APPLY with correlated derived subqueries once the dialect version supports the native clause.
    /// PT: Garante que o SQL Server interprete CROSS APPLY com subqueries derivadas correlacionadas quando a versao do dialeto suportar a clausula nativa.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_CrossApplyDerivedSubquery_ShouldFollowVersionSupport(int version)
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

        if (version < SqlServerDialect.WithCteMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains("CROSS APPLY", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.CrossApply, join.Type);
        Assert.NotNull(join.Table.Derived);
        Assert.Equal("latest", join.Table.Alias, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses OUTER APPLY with correlated derived subqueries once the dialect version supports the native clause.
    /// PT: Garante que o SQL Server interprete OUTER APPLY com subqueries derivadas correlacionadas quando a versao do dialeto suportar a clausula nativa.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_OuterApplyDerivedSubquery_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, latest.OrderId
            FROM Users u
            OUTER APPLY (
                SELECT TOP 1 o.OrderId
                FROM Orders o
                WHERE o.UserId = u.Id
                ORDER BY o.OrderId DESC
            ) latest
            """;

        if (version < SqlServerDialect.WithCteMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains("OUTER APPLY", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.OuterApply, join.Type);
        Assert.NotNull(join.Table.Derived);
        Assert.Equal("latest", join.Table.Alias, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses CROSS APPLY OPENJSON only once the dialect version reaches the SQL Server 2016 JSON feature set.
    /// PT: Garante que o SQL Server interprete CROSS APPLY OPENJSON apenas quando a versao do dialeto atingir o conjunto JSON do SQL Server 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_CrossApplyOpenJson_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, j.[value]
            FROM Users u
            CROSS APPLY OPENJSON(u.Email) j
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.OPENJSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.CrossApply, join.Type);
        Assert.NotNull(join.Table.TableFunction);
        Assert.Equal(SqlConst.OPENJSON, join.Table.TableFunction!.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses OPENJSON WITH explicit schema only once the dialect version reaches the SQL Server 2016 JSON feature set.
    /// PT: Garante que o SQL Server interprete OPENJSON WITH com schema explicito apenas quando a versao do dialeto atingir o conjunto JSON do SQL Server 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_CrossApplyOpenJsonWithSchema_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, data.Name, data.Qty
            FROM Users u
            CROSS APPLY OPENJSON(u.Email) WITH (
                Name NVARCHAR(20) '$.Name',
                Qty INT '$.Qty',
                PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON,
                RawJson NVARCHAR(MAX) '$' AS JSON
            ) data
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.OPENJSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        var withClause = Assert.IsType<SqlOpenJsonWithClause>(join.Table.OpenJsonWithClause);
        Assert.Equal(4, withClause.Columns.Count);
        Assert.Equal("Name", withClause.Columns[0].Name, ignoreCase: true);
        Assert.Equal(DbType.String, withClause.Columns[0].DbType);
        Assert.Equal("$.Qty", withClause.Columns[1].Path);
        Assert.Equal(DbType.Int32, withClause.Columns[1].DbType);
        Assert.True(withClause.Columns[2].AsJson);
        Assert.Equal("$", withClause.Columns[3].Path);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser preserves OPENJSON path modifiers and quoted-key JSON paths in the explicit schema subset.
    /// PT: Garante que o parser SQL Server preserve modificadores de path do OPENJSON e paths JSON com chave entre aspas no subset de schema explicito.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_CrossApplyOpenJsonWithStrictQuotedPaths_ShouldPreservePaths(int version)
    {
        const string sql = """
            SELECT data.Color
            FROM Users u
            CROSS APPLY OPENJSON(u.Email, 'strict $.items[1]') WITH (
                Color NVARCHAR(20) 'lax $."Name.With.Dot"'
            ) data
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.OPENJSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal("strict $.items[1]", Assert.IsType<LiteralExpr>(function.Args[1]).Value);
        Assert.Equal("lax $.\"Name.With.Dot\"", join.Table.OpenJsonWithClause!.Columns[0].Path);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses OUTER APPLY STRING_SPLIT only once the dialect version reaches the SQL Server 2016 function set.
    /// PT: Garante que o SQL Server interprete OUTER APPLY STRING_SPLIT apenas quando a versao do dialeto atingir o conjunto de funcoes do SQL Server 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_OuterApplyStringSplit_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, part.value
            FROM Users u
            OUTER APPLY STRING_SPLIT(u.Email, ',') part
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.STRING_SPLIT, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal(SqlJoinType.OuterApply, join.Type);
        Assert.NotNull(join.Table.TableFunction);
        Assert.Equal(SqlConst.STRING_SPLIT, join.Table.TableFunction!.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses STRING_SPLIT enable_ordinal only once the dialect version reaches SQL Server 2022 semantics.
    /// PT: Garante que o SQL Server interprete STRING_SPLIT com enable_ordinal apenas quando a versao do dialeto atingir a semantica do SQL Server 2022.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_CrossApplyStringSplitWithOrdinal_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, part.value, part.ordinal
            FROM Users u
            CROSS APPLY STRING_SPLIT(u.Email, ',', 1) part
            """;

        if (version < SqlServerDialect.StringSplitOrdinalMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            //Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal(SqlConst.STRING_SPLIT, function.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(3, function.Args.Count);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses UNPIVOT sources into the shared table-transform AST shape.
    /// PT: Garante que o SQL Server interprete fontes UNPIVOT no shape compartilhado de transformacao tabular da AST.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_WithUnpivot_ShouldPopulateTableTransform(int version)
    {
        const string sql = """
            SELECT up.Id, up.FieldName, up.FieldValue
            FROM (SELECT Id, Name, Email FROM Users) src
            UNPIVOT (FieldValue FOR FieldName IN (Name, Email)) up
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var source = Assert.IsType<SqlTableSource>(parsed.Table);
        var unpivot = Assert.IsType<SqlUnpivotSpec>(source.Unpivot);

        Assert.Equal("up", source.Alias, ignoreCase: true);
        Assert.Equal("FieldValue", unpivot.ValueColumnName, ignoreCase: true);
        Assert.Equal("FieldName", unpivot.NameColumnName, ignoreCase: true);
        Assert.Equal(2, unpivot.InItems.Count);
        Assert.Equal("Name", unpivot.InItems[0].SourceColumnName, ignoreCase: true);
        Assert.Equal("Email", unpivot.InItems[1].SourceColumnName, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses FOR JSON PATH only once the dialect version reaches the SQL Server 2016 JSON feature set.
    /// PT: Garante que o SQL Server interprete FOR JSON PATH apenas quando a versao do dialeto atingir o conjunto JSON do SQL Server 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_ForJsonPath_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id AS [User.Id], u.Name AS [User.Name]
            FROM Users u
            ORDER BY u.Id
            FOR JSON PATH, ROOT('users')
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.FOR_JSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var forJson = Assert.IsType<SqlForJsonClause>(parsed.ForJson);
        Assert.Equal(SqlForJsonMode.Path, forJson.Mode);
        Assert.Equal("users", forJson.RootName, ignoreCase: true);
        Assert.False(forJson.IncludeNullValues);
        Assert.False(forJson.WithoutArrayWrapper);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses FOR JSON AUTO options into the shared JSON serialization clause shape.
    /// PT: Garante que o SQL Server interprete opcoes de FOR JSON AUTO no shape compartilhado da clausula de serializacao JSON.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_ForJsonAutoWithOptions_ShouldPopulateClause(int version)
    {
        const string sql = """
            SELECT u.Id, u.Name, u.Email
            FROM Users u
            WHERE u.Id = 1
            FOR JSON AUTO, INCLUDE_NULL_VALUES, WITHOUT_ARRAY_WRAPPER
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.FOR_JSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var forJson = Assert.IsType<SqlForJsonClause>(parsed.ForJson);
        Assert.Equal(SqlForJsonMode.Auto, forJson.Mode);
        Assert.True(forJson.IncludeNullValues);
        Assert.True(forJson.WithoutArrayWrapper);
        Assert.Null(forJson.RootName);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts ROWCOUNT() and rejects foreign row-count helper aliases.
    /// PT: Garante que o parser SQL Server aceite ROWCOUNT() e rejeite aliases de row-count de outros bancos.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_LastFoundRowsFunctions_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal("ROWCOUNT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROWCOUNT()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ROWCOUNT_BIG", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROWCOUNT_BIG()", dialect)).Name, StringComparer.OrdinalIgnoreCase);

        var foundRowsEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FOUND_ROWS()", dialect));
        Assert.Contains("FOUND_ROWS", foundRowsEx.Message, StringComparison.OrdinalIgnoreCase);

        var rowCountEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ROW_COUNT()", dialect));
        Assert.Contains("ROW_COUNT", rowCountEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server metadata and session scalar helpers are exposed through an explicit dialect capability.
    /// PT: Garante que helpers escalares de metadados e sessao do SQL Server sejam expostos por uma capability explicita do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MetadataFunctionCapability_ShouldExposeSqlServerContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerMetadataFunction("DB_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("CURRENT_REQUEST_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("CURRENT_TRANSACTION_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("CONTEXT_INFO"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("DATABASE_PRINCIPAL_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("DATABASEPROPERTYEX"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("DB_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("CONNECTIONPROPERTY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("COLUMNPROPERTY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("COL_LENGTH"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("COL_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("OBJECT_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("OBJECTPROPERTYEX"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("OBJECT_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("OBJECT_SCHEMA_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ORIGINAL_DB_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ORIGINAL_LOGIN"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("APP_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("GETANSINULL"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("HOST_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("HOST_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("IS_MEMBER"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("IS_ROLEMEMBER"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("IS_SRVROLEMEMBER"));
        Assert.Equal(version >= SqlServerDialect.SessionContextMinVersion, dialect.SupportsSqlServerMetadataFunction("SESSION_CONTEXT"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_LINE"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_MESSAGE"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_NUMBER"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_PROCEDURE"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_SEVERITY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("ERROR_STATE"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SCOPE_IDENTITY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SCHEMA_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SCHEMA_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SERVERPROPERTY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SESSION_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SUSER_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SUSER_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SUSER_SID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("SUSER_SNAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("TYPE_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("TYPE_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("TYPEPROPERTY"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("USER_ID"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("USER_NAME"));
        Assert.True(dialect.SupportsSqlServerMetadataFunction("XACT_STATE"));
        Assert.False(dialect.SupportsSqlServerMetadataFunction("FOUND_ROWS"));
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts metadata and session helpers only through the explicit shared metadata capability.
    /// PT: Garante que o parser SQL Server aceite helpers de metadados e sessao apenas pela capability explicita compartilhada de metadados.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_MetadataFunctions_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal("APP_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APP_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CURRENT_REQUEST_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CURRENT_REQUEST_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CURRENT_TRANSACTION_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CURRENT_TRANSACTION_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CONTEXT_INFO", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CONTEXT_INFO()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATABASE_PRINCIPAL_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATABASE_PRINCIPAL_ID('dbo')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATABASEPROPERTYEX", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATABASEPROPERTYEX('DefaultSchema', 'Status')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CONNECTIONPROPERTY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CONNECTIONPROPERTY('net_transport')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("COLUMNPROPERTY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COLUMNPROPERTY(OBJECT_ID('Users'), 'Name', 'ColumnId')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("COL_LENGTH", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COL_LENGTH('Users', 'Id')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("COL_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COL_NAME(2, 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DB_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DB_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DB_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DB_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OBJECT_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("OBJECT_ID('Users')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OBJECTPROPERTY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("OBJECTPROPERTY(OBJECT_ID('Users'), 'IsTable')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OBJECTPROPERTYEX", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("OBJECTPROPERTYEX(OBJECT_ID('sp_ping'), 'IsProcedure')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OBJECT_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("OBJECT_NAME(2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("OBJECT_SCHEMA_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("OBJECT_SCHEMA_NAME(2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ORIGINAL_DB_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ORIGINAL_DB_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ORIGINAL_LOGIN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ORIGINAL_LOGIN()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("GETANSINULL", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("GETANSINULL()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("HOST_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("HOST_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("HOST_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("HOST_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("IS_MEMBER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("IS_MEMBER('db_owner')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("IS_ROLEMEMBER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("IS_ROLEMEMBER('db_datareader')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("IS_SRVROLEMEMBER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("IS_SRVROLEMEMBER('sysadmin')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.SessionContextMinVersion)
        {
            var sessionContextEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("SESSION_CONTEXT(N'tenant_id')", dialect));
            Assert.Contains("SESSION_CONTEXT", sessionContextEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("SESSION_CONTEXT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SESSION_CONTEXT(N'tenant_id')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("ERROR_LINE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_LINE()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ERROR_MESSAGE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_MESSAGE()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ERROR_NUMBER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_NUMBER()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ERROR_PROCEDURE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_PROCEDURE()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ERROR_SEVERITY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_SEVERITY()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ERROR_STATE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ERROR_STATE()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SCOPE_IDENTITY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SCOPE_IDENTITY()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SCHEMA_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SCHEMA_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SCHEMA_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SCHEMA_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SERVERPROPERTY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SERVERPROPERTY('ProductVersion')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SESSION_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SESSION_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SUSER_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SUSER_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SUSER_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SUSER_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SUSER_SID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SUSER_SID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SUSER_SNAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SUSER_SNAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TYPE_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TYPE_ID('int')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TYPE_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TYPE_NAME(56)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TYPEPROPERTY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TYPEPROPERTY('int', 'OwnerId')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("USER_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("USER_ID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("USER_NAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("USER_NAME()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("XACT_STATE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("XACT_STATE()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server session and user identifiers are exposed through an explicit dialect capability.
    /// PT: Garante que identificadores de sessao e usuario do SQL Server sejam expostos por uma capability explicita do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void MetadataIdentifierCapability_ShouldExposeSqlServerContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("CURRENT_USER"));
        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("@@DATEFIRST"));
        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("@@IDENTITY"));
        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("@@MAX_PRECISION"));
        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("SESSION_USER"));
        Assert.True(dialect.SupportsSqlServerMetadataIdentifier("SYSTEM_USER"));
        Assert.False(dialect.SupportsSqlServerMetadataIdentifier("CURRENT_ROLE"));
    }

    /// <summary>
    /// EN: Ensures SQL Server date helpers are exposed through an explicit dialect capability.
    /// PT: Garante que helpers de data do SQL Server sejam expostos por uma capability explicita do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void DateFunctionCapability_ShouldExposeSqlServerContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerDateFunction("DATEDIFF"));
        Assert.True(dialect.SupportsSqlServerDateFunction("DATENAME"));
        Assert.True(dialect.SupportsSqlServerDateFunction("DATEPART"));
        Assert.True(dialect.SupportsSqlServerDateFunction("DAY"));
        Assert.True(dialect.SupportsSqlServerDateFunction("MONTH"));
        Assert.True(dialect.SupportsSqlServerDateFunction("YEAR"));
        Assert.False(dialect.SupportsSqlServerDateFunction("TIMESTAMPDIFF"));
    }

    /// <summary>
    /// EN: Ensures SQL Server aggregate helpers are exposed through explicit dialect capabilities.
    /// PT: Garante que helpers de agregacao do SQL Server sejam expostos por capabilities explicitas do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void AggregateFunctionCapability_ShouldExposeSqlServerContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.ApproxCountDistinctMinVersion, dialect.SupportsApproximateAggregateFunction("APPROX_COUNT_DISTINCT"));
        Assert.False(dialect.SupportsSqlServerAggregateFunction("APPROX_COUNT_DISTINCT"));
        Assert.True(dialect.SupportsSqlServerAggregateFunction("CHECKSUM_AGG"));
        Assert.False(dialect.SupportsSqlServerAggregateFunction("SUM"));
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts approximate and checksum aggregate helpers only through explicit dialect capabilities.
    /// PT: Garante que o parser SQL Server aceite helpers aproximados e de checksum apenas por capabilities explicitas do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_SqlServerAggregateHelpers_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.ApproxCountDistinctMinVersion)
        {
            var approxEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(Name)", dialect));
            Assert.Contains("APPROX_COUNT_DISTINCT", approxEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("APPROX_COUNT_DISTINCT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("APPROX_COUNT_DISTINCT(Name)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }

        Assert.Equal("CHECKSUM_AGG", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CHECKSUM_AGG(Name)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts date helpers only through the explicit SQL Server date-function capability.
    /// PT: Garante que o parser SQL Server aceite helpers de data apenas pela capability explicita de funcoes de data do SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_SqlServerDateHelpers_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal("DATEDIFF", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATEDIFF(day, '2020-01-01', '2020-01-03')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATENAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATENAME(month, '2020-02-10')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATEPART", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATEPART(month, '2020-02-10')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DAY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DAY('2020-02-14')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("MONTH", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("MONTH('2020-02-14')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("YEAR", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("YEAR('2020-02-14')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts identifier-style session and user tokens only through the explicit metadata-identifier capability.
    /// PT: Garante que o parser SQL Server aceite tokens de sessao e usuario em estilo identificador apenas pela capability explicita de identificador de metadados.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_MetadataIdentifiers_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal("CURRENT_USER", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("CURRENT_USER", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("@@DATEFIRST", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("@@DATEFIRST", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("@@IDENTITY", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("@@IDENTITY", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("@@MAX_PRECISION", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("@@MAX_PRECISION", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SESSION_USER", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("SESSION_USER", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SYSTEM_USER", Assert.IsType<IdentifierExpr>(SqlExpressionParser.ParseScalar("SYSTEM_USER", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server string and scalar helpers are exposed through an explicit dialect capability.
    /// PT: Garante que helpers de string e escalares do SQL Server sejam expostos por uma capability explicita do dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void SqlServerScalarFunctionCapability_ShouldExposeContract(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsSqlServerScalarFunction("ABS"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ACOS"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ASCII"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ASIN"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ATAN"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ATN2"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("BINARY_CHECKSUM"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("CEILING"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("CHARINDEX"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("CHECKSUM"));
        Assert.Equal(version >= SqlServerDialect.CompressionFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("COMPRESS"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("COS"));
        Assert.Equal(version >= SqlServerDialect.CompressionFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("DECOMPRESS"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("COT"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("DEGREES"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("DIFFERENCE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("EXP"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("FLOOR"));
        Assert.Equal(version >= SqlServerDialect.FormatMinVersion, dialect.SupportsSqlServerScalarFunction("FORMAT"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("FORMATMESSAGE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("DATALENGTH"));
        Assert.Equal(version >= SqlServerDialect.DateDiffBigMinVersion, dialect.SupportsSqlServerScalarFunction("DATEDIFF_BIG"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("GROUPING"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("GROUPING_ID"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ISDATE"));
        Assert.Equal(version >= SqlServerDialect.JsonFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("ISJSON"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ISNUMERIC"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("CHAR"));
        Assert.True(dialect.SupportsSqlServerScalarFunction(SqlConst.CONCAT));
        Assert.True(dialect.SupportsSqlServerScalarFunction(SqlConst.CONCAT_WS));
        Assert.True(dialect.SupportsSqlServerScalarFunction("LEN"));
        Assert.True(dialect.SupportsSqlServerScalarFunction(SqlConst.LEFT));
        Assert.True(dialect.SupportsSqlServerScalarFunction("LOG"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("LOG10"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("LOWER"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("NCHAR"));
        Assert.Equal(version >= SqlServerDialect.JsonFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("JSON_MODIFY"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("NEWID"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("PATINDEX"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("PI"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("POWER"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("RADIANS"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("RAND"));
        Assert.True(dialect.SupportsSqlServerScalarFunction(SqlConst.REPLACE));
        Assert.True(dialect.SupportsSqlServerScalarFunction(SqlConst.RIGHT));
        Assert.True(dialect.SupportsSqlServerScalarFunction("ROUND"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SIGN"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SIN"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SQUARE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("STR"));
        Assert.Equal(version >= SqlServerDialect.StringEscapeMinVersion, dialect.SupportsSqlServerScalarFunction("STRING_ESCAPE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SUBSTRING"));
        Assert.Equal(version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("SWITCHOFFSET"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("TAN"));
        Assert.Equal(version >= SqlServerDialect.DateTimeOffsetFunctionsMinVersion, dialect.SupportsSqlServerScalarFunction("TODATETIMEOFFSET"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("TRIM"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("UPPER"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("LTRIM"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("PARSENAME"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("QUOTENAME"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("REPLICATE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("REVERSE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("RTRIM"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SOUNDEX"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SPACE"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("SQRT"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("STUFF"));
        Assert.True(dialect.SupportsSqlServerScalarFunction("UNICODE"));
        Assert.False(dialect.SupportsSqlServerScalarFunction("JSON_VALUE"));
    }

    /// <summary>
    /// EN: Ensures SQL Server parser accepts selected string and scalar helpers only through the explicit SQL Server scalar-function capability.
    /// PT: Garante que o parser SQL Server aceite helpers selecionados de string e escalares apenas pela capability explicita de funcoes escalares do SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_SqlServerScalarHelpers_ShouldFollowDialectCapability(int version)
    {
        var dialect = new SqlServerDialect(version);

        Assert.Equal("ABS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ABS(-10)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ACOS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ACOS(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ASCII", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ASCII('A')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ASIN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ASIN(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ATAN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ATAN(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ATN2", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ATN2(0, 1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("BINARY_CHECKSUM", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("BINARY_CHECKSUM('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CEILING", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CEILING(1.2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CHARINDEX", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CHARINDEX('bar', 'foobar')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CHECKSUM", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CHECKSUM('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.CompressionFunctionsMinVersion)
        {
            var compressEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("COMPRESS('Ana')", dialect));
            Assert.Contains("COMPRESS", compressEx.Message, StringComparison.OrdinalIgnoreCase);

            var decompressEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("DECOMPRESS(0x1F8B)", dialect));
            Assert.Contains("DECOMPRESS", decompressEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("COMPRESS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COMPRESS('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
            Assert.Equal("DECOMPRESS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DECOMPRESS(0x1F8B)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("COS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COS(0)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("COT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("COT(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DEGREES", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DEGREES(PI())", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DIFFERENCE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DIFFERENCE('Robert', 'Rupert')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("EXP", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("EXP(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("FLOOR", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("FLOOR(1.9)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.FormatMinVersion)
        {
            var formatEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("FORMAT(42, 'D4')", dialect));
            Assert.Contains("FORMAT", formatEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("FORMAT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("FORMAT(42, 'D4')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("FORMATMESSAGE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("FORMATMESSAGE('Hello %s #%d', 'Bob', 7)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATALENGTH", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATALENGTH('AB')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.DateDiffBigMinVersion)
        {
            var dateDiffBigEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("DATEDIFF_BIG(day, '2020-01-01', '2020-01-03')", dialect));
            Assert.Contains("DATEDIFF_BIG", dateDiffBigEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("DATEDIFF_BIG", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("DATEDIFF_BIG(day, '2020-01-01', '2020-01-03')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("GROUPING", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("GROUPING(1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("GROUPING_ID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("GROUPING_ID(1, 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ISDATE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ISDATE('2020-01-01')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var isJsonEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("ISJSON('{\"a\":1}')", dialect));
            Assert.Contains("ISJSON", isJsonEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("ISJSON", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ISJSON('{\"a\":1}')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("ISNUMERIC", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ISNUMERIC('10.5')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("CHAR", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CHAR(65)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(SqlConst.CONCAT, Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CONCAT('Ana', 'Maria')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(SqlConst.CONCAT_WS, Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("CONCAT_WS('-', 'Ana', NULL, 'Maria')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LEN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LEN('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(SqlConst.LEFT, Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LEFT('Ana', 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LOG", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LOG(10, 100)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LOG10", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LOG10(100)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LOWER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LOWER('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("NCHAR", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("NCHAR(65)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var jsonModifyEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("JSON_MODIFY(payload, '$.profile.name', 'Bia')", dialect));
            Assert.Contains("JSON_MODIFY", jsonModifyEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("JSON_MODIFY", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("JSON_MODIFY(payload, '$.profile.name', 'Bia')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("NEWID", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("NEWID()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("PI", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("PI()", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("POWER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("POWER(2, 3)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("RADIANS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("RADIANS(180)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("RAND", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("RAND(7)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(SqlConst.REPLACE, Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("REPLACE('Ban', 'a', '')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(SqlConst.RIGHT, Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("RIGHT('Ana', 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("ROUND", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("ROUND(1.235, 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SIGN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SIGN(-10)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SIN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SIN(1.5707963267948966)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SQUARE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SQUARE(3)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("STR", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("STR(123.45, 6, 1)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.StringEscapeMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("STRING_ESCAPE('a', 'json')", dialect));
            Assert.Contains("STRING_ESCAPE", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("STRING_ESCAPE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("STRING_ESCAPE('a', 'json')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("SUBSTRING", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SUBSTRING('Ana', 1, 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        if (version < SqlServerDialect.DateTimeOffsetFunctionsMinVersion)
        {
            var switchEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("SWITCHOFFSET('2020-02-29T10:11:12+01:00', '+00:00')", dialect));
            Assert.Contains("SWITCHOFFSET", switchEx.Message, StringComparison.OrdinalIgnoreCase);

            var toOffsetEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar("TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00')", dialect));
            Assert.Contains("TODATETIMEOFFSET", toOffsetEx.Message, StringComparison.OrdinalIgnoreCase);
        }
        else
        {
            Assert.Equal("SWITCHOFFSET", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SWITCHOFFSET('2020-02-29T10:11:12+01:00', '+00:00')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
            Assert.Equal("TODATETIMEOFFSET", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TODATETIMEOFFSET('2020-02-29T10:11:12', '+02:00')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        }
        Assert.Equal("TAN", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TAN(0)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TRIM", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("TRIM('  Ana  ')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("UPPER", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("UPPER('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("LTRIM", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("LTRIM('  Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("PARSENAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("PARSENAME('server.database.dbo.Users', 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("PATINDEX", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("PATINDEX('%Bob%', 'Ana Bob')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("QUOTENAME", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("QUOTENAME('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("REPLICATE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("REPLICATE('Na', 2)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("REVERSE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("REVERSE('Ana')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("RTRIM", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("RTRIM('Ana  ')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SOUNDEX", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SOUNDEX('Robert')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SPACE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SPACE(3)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SQRT", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("SQRT(9)", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("STUFF", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("STUFF('Ana', 2, 1, 'xx')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("UNICODE", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar("UNICODE('A')", dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser/tokenizer accepts @@ROWCOUNT through dialect-owned syntax capability.
    /// PT: Garante que o parser/tokenizer SQL Server aceite @@ROWCOUNT pela capability de sintaxe do próprio dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_SystemRowCountIdentifier_ShouldFollowDialectCapability(int version)
    {
        var expr = SqlExpressionParser.ParseScalar("@@ROWCOUNT", new SqlServerDialect(version));
        var identifier = Assert.IsType<IdentifierExpr>(expr);

        Assert.Equal("@@ROWCOUNT", identifier.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL-style ILIKE is rejected in SQL Server dialect.
    /// PT: Garante que ILIKE no estilo PostgreSQL seja rejeitado no dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_Ilike_ShouldBeRejected(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("name ILIKE 'jo%'", new SqlServerDialect(version)));

        Assert.Contains(SqlConst.ILIKE, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL-style sequence function calls are rejected in SQL Server dialect.
    /// PT: Garante que chamadas de sequence no estilo PostgreSQL sejam rejeitadas no dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_PostgreSqlStyleSequenceFunctionCalls_ShouldBeRejected(int version)
    {
        var dialect = new SqlServerDialect(version);

        var nextEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("nextval('sales.seq_orders')", dialect));
        Assert.Contains(SqlConst.NEXTVAL, nextEx.Message, StringComparison.OrdinalIgnoreCase);

        var currEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("currval('sales.seq_orders')", dialect));
        Assert.Contains(SqlConst.CURRVAL, currEx.Message, StringComparison.OrdinalIgnoreCase);

        var setEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("setval('sales.seq_orders', 30, false)", dialect));
        Assert.Contains(SqlConst.SETVAL, setEx.Message, StringComparison.OrdinalIgnoreCase);

        var lastEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("lastval()", dialect));
        Assert.Contains(SqlConst.LASTVAL, lastEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures CREATE SEQUENCE follows SQL Server version support and captures numeric options in the AST.
    /// PT: Garante que CREATE SEQUENCE siga o suporte por versao do SQL Server e capture opcoes numericas na AST.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseCreateSequence_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "CREATE SEQUENCE sales.seq_orders START WITH 10 INCREMENT BY 5";

        if (version < SqlServerDialect.SequenceMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Equal("sales", parsed.Table?.DbName, ignoreCase: true);
        Assert.Equal("seq_orders", parsed.Table?.Name, ignoreCase: true);
        Assert.Equal(10L, parsed.StartValue);
        Assert.Equal(5L, parsed.IncrementBy);
    }

    /// <summary>
    /// EN: Ensures DROP SEQUENCE follows SQL Server version support and preserves the IF EXISTS flag.
    /// PT: Garante que DROP SEQUENCE siga o suporte por versao do SQL Server e preserve a flag IF EXISTS.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDropSequence_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "DROP SEQUENCE IF EXISTS sales.seq_orders";

        if (version < SqlServerDialect.SequenceMinVersion)
        {
            Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            return;
        }

        var parsed = Assert.IsType<SqlDropSequenceQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.True(parsed.IfExists);
        Assert.Equal("sales", parsed.Table?.DbName, ignoreCase: true);
        Assert.Equal("seq_orders", parsed.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses the first pragmatic scalar FUNCTION DDL subset.
    /// PT: Garante que o SQL Server interprete o primeiro subset pragmatico de FUNCTION escalar.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalarFunctionDdlSubset_ShouldParse(int version)
    {
        var create = Assert.IsType<SqlCreateFunctionQuery>(SqlQueryParser.Parse(
            "CREATE FUNCTION fn_users(@baseValue INT, @incrementValue INT) RETURNS INT AS BEGIN RETURN @baseValue + @incrementValue END",
            new SqlServerDialect(version)));

        Assert.Equal("fn_users", create.Table?.Name, ignoreCase: true);
        Assert.Equal("INT", create.ReturnTypeSql, ignoreCase: true);
        Assert.Equal(2, create.Parameters.Count);
        Assert.Equal("@baseValue", create.Parameters[0].Name, ignoreCase: true);
        Assert.Equal("@incrementValue", create.Parameters[1].Name, ignoreCase: true);
        Assert.IsType<BinaryExpr>(create.Body);

        var drop = Assert.IsType<SqlDropFunctionQuery>(SqlQueryParser.Parse(
            "DROP FUNCTION IF EXISTS fn_users",
            new SqlServerDialect(version)));

        Assert.True(drop.IfExists);
        Assert.Equal("fn_users", drop.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects CREATE OR REPLACE FUNCTION outside the supported provider-real subset.
    /// PT: Garante que o SQL Server rejeite CREATE OR REPLACE FUNCTION fora do subset realista suportado pelo provider.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Vers+�o do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseCreateOrReplaceScalarFunctionDdlSubset_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE OR REPLACE FUNCTION fn_users(@baseValue INT, @incrementValue INT) RETURNS INT AS BEGIN RETURN @baseValue + @incrementValue END",
            new SqlServerDialect(version)));
        Assert.Contains("CREATE OR REPLACE FUNCTION", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server accepts DROP INDEX ... ON &lt;table&gt; in the pragmatic shared DDL subset.
    /// PT: Garante que o SQL Server aceite DROP INDEX ... ON &lt;table&gt; no subset DDL pragmático compartilhado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDropIndex_WithOnTableClause_ShouldParse(int version)
    {
        var parsed = Assert.IsType<SqlDropIndexQuery>(SqlQueryParser.Parse(
            "DROP INDEX IX_Users_Name ON dbo.Users",
            new SqlServerDialect(version)));

        Assert.Equal("IX_Users_Name", parsed.IndexName, ignoreCase: true);
        Assert.Equal("dbo", parsed.Table?.DbName, ignoreCase: true);
        Assert.Equal("Users", parsed.Table?.Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects CREATE INDEX when the table reference uses an alias outside the pragmatic shared subset.
    /// PT: Garante que o SQL Server rejeite CREATE INDEX quando a referencia da tabela usa alias fora do subset pragmatico compartilhado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseCreateIndex_WithTableAlias_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE INDEX IX_Users_Name ON dbo.Users u (Name)",
            new SqlServerDialect(version)));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects CREATE INDEX when the table reference is a derived source outside the pragmatic shared subset.
    /// PT: Garante que o SQL Server rejeite CREATE INDEX quando a referencia da tabela e uma fonte derivada fora do subset pragmatico compartilhado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseCreateIndex_WithDerivedTable_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE INDEX IX_Users_Name ON (SELECT * FROM dbo.Users) u (Name)",
            new SqlServerDialect(version)));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects CREATE INDEX with an empty key-column list outside the pragmatic shared subset.
    /// PT: Garante que o SQL Server rejeite CREATE INDEX com lista vazia de colunas-chave fora do subset pragmatico compartilhado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseCreateIndex_WithEmptyKeyColumnList_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "CREATE INDEX IX_Users_Name ON dbo.Users ()",
            new SqlServerDialect(version)));

        Assert.Contains("at least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects DROP INDEX ... ON when the table name is omitted from the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite DROP INDEX ... ON quando o nome da tabela for omitido no subset pragmático.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDropIndex_WithOnWithoutTableName_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "DROP INDEX IX_Users_Name ON ;",
            new SqlServerDialect(version)));

        Assert.Contains("table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects DROP INDEX ... ON when the table reference uses an alias outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite DROP INDEX ... ON quando a referencia da tabela usa alias fora do subset pragmático.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDropIndex_WithOnTableAlias_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "DROP INDEX IX_Users_Name ON dbo.Users u",
            new SqlServerDialect(version)));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects DROP INDEX ... ON when the table reference is a derived source outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite DROP INDEX ... ON quando a referencia da tabela e uma fonte derivada fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDropIndex_WithOnDerivedTable_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "DROP INDEX IX_Users_Name ON (SELECT * FROM dbo.Users) u",
            new SqlServerDialect(version)));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server accepts the pragmatic ALTER TABLE ... ADD subset with ADD syntax and captures column metadata.
    /// PT: Garante que o SQL Server aceite o subset pragmático de ALTER TABLE ... ADD com sintaxe ADD e capture os metadados da coluna.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_ShouldParse(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD age INT NOT NULL DEFAULT 0",
            new SqlServerDialect(version)));

        Assert.Equal("dbo", parsed.Table?.DbName, ignoreCase: true);
        Assert.Equal("Users", parsed.Table?.Name, ignoreCase: true);
        Assert.Equal("age", parsed.ColumnName, ignoreCase: true);
        Assert.Equal(DbType.Int32, parsed.ColumnType);
        Assert.False(parsed.Nullable);
        Assert.Equal("0", parsed.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures SQL Server preserves DECIMAL precision and scale metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o SQL Server preserve os metadados de precisao e escala de DECIMAL no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddDecimalColumn_ShouldPreservePrecisionAndScale(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD amount DECIMAL(10, 4) NOT NULL DEFAULT 0",
            new SqlServerDialect(version)));

        Assert.Equal(DbType.Decimal, parsed.ColumnType);
        Assert.Equal(10, parsed.Size);
        Assert.Equal(4, parsed.DecimalPlaces);
        Assert.False(parsed.Nullable);
        Assert.Equal("0", parsed.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures SQL Server preserves binary column size metadata in the pragmatic ALTER TABLE ... ADD subset.
    /// PT: Garante que o SQL Server preserve o metadado de tamanho de coluna binaria no subset pragmatico de ALTER TABLE ... ADD.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddBinaryColumn_ShouldPreserveSize(int version)
    {
        var parsed = Assert.IsType<SqlAlterTableAddColumnQuery>(SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD payload VARBINARY(16) NULL",
            new SqlServerDialect(version)));

        Assert.Equal(DbType.Binary, parsed.ColumnType);
        Assert.Equal(16, parsed.Size);
        Assert.True(parsed.Nullable);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when the table reference uses an alias outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando a referencia da tabela usa alias fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithTableAlias_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users u ADD age INT",
            new SqlServerDialect(version)));

        Assert.Contains("alias", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when the table reference is a derived source outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando a referencia da tabela e uma fonte derivada fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithDerivedTable_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE (SELECT * FROM dbo.Users) u ADD age INT",
            new SqlServerDialect(version)));

        Assert.Contains("concrete table name", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when NOT NULL is paired with DEFAULT NULL outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando NOT NULL e combinado com DEFAULT NULL fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_NotNullWithDefaultNull_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD status VARCHAR(20) NOT NULL DEFAULT NULL",
            new SqlServerDialect(version)));

        Assert.Contains("default null", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when VARCHAR type arguments are not numeric outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de tipo VARCHAR nao sao numericos fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithInvalidVarcharTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD nickname VARCHAR(foo)",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when DECIMAL scale arguments are not numeric outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de escala de DECIMAL nao sao numericos fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithInvalidDecimalTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD amount DECIMAL(10, foo)",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when VARCHAR type arguments are empty outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de tipo VARCHAR estao vazios fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithEmptyVarcharTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD nickname VARCHAR()",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when DECIMAL type arguments are empty outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de tipo DECIMAL estao vazios fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithEmptyDecimalTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD amount DECIMAL()",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when VARCHAR type arguments contain a trailing empty entry outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de tipo VARCHAR contem uma entrada vazia final fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithTrailingCommaInVarcharTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD nickname VARCHAR(10,)",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server rejects ALTER TABLE ... ADD when DECIMAL type arguments contain a trailing empty entry outside the pragmatic subset.
    /// PT: Garante que o SQL Server rejeite ALTER TABLE ... ADD quando os argumentos de tipo DECIMAL contem uma entrada vazia final fora do subset pragmatico.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseAlterTableAddColumn_WithTrailingCommaInDecimalTypeArguments_ShouldReject(int version)
    {
        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(
            "ALTER TABLE dbo.Users ADD amount DECIMAL(10,)",
            new SqlServerDialect(version)));

        Assert.Contains("type arguments", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

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
            new SqlParameter("@p0", 1),
            new SqlParameter("@p1", 2)
        };

        var parsed = SqlQueryParser.Parse(sql, new SqlServerDialect(version), pars);
        Assert.IsType<SqlSelectQuery>(parsed);
    }


    /// <summary>
    /// EN: Ensures SQL Server OFFSET/FETCH pagination is normalized to the canonical row-limit AST node.
    /// PT: Garante que a paginação OFFSET/FETCH do SQL Server seja normalizada para o nó canônico de AST de limite de linhas.
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
        Assert.Equal(new LiteralExpr(2), rowLimit.Count);
        Assert.Equal(new LiteralExpr(1), rowLimit.Offset);
    }

    /// <summary>
    /// EN: Verifies parsing SELECT with LIMIT returns an actionable hint for SQL Server pagination syntax.
    /// PT: Verifica que o parsing de SELECT com LIMIT retorna uma dica acionável para a sintaxe de paginação do SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_Limit_ShouldProvidePaginationHint(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse("SELECT id FROM users ORDER BY id LIMIT 5", new SqlServerDialect(version)));

        Assert.Contains(SqlConst.LIMIT, ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(SqlConst.OFFSET, ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(SqlConst.FETCH, ex.Message, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Ensures SQL Server rejects Oracle-style JSON_VALUE RETURNING clause with an actionable dialect gate.
    /// PT: Garante que o SQL Server rejeite a cláusula Oracle-style JSON_VALUE RETURNING com gate acionável de dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_JsonValueWithReturningClause_ShouldBeRejected(int version)
    {
        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("JSON_VALUE(payload, '$.a.b' RETURNING NUMBER)", new SqlServerDialect(version)));

        Assert.Contains("JSON_VALUE", ex.Message, StringComparison.OrdinalIgnoreCase);
        if (version >= SqlServerDialect.JsonFunctionsMinVersion)
            Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures JSON_QUERY follows SQL Server version support starting in 2016.
    /// PT: Garante que JSON_QUERY siga o suporte por versão do SQL Server a partir de 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_JsonQuery_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "JSON_QUERY(payload, '$.profile')";
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("JSON_QUERY", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("JSON_QUERY", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server exposes high-precision temporal calls only for versions that support them natively.
    /// PT: Garante que o SQL Server exponha chamadas temporais de alta precisão apenas para versões que as suportam nativamente.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void TemporalCapabilities_ShouldFollowSqlServerVersionSupport(int version)
    {
        var dialect = new SqlServerDialect(version);
        var supported = version >= SqlServerDialect.HighPrecisionTemporalFunctionsMinVersion;

        Assert.True(dialect.SupportsGetUtcDateFunction);
        Assert.Contains("GETUTCDATE", dialect.TemporalFunctionCallNames, StringComparer.OrdinalIgnoreCase);
        Assert.Equal(supported, dialect.TemporalFunctionCallNames.Contains("SYSDATETIME", StringComparer.OrdinalIgnoreCase));
        Assert.Equal(supported, dialect.TemporalFunctionCallNames.Contains("SYSDATETIMEOFFSET", StringComparer.OrdinalIgnoreCase));
        Assert.Equal(supported, dialect.TemporalFunctionCallNames.Contains("SYSUTCDATETIME", StringComparer.OrdinalIgnoreCase));
    }

    /// <summary>
    /// EN: Ensures SQL Server FROMPARTS constructors are gated by the native 2012+ feature boundary.
    /// PT: Garante que os construtores FROMPARTS do SQL Server sejam controlados pelo limite nativo do recurso 2012+.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_FromPartsFunctions_ShouldFollowSqlServerVersionSupport(int version)
    {
        var dialect = new SqlServerDialect(version);
        const string dateSql = "DATEFROMPARTS(2020, 2, 29)";
        const string dateTimeSql = "DATETIMEFROMPARTS(2020, 2, 29, 10, 11, 12)";
        const string dateTime2Sql = "DATETIME2FROMPARTS(2020, 2, 29, 10, 11, 12, 1234567)";
        const string dateTimeOffsetSql = "DATETIMEOFFSETFROMPARTS(2020, 2, 29, 10, 11, 12, 1234567, 60)";
        const string timeSql = "TIMEFROMPARTS(10, 11, 12, 1234567, 7)";
        const string smallDateTimeSql = "SMALLDATETIMEFROMPARTS(2020, 2, 29, 10, 11)";

        if (version < SqlServerDialect.FromPartsMinVersion)
        {
            var dateEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(dateSql, dialect));
            Assert.Contains("DATEFROMPARTS", dateEx.Message, StringComparison.OrdinalIgnoreCase);

            var dateTimeEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(dateTimeSql, dialect));
            Assert.Contains("DATETIMEFROMPARTS", dateTimeEx.Message, StringComparison.OrdinalIgnoreCase);

            var dateTime2Ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(dateTime2Sql, dialect));
            Assert.Contains("DATETIME2FROMPARTS", dateTime2Ex.Message, StringComparison.OrdinalIgnoreCase);

            var dateTimeOffsetEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(dateTimeOffsetSql, dialect));
            Assert.Contains("DATETIMEOFFSETFROMPARTS", dateTimeOffsetEx.Message, StringComparison.OrdinalIgnoreCase);

            var timeEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(timeSql, dialect));
            Assert.Contains("TIMEFROMPARTS", timeEx.Message, StringComparison.OrdinalIgnoreCase);

            var smallDateTimeEx = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(smallDateTimeSql, dialect));
            Assert.Contains("SMALLDATETIMEFROMPARTS", smallDateTimeEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        Assert.Equal("DATEFROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(dateSql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATETIMEFROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(dateTimeSql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATETIME2FROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(dateTime2Sql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("DATETIMEOFFSETFROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(dateTimeOffsetSql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("TIMEFROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(timeSql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("SMALLDATETIMEFROMPARTS", Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(smallDateTimeSql, dialect)).Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server TRY_CAST is gated by the native 2012+ feature boundary and preserves the target type as raw SQL.
    /// PT: Garante que TRY_CAST no SQL Server seja controlado pelo limite nativo do recurso 2012+ e preserve o tipo de destino como SQL bruto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_TryCast_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "TRY_CAST('42' AS INT)";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.TryCastMinVersion, dialect.SupportsTryCastFunction);

        if (version < SqlServerDialect.TryCastMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("TRY_CAST", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("TRY_CAST", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("INT", Assert.IsType<RawSqlExpr>(call.Args[1]).Sql, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures TRY_CONVERT follows SQL Server version support starting in 2012 and keeps the target type as raw SQL.
    /// PT: Garante que TRY_CONVERT siga o suporte por versão do SQL Server a partir de 2012 e mantenha o tipo de destino como SQL bruto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_TryConvert_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "TRY_CONVERT(INT, '42')";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.TryConvertMinVersion, dialect.SupportsTryConvertFunction);

        if (version < SqlServerDialect.TryConvertMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("TRY_CONVERT", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("TRY_CONVERT", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.IsType<RawSqlExpr>(call.Args[1]);
        Assert.Equal("INT", Assert.IsType<RawSqlExpr>(call.Args[1]).Sql, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PARSE follows SQL Server version support starting in 2012 and keeps the target type as raw SQL.
    /// PT: Garante que PARSE siga o suporte por versão do SQL Server a partir de 2012 e mantenha o tipo de destino como SQL bruto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_Parse_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "PARSE('42' AS INT)";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.ParseMinVersion, dialect.SupportsParseFunction);

        if (version < SqlServerDialect.ParseMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("PARSE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("PARSE", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.IsType<RawSqlExpr>(call.Args[1]);
        Assert.Equal("INT", Assert.IsType<RawSqlExpr>(call.Args[1]).Sql, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures TRY_PARSE follows SQL Server version support starting in 2012 and keeps the target type as raw SQL.
    /// PT: Garante que TRY_PARSE siga o suporte por versão do SQL Server a partir de 2012 e mantenha o tipo de destino como SQL bruto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_TryParse_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "TRY_PARSE('42' AS INT)";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.ParseMinVersion, dialect.SupportsTryParseFunction);

        if (version < SqlServerDialect.ParseMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("TRY_PARSE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("TRY_PARSE", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.IsType<RawSqlExpr>(call.Args[1]);
        Assert.Equal("INT", Assert.IsType<RawSqlExpr>(call.Args[1]).Sql, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures EOMONTH follows SQL Server version support starting in 2012.
    /// PT: Garante que EOMONTH siga o suporte por versão do SQL Server a partir de 2012.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_Eomonth_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "EOMONTH('2020-02-15')";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.EomonthMinVersion, dialect.SupportsEomonthFunction);

        if (version < SqlServerDialect.EomonthMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("EOMONTH", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("EOMONTH", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures TRANSLATE follows SQL Server version support starting in 2017.
    /// PT: Garante que TRANSLATE siga o suporte por versão do SQL Server a partir de 2017.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_Translate_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "TRANSLATE('abc', 'ab', 'xy')";
        var dialect = new SqlServerDialect(version);

        Assert.Equal(version >= SqlServerDialect.TranslateMinVersion, dialect.SupportsSqlServerScalarFunction("TRANSLATE"));

        if (version < SqlServerDialect.TranslateMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("TRANSLATE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("TRANSLATE", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures GETUTCDATE remains available as a zero-argument SQL Server temporal call across supported versions.
    /// PT: Garante que GETUTCDATE permaneça disponivel como chamada temporal sem argumentos do SQL Server em todas as versões suportadas.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_GetUtcDate_ShouldRemainAvailableAcrossSqlServerVersions(int version)
    {
        const string sql = "GETUTCDATE()";
        var dialect = new SqlServerDialect(version);

        Assert.True(dialect.SupportsGetUtcDateFunction);

        var call = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("GETUTCDATE", call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures OPENJSON follows SQL Server version support starting in 2016.
    /// PT: Garante que OPENJSON siga o suporte por versão do SQL Server a partir de 2016.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_OpenJson_ShouldFollowSqlServerVersionSupport(int version)
    {
        const string sql = "OPENJSON(payload)";
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar(sql, dialect));

            Assert.Contains(SqlConst.OPENJSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = SqlExpressionParser.ParseScalar(sql, dialect);
        var call = Assert.IsType<CallExpr>(expr);
        Assert.Equal(SqlConst.OPENJSON, call.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses schema-qualified table-valued functions in APPLY once the shared function subset is supported.
    /// PT: Garante que o SQL Server interprete funcoes de tabela qualificadas por schema em APPLY quando o subset compartilhado de funcoes estiver suportado.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_SchemaQualifiedTableFunctionInApply_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, part.value
            FROM Users u
            CROSS APPLY dbo.STRING_SPLIT(u.Email, ',') part
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.STRING_SPLIT, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal("dbo", join.Table.DbName, ignoreCase: true);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal(SqlConst.STRING_SPLIT, function.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses schema-qualified OPENJSON WITH explicit schema through the shared APPLY table-function path.
    /// PT: Garante que o SQL Server interprete OPENJSON qualificado por schema com WITH explicito pelo caminho compartilhado de funcao tabular em APPLY.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_SchemaQualifiedOpenJsonWithSchema_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT data.Name, data.PayloadJson
            FROM Users u
            CROSS APPLY dbo.OPENJSON(u.Email) WITH (
                Name NVARCHAR(20) '$.Name',
                PayloadJson NVARCHAR(MAX) '$.Payload' AS JSON
            ) data
            """;

        if (version < SqlServerDialect.JsonFunctionsMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            Assert.Contains(SqlConst.OPENJSON, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal("dbo", join.Table.DbName, ignoreCase: true);
        var withClause = Assert.IsType<SqlOpenJsonWithClause>(join.Table.OpenJsonWithClause);
        Assert.Equal(2, withClause.Columns.Count);
        Assert.True(withClause.Columns[1].AsJson);
    }

    /// <summary>
    /// EN: Ensures SQL Server parses schema-qualified STRING_SPLIT enable_ordinal only once the dialect reaches SQL Server 2022 semantics.
    /// PT: Garante que o SQL Server interprete STRING_SPLIT qualificado por schema com enable_ordinal apenas quando o dialeto atingir a semantica do SQL Server 2022.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseSelect_SchemaQualifiedStringSplitWithOrdinal_ShouldFollowVersionSupport(int version)
    {
        const string sql = """
            SELECT u.Id, part.value, part.ordinal
            FROM Users u
            CROSS APPLY dbo.STRING_SPLIT(u.Email, ',', 1) part
            """;

        if (version < SqlServerDialect.StringSplitOrdinalMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
            //Assert.Contains("enable_ordinal", ex.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("SQL não suportado para dialeto", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        var join = Assert.Single(parsed.Joins);
        Assert.Equal("dbo", join.Table.DbName, ignoreCase: true);
        var function = Assert.IsType<FunctionCallExpr>(join.Table.TableFunction);
        Assert.Equal(3, function.Args.Count);
    }

    /// <summary>
    /// EN: Ensures SQL Server accepts DATEADD and rejects MySQL DATE_ADD/TIMESTAMPADD syntax.
    /// PT: Garante que o SQL Server aceite DATEADD e rejeite a sintaxe DATE_ADD/TIMESTAMPADD do MySQL.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_DateAddFamily_ShouldRespectSqlServerDialectRule(int version)
    {
        var dialect = new SqlServerDialect(version);

        var expr = SqlExpressionParser.ParseScalar("DATEADD(DAY, 1, created_at)", dialect);
        var call = Assert.IsType<CallExpr>(expr);
        Assert.Equal("DATEADD", call.Name, StringComparer.OrdinalIgnoreCase);

        var dateAddEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("DATE_ADD(created_at, INTERVAL 1 DAY)", dialect));
        Assert.Contains("DATE_ADD", dateAddEx.Message, StringComparison.OrdinalIgnoreCase);

        var timestampAddEx = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("TIMESTAMPADD(DAY, 1, created_at)", dialect));
        Assert.Contains("TIMESTAMPADD", timestampAddEx.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures NEXT/PREVIOUS VALUE FOR follow SQL Server sequence-expression support by version.
    /// PT: Garante que NEXT/PREVIOUS VALUE FOR sigam o suporte a expressoes de sequence por versao no SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_SequenceValueFunctions_ShouldFollowSqlServerVersionSupport(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.SequenceMinVersion)
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

        Assert.Contains(SqlConst.PIVOT, ex.Message, StringComparison.OrdinalIgnoreCase);
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
    /// EN: Ensures SQL Server parser accepts ordered-set WITHIN GROUP for STRING_AGG.
    /// PT: Garante que o parser SQL Server aceite ordered-set WITHIN GROUP para STRING_AGG.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_StringAggWithinGroup_ShouldParse(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect));
            Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|')", dialect));
            return;
        }

        var expr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC)", dialect);
        var call = Assert.IsType<CallExpr>(expr);

        Assert.Equal("STRING_AGG", call.Name, StringComparer.OrdinalIgnoreCase);
        Assert.NotNull(call.WithinGroupOrderBy);
        Assert.Single(call.WithinGroupOrderBy!);
        Assert.True(call.WithinGroupOrderBy![0].Desc);

        var multiExpr = SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC, id ASC)", dialect);
        var multiCall = Assert.IsType<CallExpr>(multiExpr);
        Assert.NotNull(multiCall.WithinGroupOrderBy);
        Assert.Equal(2, multiCall.WithinGroupOrderBy!.Count);
        Assert.True(multiCall.WithinGroupOrderBy[0].Desc);
        Assert.False(multiCall.WithinGroupOrderBy[1].Desc);
    }

    /// <summary>
    /// EN: Ensures SQL Server parser blocks non-native ordered-set aggregate names with WITHIN GROUP.
    /// PT: Garante que o parser SQL Server bloqueie nomes não nativos de agregação ordered-set com WITHIN GROUP.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_ListAggWithinGroup_ShouldThrowNotSupported(int version)
    {
        var dialect = new SqlServerDialect(version);

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
    [MemberDataSqlServerVersion]
    public void ParseScalar_StringAggWithinGroupWithoutOrderBy_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (amount DESC)", dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByTrailingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC,)", dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByEmptyList_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY)", dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByLeadingComma_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY, amount DESC)", dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

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
    [MemberDataSqlServerVersion]
    public void ParseScalar_WithinGroupOrderByMissingCommaBetweenExpressions_ShouldThrowActionableError(int version)
    {
        var dialect = new SqlServerDialect(version);

        if (version < SqlServerDialect.StringAggMinVersion)
        {
            var gateEx = Assert.Throws<NotSupportedException>(() =>
                SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

            Assert.Contains("STRING_AGG", gateEx.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlExpressionParser.ParseScalar("STRING_AGG(amount, '|') WITHIN GROUP (ORDER BY amount DESC id ASC)", dialect));

        Assert.Contains("requires commas", ex.Message, StringComparison.OrdinalIgnoreCase);
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

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server UPDATE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em UPDATE no SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server UPDATE statements even without WHERE.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em UPDATE no SQL Server mesmo sem WHERE.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithReturningWithoutWhere_ShouldBeRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no UPDATE do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server UPDATE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no UPDATE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que UPDATE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures UPDATE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignments without comma separator are rejected with actionable message.
    /// PT: Garante que atribuições em UPDATE SET sem separação por vírgula sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetAssignmentsWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' updated_at = GETDATE() WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("must separate assignments with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment with malformed expression is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET com expressão malformada seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetInvalidAssignmentExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = (GETDATE() WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("assignment for 'name' has an invalid expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET assignment without equals is rejected with actionable message.
    /// PT: Garante que atribuição em UPDATE SET sem sinal de igual seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetAssignmentWithoutEquals_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("requires '=' between column and expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET without assignments and terminated by semicolon is rejected with actionable token context.
    /// PT: Garante que UPDATE SET sem atribuições e finalizado por ponto e vírgula seja rejeitado com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetWithoutAssignmentsBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("requires at least one assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET with repeated SET keyword is rejected with actionable message.
    /// PT: Garante que UPDATE SET com palavra-chave SET repetida seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetRepeatedSetKeyword_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET SET name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("must not repeat SET keyword", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'SET'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET leading comma is rejected with actionable token context.
    /// PT: Garante que vírgula inicial em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET , name = 'b' WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma before assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ','", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE SET trailing comma is rejected with actionable token context.
    /// PT: Garante que vírgula final em UPDATE SET seja rejeitada com contexto acionável de token.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_SetTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b', WHERE id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma without assignment", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found 'WHERE'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures UPDATE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que UPDATE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("UPDATE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PostgreSQL RETURNING clause is rejected for SQL Server DELETE statements.
    /// PT: Garante que a cláusula RETURNING do PostgreSQL seja rejeitada em DELETE no SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithReturning_ShouldBeRejected(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server DELETE remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no DELETE do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with leading comma in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula inicial no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no DELETE continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in DELETE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no DELETE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server DELETE remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no DELETE do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que DELETE com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures DELETE WHERE without predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE sem predicado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereWithoutPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found '<end-of-statement>'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE terminated only by semicolon is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE finalizado apenas por ponto e vírgula seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereOnlySemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("WHERE requires a predicate", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("found ';'", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures DELETE WHERE with malformed predicate is rejected with actionable message.
    /// PT: Garante que DELETE com WHERE malformado seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseDelete_WhereInvalidPredicate_ShouldThrowActionableError(int version)
    {
        const string sql = "DELETE FROM users WHERE (id = 1";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("DELETE WHERE predicate is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed RETURNING in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado em INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures empty RETURNING list in SQL Server INSERT remains blocked by dialect gate.
    /// PT: Garante que lista vazia em RETURNING no INSERT do SQL Server continue bloqueada pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with trailing comma in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com vírgula final no INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no INSERT continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in INSERT remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados no INSERT continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures RETURNING alias without expression in SQL Server INSERT remains blocked by dialect gate.
    /// PT: Garante que RETURNING com alias sem expressão no INSERT do SQL Server continue bloqueado pelo gate de dialeto.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedReturningAliasWithoutExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING AS user_id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON DUPLICATE KEY UPDATE remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnDuplicateKeyUpdate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name),";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateReturningClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with unbalanced parentheses in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com parênteses desbalanceados em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with leading comma in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula inicial em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateLeadingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING, id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with trailing comma in RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com vírgula final em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateTrailingCommaReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and followed by RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e seguido por RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsReturningClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula WHERE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE without assignments and with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE sem atribuições e com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWithoutAssignmentsUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE SET name = VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que atribuição em ON DUPLICATE KEY UPDATE sem sinal de igual continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE leading comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que vírgula inicial em ON DUPLICATE KEY UPDATE continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE , name = VALUES(name)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula WHERE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with table-source clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula de table-source continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON DUPLICATE KEY UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON DUPLICATE KEY UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnDuplicateUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT with unexpected trailing token is rejected with actionable message.
    /// PT: Garante que INSERT com token inesperado ao final seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithUnexpectedTrailingToken_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') EXTRA";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("Unexpected token after INSERT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1),";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuples without comma separator are rejected with actionable message.
    /// PT: Garante que tuplas em INSERT VALUES sem vírgula separadora sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTuplesWithoutCommaSeparator_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES (1) (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("separate row tuples with commas", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id) VALUES , (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES with malformed scalar expression is rejected with actionable message.
    /// PT: Garante que INSERT VALUES com expressão escalar malformada seja rejeitado com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1 +, 'a')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 1 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES reports row/position for malformed expression in later rows.
    /// PT: Garante que INSERT VALUES reporte linha/posição para expressão malformada em linhas posteriores.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesSecondRowInvalidExpression_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a'), (2 +, 'b')";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 2 expression 1 is invalid", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list trailing comma is rejected with actionable message.
    /// PT: Garante que vírgula final na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id,) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("trailing comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT empty column list is rejected with actionable message.
    /// PT: Garante que lista de colunas vazia no INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_EmptyColumnList_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users () VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("at least one column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT column list leading comma is rejected with actionable message.
    /// PT: Garante que vírgula inicial na lista de colunas do INSERT seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListLeadingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (,id) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("unexpected comma", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT column list unclosed before semicolon is rejected with actionable message.
    /// PT: Garante que lista de colunas do INSERT não fechada antes de ponto e vírgula seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ColumnListUnclosedBeforeSemicolon_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id;";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rejects empty expression between commas inside tuple.
    /// PT: Garante que INSERT VALUES rejeite expressão vazia entre vírgulas dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleMissingExpressionBetweenCommas_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,,2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures INSERT VALUES rejects trailing comma inside tuple.
    /// PT: Garante que INSERT VALUES rejeite vírgula final dentro da tupla.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleTrailingComma_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1,)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("empty expression", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES tuple with unclosed parenthesis is rejected with actionable message.
    /// PT: Garante que tupla em INSERT VALUES com parêntese não fechado seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesTupleUnclosedParenthesis_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 2";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("not closed correctly", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES row expression count mismatch is rejected with actionable message.
    /// PT: Garante que divergência entre número de colunas e expressões em INSERT VALUES seja rejeitada com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesColumnCountMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("column count", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures INSERT VALUES rows with inconsistent expression counts are rejected with actionable message.
    /// PT: Garante que linhas de INSERT VALUES com cardinalidade inconsistente de expressões sejam rejeitadas com mensagem acionável.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_ValuesRowArityMismatch_ShouldThrowActionableError(int version)
    {
        const string sql = "INSERT INTO users VALUES (1, 'a'), (2)";

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains("row 2", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("row 1", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures PostgreSQL ON CONFLICT remains rejected by SQL Server dialect gate even when malformed.
    /// PT: Garante que ON CONFLICT do PostgreSQL continue rejeitado pelo gate de dialeto SQL Server mesmo malformado.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnConflictTarget_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (, id) DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target interrupted by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que alvo de ON CONFLICT interrompido por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetUnclosedBeforeSemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures malformed ON CONFLICT DO UPDATE SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithMalformedOnConflictDoUpdateSet_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT without DO branch remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT sem ramo DO continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictMissingDoBranch_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id)";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with additional clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET ... RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET ... RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoNothingUnexpectedContinuation_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON CONFLICT target expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que expressão malformada no alvo de ON CONFLICT continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id +) DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE; DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com WHERE de alvo malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id = DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with valid WHERE and empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com WHERE válido e lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothing_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingUnexpectedContinuationToken_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO NOTHING with additional WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO NOTHING com cláusula WHERE adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereDoNothingWithWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) WHERE id > 0 DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT target WHERE + DO UPDATE WHERE remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT com target WHERE + DO UPDATE WHERE continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictTargetWhereUpdateWhereWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT (id) WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with table-source clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula de table-source continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by FROM remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetFromWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET followed directly by USING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET seguido diretamente por USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetUsingWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed ON CONFLICT DO UPDATE SET assignment expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que expressão de atribuição malformada em ON CONFLICT DO UPDATE SET continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetInvalidAssignmentExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name = (EXCLUDED.name WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT DO UPDATE SET com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT DO UPDATE SET assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que atribuição em ON CONFLICT DO UPDATE SET sem sinal de igual continue rejeitada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictDoUpdateSetAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT (id) DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without name remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutName_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without name at end-of-statement remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem nome no fim do statement continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutNameAtEndOfStatement_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT without DO branch remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT sem ramo DO continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintWithoutDoBranch_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO with invalid continuation remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO com continuação inválida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoInvalidContinuation_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO SKIP";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE without SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE sem SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithoutSet_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET without assignments remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET sem atribuições continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with leading comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula inicial continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetLeadingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET , name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with trailing comma remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com vírgula final continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetTrailingComma_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name,";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignments without comma separator remain rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuições sem separador por vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetAssignmentsWithoutCommaSeparator_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name updated_at = NOW()";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with repeated SET keyword remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com palavra-chave SET repetida continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetRepeatedSetKeyword_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET assignment without equals remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com atribuição sem sinal de igual continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetAssignmentWithoutEquals_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET with malformed assignment expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET com expressão de atribuição malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetInvalidAssignmentExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = (EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by FROM remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetFromWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE SET followed directly by USING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE SET seguido diretamente por USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateSetUsingWithoutAssignments_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothing_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO NOTHING + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO NOTHING + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereDoNothingEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0 DO NOTHING RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with FROM clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula FROM continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithFromClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING FROM users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with USING clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula USING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUsingClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING USING users";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with SET clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithSetClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with UPDATE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula UPDATE continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUpdateClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING UPDATE SET name = 'b'";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with additional WHERE clause remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com cláusula WHERE adicional continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithWhereClause_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING WHERE id = 1";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO NOTHING with unexpected continuation token remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO NOTHING com token de continuação inesperado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoNothingWithUnexpectedContinuationToken_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING EXTRA";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE; DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE de alvo malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO NOTHING";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE malformed before DO UPDATE SET remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com WHERE malformado antes do DO UPDATE SET continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereInvalidPredicateBeforeDoUpdate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey WHERE id = DO UPDATE SET name = EXCLUDED.name";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with malformed expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com expressão malformada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with unbalanced parentheses remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com parênteses desbalanceados continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE + RETURNING with empty projection list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE + RETURNING com lista de projeção vazia continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id
RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT target WHERE + DO UPDATE WHERE remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT com target WHERE + DO UPDATE WHERE continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintTargetWhereUpdateWhereWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = @"INSERT INTO users (id, name)
VALUES (1, 'a')
ON CONFLICT ON CONSTRAINT users_pkey WHERE id > 0
DO UPDATE SET name = EXCLUDED.name
WHERE users.id = EXCLUDED.id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereOnlySemicolon_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE; RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE terminated only by semicolon remains rejected by SQL Server dialect gate even without RETURNING.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE finalizado apenas por ponto e vírgula continue rejeitado pelo gate de dialeto SQL Server mesmo sem RETURNING.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereOnlySemicolonWithoutReturning_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE without predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE sem predicado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereWithoutPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE WHERE with malformed predicate remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE malformado continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereInvalidPredicate_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE id = RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and malformed RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão malformada em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereInvalidReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and unbalanced RETURNING expression remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e expressão RETURNING desbalanceada continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereUnbalancedReturningExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures ON CONFLICT ON CONSTRAINT DO UPDATE with valid WHERE and empty RETURNING list remains rejected by SQL Server dialect gate.
    /// PT: Garante que ON CONFLICT ON CONSTRAINT DO UPDATE com WHERE válido e lista vazia em RETURNING continue rejeitado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseInsert_WithOnConflictOnConstraintDoUpdateWhereEmptyReturningList_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON CONFLICT ON CONSTRAINT users_pkey DO UPDATE SET name = EXCLUDED.name WHERE users.id = EXCLUDED.id RETURNING;";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));
        Assert.Contains("ON CONFLICT", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING with unbalanced parentheses in UPDATE remains blocked by SQL Server dialect gate.
    /// PT: Garante que RETURNING malformado com parênteses desbalanceados em UPDATE continue bloqueado pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningUnbalancedParenthesis_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING (id";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures malformed RETURNING expression in UPDATE remains blocked by SQL Server dialect gate.
    /// PT: Garante que expressão malformada em RETURNING no UPDATE continue bloqueada pelo gate de dialeto SQL Server.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseUpdate_WithMalformedReturningInvalidExpression_ShouldBeRejectedByDialectGate(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id +";

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlQueryParser.Parse(sql, new SqlServerDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }


    /// <summary>
    /// EN: Verifies UPDATE parsing keeps SET subquery text and WHERE boundary intact when FROM contains joins.
    /// PT: Verifica que o parsing de UPDATE mantém o texto da subquery no SET e o limite do WHERE intacto quando o FROM contém joins.
    /// </summary>
    /// <param name="version">EN: SQL Server dialect version under test. PT: Versão do dialeto SQL Server em teste.</param>
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

    /// <summary>
    /// EN: Ensures SQL Server parser rejects MySQL full-text MATCH ... AGAINST syntax.
    /// PT: Garante que o parser SQL Server rejeite sintaxe full-text MATCH ... AGAINST do MySQL.
    /// </summary>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataSqlServerVersion]
    public void ParseScalar_MatchAgainst_ShouldBeRejectedByDialectGate(int version)
    {
        var dialect = new SqlServerDialect(version);

        var ex = Assert.Throws<NotSupportedException>(() =>
            SqlExpressionParser.ParseScalar("MATCH(name) AGAINST ('john' IN BOOLEAN MODE)", dialect));

        Assert.Contains("MATCH ... AGAINST", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

}
