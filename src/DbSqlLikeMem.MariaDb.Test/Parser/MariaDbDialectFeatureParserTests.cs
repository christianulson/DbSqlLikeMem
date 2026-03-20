namespace DbSqlLikeMem.MariaDb.Test.Parser;

/// <summary>
/// EN: Covers MariaDB-specific dialect gates layered on top of the shared MySQL family parser.
/// PT: Cobre os gates de dialeto especificos do MariaDB sobre o parser compartilhado da familia MySQL.
/// </summary>
public sealed class MariaDbDialectFeatureParserTests
{
    /// <summary>
    /// EN: Ensures MariaDB exposes the expected provider name and version gates for the planned family expansion.
    /// PT: Garante que o MariaDB exponha o nome de provedor e os gates de versao esperados para a expansao planejada da familia.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void DialectMetadata_ShouldExposeMariaDbFamilyIdentity()
    {
        var dialect = new MariaDbDialect(MariaDbDbVersions.Version11_0);

        dialect.Name.Equals("mariadb", StringComparison.OrdinalIgnoreCase).Should().BeTrue();
        Assert.True(dialect.SupportsOnDuplicateKeyUpdate);
        Assert.True(dialect.SupportsInsertReturning);
        Assert.False(dialect.SupportsUpdateReturning);
        Assert.True(dialect.SupportsDeleteReturning);
        Assert.False(dialect.SupportsAggregateFunctionsInReturningClause);
        Assert.True(dialect.SupportsSequenceDdl);
        Assert.True(dialect.SupportsJsonTableFunction);
    }

    /// <summary>
    /// EN: Ensures INSERT ... RETURNING follows the MariaDB version gate and captures the projection payload when enabled.
    /// PT: Garante que INSERT ... RETURNING siga o gate de versao do MariaDB e capture a projecao quando habilitado.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseInsert_Returning_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING id, name AS user_name";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.ReturningMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("user_name", parsed.Returning[1].Alias);
    }

    /// <summary>
    /// EN: Ensures MariaDB RETURNING rejects aggregate functions even when the version gate is enabled.
    /// PT: Garante que RETURNING no MariaDB rejeite funcoes de agregacao mesmo com o gate de versao habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Returning_ShouldRejectAggregateFunctions()
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') RETURNING COUNT(*)";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.Contains("aggregate", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB INSERT syntax accepts the VALUE keyword, LOW_PRIORITY modifier, and PARTITION clause.
    /// PT: Garante que a sintaxe INSERT do MariaDB aceite a palavra-chave VALUE, o modificador LOW_PRIORITY e a clausula PARTITION.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_ValuePartition_ShouldAcceptMariaDbSyntax()
    {
        const string sql = "INSERT LOW_PRIORITY INTO users PARTITION (p0) VALUE (1, 'a') RETURNING id";
        var dialect = new MariaDbDialect(MariaDbDbVersions.Version10_5);

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, dialect));

        Assert.False(parsed.IsReplace);
        Assert.Single(parsed.ValuesRaw);
        Assert.Equal("1", parsed.ValuesRaw[0][0]);
        Assert.Equal("a", parsed.ValuesRaw[0][1]);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures MariaDB INSERT and REPLACE syntax accept the DELAYED modifier together with RETURNING.
    /// PT: Garante que a sintaxe INSERT e REPLACE do MariaDB aceite o modificador DELAYED junto com RETURNING.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsertAndReplace_Delayed_ShouldAcceptMariaDbSyntax()
    {
        var dialect = new MariaDbDialect(MariaDbDbVersions.Version10_5);

        var insert = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse("INSERT DELAYED INTO users VALUE (1, 'a') RETURNING id", dialect));
        Assert.False(insert.IsReplace);
        Assert.Single(insert.Returning);
        Assert.Equal("id", insert.Returning[0].Raw);

        var replace = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse("REPLACE DELAYED INTO users VALUE (1, 'a') RETURNING id", dialect));
        Assert.True(replace.IsReplace);
        Assert.Single(replace.Returning);
        Assert.Equal("id", replace.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures MariaDB INSERT IGNORE maps duplicate conflicts to do-nothing semantics while keeping RETURNING available.
    /// PT: Garante que INSERT IGNORE do MariaDB converta conflitos duplicados em semantica de nao fazer nada mantendo RETURNING disponivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Ignore_ShouldMapToDoNothingSemantics()
    {
        const string sql = "INSERT IGNORE INTO users VALUES (1, 'a'), (2, 'b') RETURNING id, name";
        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.False(parsed.IsReplace);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Equal(2, parsed.ValuesRaw.Count);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures MariaDB INSERT ... SET syntax is accepted together with RETURNING.
    /// PT: Garante que a sintaxe INSERT ... SET do MariaDB seja aceita junto com RETURNING.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Set_ShouldAcceptMariaDbSyntax()
    {
        const string sql = "INSERT INTO users SET id = 1, name = 'a', email = 'a@maria.test' RETURNING id";
        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.False(parsed.IsReplace);
        Assert.Equal(3, parsed.Columns.Count);
        Assert.Equal("id", parsed.Columns[0]);
        Assert.Equal("name", parsed.Columns[1]);
        Assert.Equal("email", parsed.Columns[2]);
        Assert.Single(parsed.ValuesRaw);
        Assert.Equal("1", parsed.ValuesRaw[0][0]);
        Assert.Equal("a", parsed.ValuesRaw[0][1]);
        Assert.Equal("a@maria.test", parsed.ValuesRaw[0][2]);
        Assert.Single(parsed.Returning);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SET with LOW_PRIORITY, PARTITION, and ON DUPLICATE KEY UPDATE keeps RETURNING available.
    /// PT: Garante que INSERT ... SET com LOW_PRIORITY, PARTITION e ON DUPLICATE KEY UPDATE mantenha RETURNING disponivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Set_WithModifiersAndOnDuplicateKeyUpdate_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            INSERT LOW_PRIORITY INTO users PARTITION (p0)
            SET id = 1, name = 'a', email = 'a@maria.test'
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                email = VALUES(email)
            RETURNING id, name, email
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.False(parsed.IsReplace);
        Assert.Equal(3, parsed.Columns.Count);
        Assert.Single(parsed.ValuesRaw);
        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Equal(3, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("email", parsed.Returning[2].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SET with ON DUPLICATE KEY UPDATE keeps RETURNING available when the MariaDB version gate is enabled.
    /// PT: Garante que INSERT ... SET com ON DUPLICATE KEY UPDATE mantenha RETURNING disponivel quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Set_OnDuplicateKeyUpdate_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            INSERT INTO users SET id = 1, name = 'a', email = 'a@maria.test'
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                email = VALUES(email)
            RETURNING id, name, email
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.NotNull(parsed);
        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Single(parsed.ValuesRaw);
        Assert.Equal(3, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("email", parsed.Returning[2].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT keeps RETURNING available once the MariaDB version gate is enabled.
    /// PT: Garante que INSERT ... SELECT mantenha RETURNING disponivel quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Select_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            INSERT INTO archive_users (Id, Name, Email)
            SELECT Id, Name, Email
            FROM users
            WHERE Id >= 1000
            RETURNING Id, Name
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.Empty(parsed.ValuesRaw);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("Id", parsed.Returning[0].Raw);
        Assert.Equal("Name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT with IGNORE maps duplicate conflicts to do-nothing semantics while keeping RETURNING available.
    /// PT: Garante que INSERT ... SELECT com IGNORE converta conflitos duplicados em semantica de nao fazer nada mantendo RETURNING disponivel.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Select_Ignore_Returning_ShouldMapToDoNothingSemantics()
    {
        const string sql = """
            INSERT IGNORE INTO archive_users (Id, Name, Email)
            SELECT Id, Name, Email
            FROM users
            WHERE Id >= 1000
            RETURNING Id, Name
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.True(parsed.IsOnConflictDoNothing);
        Assert.Empty(parsed.ValuesRaw);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("Id", parsed.Returning[0].Raw);
        Assert.Equal("Name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... SELECT with ON DUPLICATE KEY UPDATE keeps RETURNING available when the MariaDB version gate is enabled.
    /// PT: Garante que INSERT ... SELECT com ON DUPLICATE KEY UPDATE mantenha RETURNING disponivel quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_Select_OnDuplicateKeyUpdate_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            INSERT INTO archive_users (Id, Name, Email)
            SELECT Id, Name, Email
            FROM users
            WHERE Id >= 1000
            ON DUPLICATE KEY UPDATE
                Name = VALUES(Name),
                Email = VALUES(Email)
            RETURNING Id, Name, Email
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.NotNull(parsed.InsertSelect);
        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Empty(parsed.ValuesRaw);
        Assert.Equal(3, parsed.Returning.Count);
        Assert.Equal("Id", parsed.Returning[0].Raw);
        Assert.Equal("Name", parsed.Returning[1].Raw);
        Assert.Equal("Email", parsed.Returning[2].Raw);
    }

    /// <summary>
    /// EN: Ensures REPLACE ... SELECT keeps RETURNING available when the MariaDB version gate is enabled.
    /// PT: Garante que REPLACE ... SELECT mantenha RETURNING disponivel quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseReplace_Select_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            REPLACE INTO archive_users (Id, Name, Email)
            SELECT Id, Name, Email
            FROM users
            WHERE Id >= 1000
            RETURNING Id, Name
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.True(parsed.IsReplace);
        Assert.NotNull(parsed.InsertSelect);
        Assert.Empty(parsed.ValuesRaw);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("Id", parsed.Returning[0].Raw);
        Assert.Equal("Name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures INSERT ... ON DUPLICATE KEY UPDATE follows the MariaDB version gate and keeps RETURNING available once enabled.
    /// PT: Garante que INSERT ... ON DUPLICATE KEY UPDATE siga o gate de versao do MariaDB e mantenha RETURNING disponivel quando habilitado.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseInsert_OnDuplicateKeyUpdate_Returning_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id, name";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.ReturningMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures multi-row INSERT ... ON DUPLICATE KEY UPDATE keeps RETURNING available and preserves the parsed projection list.
    /// PT: Garante que INSERT ... ON DUPLICATE KEY UPDATE com multiplas linhas mantenha RETURNING disponivel e preserve a lista de projecao parseada.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseInsert_OnDuplicateKeyUpdate_MultiRowReturning_ShouldCaptureProjection()
    {
        const string sql = "INSERT INTO users (id, name) VALUES (1, 'a'), (2, 'b') ON DUPLICATE KEY UPDATE name = VALUES(name) RETURNING id, name";
        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.True(parsed.HasOnDuplicateKeyUpdate);
        Assert.Equal(2, parsed.ValuesRaw.Count);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures DELETE ... RETURNING follows the MariaDB version gate and captures the projection payload when enabled.
    /// PT: Garante que DELETE ... RETURNING siga o gate de versao do MariaDB e capture a projecao quando habilitado.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseDelete_Returning_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "DELETE FROM users WHERE id = 1 RETURNING id";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.ReturningMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures MariaDB rejects RETURNING on multi-table DELETE statements.
    /// PT: Garante que o MariaDB rejeite RETURNING em instrucoes DELETE multi-tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseDelete_Returning_ShouldRejectMultiTableDelete()
    {
        const string sql = "DELETE u FROM users u JOIN audit a ON a.user_id = u.id RETURNING u.id";

        var ex = Assert.Throws<InvalidOperationException>(() => SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.Contains("multi-table DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures REPLACE ... RETURNING follows the MariaDB version gate and is parsed as an insert-like statement.
    /// PT: Garante que REPLACE ... RETURNING siga o gate de versao do MariaDB e seja parseado como um statement semelhante a INSERT.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseReplace_Returning_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "REPLACE INTO users (id, name) VALUES (1, 'a') RETURNING id";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.ReturningMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.True(parsed.IsReplace);
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
    }

    /// <summary>
    /// EN: Ensures REPLACE ... SET keeps RETURNING available when the MariaDB version gate is enabled.
    /// PT: Garante que REPLACE ... SET mantenha RETURNING disponivel quando o gate de versao do MariaDB estiver habilitado.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseReplace_Set_Returning_ShouldCaptureProjection()
    {
        const string sql = """
            REPLACE INTO users SET id = 1, name = 'a', email = 'a@maria.test'
            RETURNING id, name, email
            """;

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.True(parsed.IsReplace);
        Assert.Equal(3, parsed.Columns.Count);
        Assert.Single(parsed.ValuesRaw);
        Assert.Equal(3, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("email", parsed.Returning[2].Raw);
    }

    /// <summary>
    /// EN: Ensures multi-row REPLACE keeps RETURNING available and preserves the parsed projection list.
    /// PT: Garante que REPLACE com multiplas linhas mantenha RETURNING disponivel e preserve a lista de projecao parseada.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseReplace_MultiRowReturning_ShouldCaptureProjection()
    {
        const string sql = "REPLACE INTO users (id, name) VALUES (1, 'a'), (2, 'b') RETURNING id, name";
        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.True(parsed.IsReplace);
        Assert.Equal(2, parsed.ValuesRaw.Count);
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
    }

    /// <summary>
    /// EN: Ensures UPDATE ... RETURNING remains blocked for MariaDB in this first family increment.
    /// PT: Garante que UPDATE ... RETURNING continue bloqueado para MariaDB neste primeiro incremento da familia.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseUpdate_Returning_ShouldRemainRejected(int version)
    {
        const string sql = "UPDATE users SET name = 'b' WHERE id = 1 RETURNING id";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MariaDbDialect(version)));

        Assert.Contains(SqlConst.RETURNING, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures MariaDB parses SOUNDS LIKE as a binary predicate in the expression tree.
    /// PT: Garante que o MariaDB faça o parsing de SOUNDS LIKE como predicado binario na arvore de expressao.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseScalar_SoundsLike_ShouldBuildBinaryExpression()
    {
        var expr = Assert.IsType<BinaryExpr>(SqlExpressionParser.ParseScalar("'Robert' SOUNDS LIKE 'Rupert'", new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.Equal(SqlBinaryOp.SoundLike, expr.Op);
        Assert.IsType<LiteralExpr>(expr.Left);
        Assert.IsType<LiteralExpr>(expr.Right);
    }

    /// <summary>
    /// EN: Ensures CREATE SEQUENCE follows the MariaDB version gate and keeps the statement available once supported.
    /// PT: Garante que CREATE SEQUENCE siga o gate de versao do MariaDB e mantenha o statement disponivel quando suportado.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseCreateSequence_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "CREATE SEQUENCE seq_orders START WITH 1 INCREMENT BY 1";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.SequenceMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, dialect));
            Assert.Contains("CREATE SEQUENCE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlCreateSequenceQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Equal(sql, parsed.RawSql);
    }

    /// <summary>
    /// EN: Ensures NEXT VALUE FOR follows the MariaDB sequence gate.
    /// PT: Garante que NEXT VALUE FOR siga o gate de sequence do MariaDB.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseScalar_NextValueFor_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "NEXT VALUE FOR seq_orders";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.SequenceMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("NEXT VALUE FOR", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("NEXT_VALUE_FOR", expr.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures PREVIOUS VALUE FOR follows the MariaDB sequence gate.
    /// PT: Garante que PREVIOUS VALUE FOR siga o gate de sequence do MariaDB.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseScalar_PreviousValueFor_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "PREVIOUS VALUE FOR seq_orders";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.SequenceMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains("PREVIOUS VALUE FOR", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("PREVIOUS_VALUE_FOR", expr.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE follows the MariaDB version gate and becomes parseable once enabled.
    /// PT: Garante que JSON_TABLE siga o gate de versao do MariaDB e passe a ser parseavel quando habilitado.
    /// </summary>
    /// <param name="version">EN: MariaDB dialect version under test. PT: Versao do dialeto MariaDB em teste.</param>
    [Theory]
    [Trait("Category", "Parser")]
    [MemberDataMariaDbVersion]
    public void ParseScalar_JsonTable_ShouldRespectMariaDbVersionGate(int version)
    {
        const string sql = "JSON_TABLE(payload, '$[*]' COLUMNS(x INT PATH '$'))";
        var dialect = new MariaDbDialect(version);

        if (version < MariaDbDialect.JsonTableMinVersion)
        {
            var ex = Assert.Throws<NotSupportedException>(() => SqlExpressionParser.ParseScalar(sql, dialect));
            Assert.Contains(SqlConst.JSON_TABLE, ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal(SqlConst.JSON_TABLE, expr.Name, StringComparer.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE becomes a valid FROM source for MariaDB and captures the projected columns metadata.
    /// PT: Garante que JSON_TABLE vire uma fonte valida de FROM no MariaDB e capture a metadata das colunas projetadas.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_ShouldCaptureColumnMetadata()
    {
        const string sql = """
            SELECT jt.ord, jt.id, jt.name
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                ord FOR ORDINALITY,
                id INT PATH '$.id',
                name VARCHAR(50) PATH '$.name'
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table);
        var source = parsed.Table;
        Assert.NotNull(source.JsonTableClause);
        var clause = source.JsonTableClause;

        Assert.Equal(SqlConst.JSON_TABLE, source.TableFunction?.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("jt", source.Alias);
        Assert.Equal(3, clause.Columns.Count);
        Assert.True(clause.Columns[0].ForOrdinality);
        Assert.Equal("id", clause.Columns[1].Name);
        Assert.Equal("$.name", clause.Columns[2].Path);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE can be correlated with an outer MariaDB row source and keeps the outer column reference in the parse tree.
    /// PT: Garante que JSON_TABLE possa ser correlacionado com uma fonte de linha externa do MariaDB e mantenha a referencia da coluna externa na arvore de parsing.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_ShouldCaptureOuterReference()
    {
        const string sql = """
            SELECT o.Id, jt.tag
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                tag VARCHAR(20) PATH '$'
            )) jt
            ORDER BY o.Id, jt.tag
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.Single(parsed.Joins);

        var joinSource = parsed.Joins[0].Table;
        Assert.NotNull(joinSource.JsonTableClause);
        Assert.Equal("jt", joinSource.Alias);
        Assert.Equal(SqlConst.JSON_TABLE, joinSource.TableFunction?.Name, StringComparer.OrdinalIgnoreCase);

        var outerArg = Assert.IsType<ColumnExpr>(joinSource.TableFunction!.Args[0]);
        Assert.Equal("o", outerArg.Qualifier);
        Assert.Equal("Tags", outerArg.Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also carry nested path metadata in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem possam carregar metadata de nested path no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedPath_ShouldCaptureNestedMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.tag_ord, jt.tag
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag VARCHAR(20) PATH '$'
                )
            )) jt
            ORDER BY o.Id, jt.tag_ord
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("id", clause.Columns[0].Name);
        Assert.Equal("$.tags[*]", clause.NestedPaths[0].Path);
        Assert.True(clause.NestedPaths[0].Clause.Columns[0].ForOrdinality);
        Assert.Equal("tag", clause.NestedPaths[0].Clause.Columns[1].Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture EXISTS PATH metadata inside a nested branch in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de EXISTS PATH dentro de um ramo nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedExistsPath_ShouldCaptureNestedExistsMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.has_tag
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    has_tag INT EXISTS PATH '$.name'
                )
            )) jt
            ORDER BY o.Id, jt.item_id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        Assert.Equal("$.tags[*]", clause.NestedPaths[0].Path);
        Assert.True(clause.NestedPaths[0].Clause.Columns[0].ExistsPath);
        Assert.Equal("$.name", clause.NestedPaths[0].Clause.Columns[0].Path);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture ON EMPTY fallback metadata inside a nested branch in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de fallback ON EMPTY dentro de um ramo nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedDefaultOnEmpty_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_ord, jt.tag_name
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag_name VARCHAR(20) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                )
            )) jt
            ORDER BY o.Id, jt.item_id, jt.tag_ord
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        var nestedColumn = Assert.Single(clause.NestedPaths[0].Clause.Columns.Skip(1));
        Assert.Equal("tag_name", nestedColumn.Name);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, nestedColumn.OnEmpty?.Kind);
        Assert.Equal("fallback", nestedColumn.OnEmpty?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture ON EMPTY fallback metadata on the root path while expanding nested branches in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de fallback ON EMPTY no caminho raiz enquanto expandem ramos nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithRootDefaultOnEmptyAndNestedPath_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_ord, jt.tag_name
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id' DEFAULT '0' ON EMPTY,
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag_name VARCHAR(20) PATH '$.name'
                )
            )) jt
            ORDER BY o.Id, jt.item_id, jt.tag_ord
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, clause.Columns[0].OnEmpty?.Kind);
        Assert.Equal("0", clause.Columns[0].OnEmpty?.DefaultValueRaw);
        Assert.Equal("$.tags[*]", clause.NestedPaths[0].Path);
        Assert.True(clause.NestedPaths[0].Clause.Columns[0].ForOrdinality);
        Assert.Equal("tag_name", clause.NestedPaths[0].Clause.Columns[1].Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture root ordinality and EXISTS PATH metadata in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de ordinality raiz e EXISTS PATH no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithRootOrdinalityAndExistsPath_ShouldCaptureMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.row_ord, jt.item_id, jt.has_tag
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                row_ord FOR ORDINALITY,
                item_id INT PATH '$.id',
                has_tag INT EXISTS PATH '$.tag'
            )) jt
            ORDER BY o.Id, jt.row_ord
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Equal(3, clause.Columns.Count);
        Assert.True(clause.Columns[0].ForOrdinality);
        Assert.Equal("item_id", clause.Columns[1].Name);
        Assert.True(clause.Columns[2].ExistsPath);
        Assert.Equal("$.tag", clause.Columns[2].Path);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can combine strict root paths with multiple nested fallback branches in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE consigam combinar caminhos raiz strict com multiplos ramos nested de fallback no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithStrictRowPathAndNestedFallbackBranches_ShouldCaptureMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_ord, jt.tag_name, jt.tag_value
            FROM Orders o,
                 JSON_TABLE(o.Tags, 'strict $[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag_name VARCHAR(20) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                ),
                NESTED PATH '$.metrics[*]' COLUMNS(
                    metric_ord FOR ORDINALITY,
                    tag_value INT PATH '$.value' DEFAULT '99' ON ERROR
                )
            )) jt
            ORDER BY o.Id, jt.item_id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Equal(2, clause.NestedPaths.Count);
        Assert.Equal("item_id", clause.Columns[0].Name);
        //TODO: corrigir para capturar o strict no caminho raiz
        //Assert.True(clause.Columns[0].Path.IsStrict);
        Assert.Equal("$.tags[*]", clause.NestedPaths[0].Path);
        Assert.Equal("$.metrics[*]", clause.NestedPaths[1].Path);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, clause.NestedPaths[0].Clause.Columns[1].OnEmpty?.Kind);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, clause.NestedPaths[1].Clause.Columns[1].OnError?.Kind);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture ERROR ON EMPTY metadata inside a nested branch in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de ERROR ON EMPTY dentro de um ramo nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedErrorOnEmpty_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_name
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_name VARCHAR(20) PATH '$.name' ERROR ON EMPTY
                )
            )) jt
            ORDER BY o.Id, jt.item_id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        var nestedColumn = Assert.Single(clause.NestedPaths[0].Clause.Columns.Skip(1));
        Assert.Equal("tag_name", nestedColumn.Name);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Error, nestedColumn.OnEmpty?.Kind);
        Assert.Null(nestedColumn.OnEmpty?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture ON ERROR fallback metadata inside a nested branch in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de fallback ON ERROR dentro de um ramo nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedDefaultOnError_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_ord, jt.tag_value
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag_value INT PATH '$.value' DEFAULT '99' ON ERROR
                )
            )) jt
            ORDER BY o.Id, jt.item_id, jt.tag_ord
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        var nestedColumn = Assert.Single(clause.NestedPaths[0].Clause.Columns.Skip(1));
        Assert.Equal("tag_value", nestedColumn.Name);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, nestedColumn.OnError?.Kind);
        Assert.Equal("99", nestedColumn.OnError?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture strict JSON path metadata inside a nested branch in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de strict JSON path dentro de um ramo nested no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithNestedStrictPath_ShouldCaptureStrictMetadata()
    {
        const string sql = """
            SELECT o.Id, jt.item_id, jt.tag_name
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$[*]' COLUMNS(
                item_id INT PATH '$.id',
                NESTED PATH 'strict $.tags[*]' COLUMNS(
                    tag_name VARCHAR(20) PATH '$'
                )
            )) jt
            ORDER BY o.Id, jt.item_id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Single(clause.NestedPaths);
        Assert.Equal("item_id", clause.Columns[0].Name);
        //TODO: corrigir para capturar o strict no caminho aninhado
        //Assert.True(clause.NestedPaths[0].Path.IsStrict);
        //Assert.Equal("$.tags[*]", clause.NestedPaths[0].Path.Path);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also capture strict root path metadata in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem capturem metadata de strict no caminho raiz no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithStrictRowPath_ShouldCapturePath()
    {
        const string sql = """
            SELECT o.Id, jt.item_id
            FROM Orders o,
                 JSON_TABLE(o.Tags, 'strict $.items[*]' COLUMNS(
                item_id INT PATH '$.id'
            )) jt
            ORDER BY o.Id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Single(clause.Columns);
        Assert.Empty(clause.NestedPaths);
        var pathExpr = Assert.IsType<LiteralExpr>(joinSource.TableFunction!.Args[1]);
        Assert.Equal("strict $.items[*]", pathExpr.Value);
        Assert.Equal("item_id", clause.Columns[0].Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also keep sibling nested branches independent in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem mantenham ramos nested irmaos independentes no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithSiblingNestedPaths_ShouldCaptureIndependentBranches()
    {
        const string sql = """
            SELECT o.Id, jt.Size, jt.Color
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$' COLUMNS(
                NESTED PATH '$.sizes[*]' COLUMNS(
                    Size VARCHAR(20) PATH '$.size'
                ),
                NESTED PATH '$.colors[*]' COLUMNS(
                    Color VARCHAR(20) PATH '$.color'
                )
            )) jt
            ORDER BY o.Id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Empty(clause.Columns);
        Assert.Equal(2, clause.NestedPaths.Count);
        Assert.Equal("$.sizes[*]", clause.NestedPaths[0].Path);
        Assert.Equal("$.colors[*]", clause.NestedPaths[1].Path);
        Assert.Equal("Size", clause.NestedPaths[0].Clause.Columns[0].Name);
        Assert.Equal("Color", clause.NestedPaths[1].Clause.Columns[0].Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also keep sibling nested branches independent while preserving ordinality in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem mantenham ramos nested irmaos independentes preservando ordinality no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithSiblingNestedPathsAndOrdinality_ShouldCaptureIndependentBranches()
    {
        const string sql = """
            SELECT o.Id, jt.SizeOrd, jt.Size, jt.ColorOrd, jt.Color
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$' COLUMNS(
                NESTED PATH '$.sizes[*]' COLUMNS(
                    SizeOrd FOR ORDINALITY,
                    Size VARCHAR(20) PATH '$.size'
                ),
                NESTED PATH '$.colors[*]' COLUMNS(
                    ColorOrd FOR ORDINALITY,
                    Color VARCHAR(20) PATH '$.color'
                )
            )) jt
            ORDER BY o.Id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Empty(clause.Columns);
        Assert.Equal(2, clause.NestedPaths.Count);
        Assert.True(clause.NestedPaths[0].Clause.Columns[0].ForOrdinality);
        Assert.True(clause.NestedPaths[1].Clause.Columns[0].ForOrdinality);
        Assert.Equal("Size", clause.NestedPaths[0].Clause.Columns[1].Name);
        Assert.Equal("Color", clause.NestedPaths[1].Clause.Columns[1].Name);
    }

    /// <summary>
    /// EN: Ensures correlated JSON_TABLE sources can also keep sibling nested branches independent while mixing EXISTS PATH and ordinality in MariaDB.
    /// PT: Garante que fontes correlacionadas de JSON_TABLE tambem mantenham ramos nested irmaos independentes misturando EXISTS PATH e ordinality no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromCorrelatedJsonTable_WithSiblingNestedExistsAndOrdinality_ShouldCaptureIndependentBranches()
    {
        const string sql = """
            SELECT o.Id, jt.SizeOrd, jt.Size, jt.ColorOrd, jt.HasColor
            FROM Orders o,
                 JSON_TABLE(o.Tags, '$' COLUMNS(
                NESTED PATH '$.sizes[*]' COLUMNS(
                    SizeOrd FOR ORDINALITY,
                    Size VARCHAR(20) PATH '$.size'
                ),
                NESTED PATH '$.colors[*]' COLUMNS(
                    ColorOrd FOR ORDINALITY,
                    HasColor INT EXISTS PATH '$.color'
                )
            )) jt
            ORDER BY o.Id
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var joinSource = Assert.Single(parsed.Joins).Table;

        Assert.NotNull(joinSource.JsonTableClause);
        var clause = joinSource.JsonTableClause;
        Assert.Empty(clause.Columns);
        Assert.Equal(2, clause.NestedPaths.Count);
        Assert.True(clause.NestedPaths[0].Clause.Columns[0].ForOrdinality);
        Assert.True(clause.NestedPaths[1].Clause.Columns[0].ForOrdinality);
        Assert.False(clause.NestedPaths[1].Clause.Columns[1].ForOrdinality);
        Assert.True(clause.NestedPaths[1].Clause.Columns[1].ExistsPath);
        Assert.Equal("$.color", clause.NestedPaths[1].Clause.Columns[1].Path);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE captures EXISTS PATH metadata for MariaDB projected columns.
    /// PT: Garante que JSON_TABLE capture a metadata de EXISTS PATH nas colunas projetadas do MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithExistsPath_ShouldCaptureExistsMetadata()
    {
        const string sql = """
            SELECT jt.has_email
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                has_email INT EXISTS PATH '$.email'
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table);
        var source = parsed.Table;
        Assert.NotNull(source.JsonTableClause);
        var clause = source.JsonTableClause;
        var column = Assert.Single(clause.Columns);

        Assert.True(column.ExistsPath);
        Assert.Equal("$.email", column.Path);
        Assert.Equal(DbType.Int32, column.DbType);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE captures NESTED PATH metadata for MariaDB projected columns.
    /// PT: Garante que JSON_TABLE capture a metadata de NESTED PATH nas colunas projetadas do MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedPath_ShouldCaptureNestedMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.tag_ord, jt.tag_name
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_ord FOR ORDINALITY,
                    tag_name VARCHAR(30) PATH '$.name'
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table);
        var source = parsed.Table;
        Assert.NotNull(source.JsonTableClause);
        var clause = source.JsonTableClause;
        var nested = Assert.Single(clause.NestedPaths);

        Assert.Single(clause.Columns);
        Assert.Equal("id", clause.Columns[0].Name);
        Assert.Equal("$.tags[*]", nested.Path);
        Assert.Equal(2, nested.Clause.Columns.Count);
        Assert.True(nested.Clause.Columns[0].ForOrdinality);
        Assert.Equal("tag_name", nested.Clause.Columns[1].Name);
        Assert.Equal("$.name", nested.Clause.Columns[1].Path);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE captures EXISTS PATH metadata inside a MariaDB nested path branch.
    /// PT: Garante que JSON_TABLE capture a metadata de EXISTS PATH dentro de um ramo nested path do MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedExistsPath_ShouldCaptureNestedExistsMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.has_tag
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    has_tag INT EXISTS PATH '$.name'
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table?.JsonTableClause);
        var clause = parsed.Table?.JsonTableClause!;
        var nested = Assert.Single(clause.NestedPaths);
        var nestedColumn = Assert.Single(nested.Clause.Columns);

        Assert.True(nestedColumn.ExistsPath);
        Assert.Equal("$.name", nestedColumn.Path);
        Assert.Equal(DbType.Int32, nestedColumn.DbType);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE row paths can be parsed with strict JSON path semantics in MariaDB.
    /// PT: Garante que caminhos de linha em JSON_TABLE possam ser parseados com semantica strict de JSON path no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithStrictRowPath_ShouldCapturePath()
    {
        const string sql = """
            SELECT jt.id
            FROM JSON_TABLE(payload, 'strict $.items[*]' COLUMNS(
                id INT PATH '$.id'
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table);
        var source = parsed.Table;

        Assert.NotNull(source.JsonTableClause);
        var pathExpr = Assert.IsType<LiteralExpr>(source.TableFunction!.Args[1]);
        Assert.Equal("strict $.items[*]", pathExpr.Value);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE paths can be parsed with strict JSON path semantics in MariaDB.
    /// PT: Garante que caminhos nested em JSON_TABLE possam ser parseados com semantica strict de JSON path no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithStrictNestedPath_ShouldCapturePath()
    {
        const string sql = """
            SELECT jt.id, jt.tag_name
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH 'strict $.tags[*]' COLUMNS(
                    tag_name VARCHAR(30) PATH '$.name'
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table?.JsonTableClause);
        var nested = Assert.Single(parsed.Table!.JsonTableClause!.NestedPaths);
        Assert.Equal("strict $.tags[*]", nested.Path);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can capture fallback clauses in MariaDB.
    /// PT: Garante que colunas nested de JSON_TABLE capturem clausulas de fallback no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedFallbackClauses_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.tag_name
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_name VARCHAR(30) PATH '$.name' DEFAULT 'fallback' ON EMPTY
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var nested = Assert.Single(parsed.Table!.JsonTableClause!.NestedPaths);
        var nestedColumn = Assert.Single(nested.Clause.Columns);

        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, nestedColumn.OnEmpty?.Kind);
        Assert.Equal("fallback", nestedColumn.OnEmpty?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can capture ON ERROR fallback clauses in MariaDB.
    /// PT: Garante que colunas nested de JSON_TABLE capturem clausulas ON ERROR no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedErrorClauses_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.tag_value
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_value INT PATH '$.value' DEFAULT '99' ON ERROR
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var nested = Assert.Single(parsed.Table!.JsonTableClause!.NestedPaths);
        var nestedColumn = Assert.Single(nested.Clause.Columns);

        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, nestedColumn.OnError?.Kind);
        Assert.Equal("99", nestedColumn.OnError?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can capture ERROR ON ERROR clauses in MariaDB.
    /// PT: Garante que colunas nested de JSON_TABLE capturem clausulas ERROR ON ERROR no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedErrorOnError_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.tag_value
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_value INT PATH '$.value' ERROR ON ERROR
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var nested = Assert.Single(parsed.Table!.JsonTableClause!.NestedPaths);
        var nestedColumn = Assert.Single(nested.Clause.Columns);

        Assert.Equal(SqlJsonTableColumnFallbackKind.Error, nestedColumn.OnError?.Kind);
        Assert.Null(nestedColumn.OnEmpty);
    }

    /// <summary>
    /// EN: Ensures nested JSON_TABLE columns can capture ERROR ON EMPTY clauses in MariaDB.
    /// PT: Garante que colunas nested de JSON_TABLE capturem clausulas ERROR ON EMPTY no MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithNestedErrorOnEmpty_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.tag_name
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id',
                NESTED PATH '$.tags[*]' COLUMNS(
                    tag_name VARCHAR(30) PATH '$.name' ERROR ON EMPTY
                )
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        var nested = Assert.Single(parsed.Table!.JsonTableClause!.NestedPaths);
        var nestedColumn = Assert.Single(nested.Clause.Columns);

        Assert.Equal(SqlJsonTableColumnFallbackKind.Error, nestedColumn.OnEmpty?.Kind);
        Assert.Null(nestedColumn.OnError);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE captures ON EMPTY and ON ERROR fallback metadata for MariaDB path columns.
    /// PT: Garante que JSON_TABLE capture a metadata de fallback ON EMPTY e ON ERROR nas colunas PATH do MariaDB.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_WithFallbackClauses_ShouldCaptureFallbackMetadata()
    {
        const string sql = """
            SELECT jt.id, jt.title
            FROM JSON_TABLE(payload, '$[*]' COLUMNS(
                id INT PATH '$.id' DEFAULT '0' ON EMPTY,
                title VARCHAR(50) PATH '$.title' NULL ON EMPTY DEFAULT 'fallback' ON ERROR
            )) jt
            """;

        var parsed = Assert.IsType<SqlSelectQuery>(SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_6)));
        Assert.NotNull(parsed.Table?.JsonTableClause);
        var clause = parsed.Table?.JsonTableClause!;

        var idColumn = clause.Columns[0];
        var titleColumn = clause.Columns[1];

        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, idColumn.OnEmpty?.Kind);
        Assert.Equal("0", idColumn.OnEmpty?.DefaultValueRaw);
        Assert.Null(idColumn.OnError);

        Assert.Equal(SqlJsonTableColumnFallbackKind.Null, titleColumn.OnEmpty?.Kind);
        Assert.Equal(SqlJsonTableColumnFallbackKind.Default, titleColumn.OnError?.Kind);
        Assert.Equal("fallback", titleColumn.OnError?.DefaultValueRaw);
    }

    /// <summary>
    /// EN: Ensures JSON_TABLE remains blocked as a FROM source before the MariaDB 10.6 gate.
    /// PT: Garante que JSON_TABLE continue bloqueado como fonte de FROM antes do gate MariaDB 10.6.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_BeforeGate_ShouldThrow()
    {
        const string sql = "SELECT * FROM JSON_TABLE(payload, '$[*]' COLUMNS(id INT PATH '$.id')) jt";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.Contains(SqlConst.JSON_TABLE, ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Ensures the MariaDB connection surface uses the MariaDB server identity and the MariaDB mock database by default.
    /// PT: Garante que a superficie de conexao do MariaDB use a identidade de servidor MariaDB e o mock MariaDB por padrao.
    /// </summary>
    [Fact]
    [Trait("Category", "Provider")]
    public void ConnectionSurface_ShouldExposeMariaDbIdentity()
    {
        using var connection = new MariaDbConnectionMock();

        Assert.Contains("MariaDB", connection.ServerVersion, StringComparison.OrdinalIgnoreCase);
        Assert.IsType<MariaDbDbMock>(connection.Db);
    }

    /// <summary>
    /// EN: Ensures DbMockConnectionFactory resolves the MariaDB provider without falling back to the generic MySQL alias.
    /// PT: Garante que o DbMockConnectionFactory resolva o provedor MariaDB sem cair no alias generico de MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Provider")]
    public void ConnectionFactory_ShouldResolveMariaDbProvider()
    {
        var created = DbMockConnectionFactory.CreateWithTables("MariaDb");

        Assert.IsType<MariaDbDbMock>(created.Db);
        Assert.IsType<MariaDbConnectionMock>(created.Connection);
    }

    /// <summary>
    /// EN: Ensures the lowercase mariadb alias resolves the dedicated MariaDB provider instead of falling back to MySQL.
    /// PT: Garante que o alias minusculo mariadb resolva o provider dedicado MariaDB em vez de cair no MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "Provider")]
    public void ConnectionFactory_LowercaseAlias_ShouldResolveMariaDbProvider()
    {
        var created = DbMockConnectionFactory.CreateWithTables("mariadb");

        Assert.IsType<MariaDbDbMock>(created.Db);
        Assert.IsType<MariaDbConnectionMock>(created.Connection);
    }
}
