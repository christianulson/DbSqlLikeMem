namespace DbSqlLikeMem.MySql;

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
        Assert.True(dialect.SupportsInsertReturning);
        Assert.False(dialect.SupportsUpdateReturning);
        Assert.True(dialect.SupportsDeleteReturning);
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
            Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlInsertQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Equal(2, parsed.Returning.Count);
        Assert.Equal("id", parsed.Returning[0].Raw);
        Assert.Equal("name", parsed.Returning[1].Raw);
        Assert.Equal("user_name", parsed.Returning[1].Alias);
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
            Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var parsed = Assert.IsType<SqlDeleteQuery>(SqlQueryParser.Parse(sql, dialect));
        Assert.Single(parsed.Returning);
        Assert.Equal("id", parsed.Returning[0].Raw);
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

        Assert.Contains("RETURNING", ex.Message, StringComparison.OrdinalIgnoreCase);
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
            Assert.Contains("JSON_TABLE", ex.Message, StringComparison.OrdinalIgnoreCase);
            return;
        }

        var expr = Assert.IsType<CallExpr>(SqlExpressionParser.ParseScalar(sql, dialect));
        Assert.Equal("JSON_TABLE", expr.Name, StringComparer.OrdinalIgnoreCase);
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

        Assert.Equal("JSON_TABLE", source.TableFunction?.Name, StringComparer.OrdinalIgnoreCase);
        Assert.Equal("jt", source.Alias);
        Assert.Equal(3, clause.Columns.Count);
        Assert.True(clause.Columns[0].ForOrdinality);
        Assert.Equal("id", clause.Columns[1].Name);
        Assert.Equal("$.name", clause.Columns[2].Path);
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
    /// EN: Ensures JSON_TABLE remains blocked as a FROM source before the MariaDB 10.6 gate.
    /// PT: Garante que JSON_TABLE continue bloqueado como fonte de FROM antes do gate MariaDB 10.6.
    /// </summary>
    [Fact]
    [Trait("Category", "Parser")]
    public void ParseSelect_FromJsonTable_BeforeGate_ShouldThrow()
    {
        const string sql = "SELECT * FROM JSON_TABLE(payload, '$[*]' COLUMNS(id INT PATH '$.id')) jt";

        var ex = Assert.Throws<NotSupportedException>(() => SqlQueryParser.Parse(sql, new MariaDbDialect(MariaDbDbVersions.Version10_5)));

        Assert.Contains("JSON_TABLE", ex.Message, StringComparison.OrdinalIgnoreCase);
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
