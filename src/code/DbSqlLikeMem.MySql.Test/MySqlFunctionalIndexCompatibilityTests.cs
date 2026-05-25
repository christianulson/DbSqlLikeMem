namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Compatibility tests for MySQL functional (expression-based) indexes introduced in 8.0.13.
/// PT-br: Testes de compatibilidade para indices funcionais (baseados em expressao) do MySQL
///        introduzidos na versao 8.0.13.
/// </summary>
/// <param name="helper">
/// EN: Output helper used by the test base.
/// PT-br: Helper de saida usado pela base de testes.
/// </param>
public sealed class MySqlFunctionalIndexCompatibilityTests(
    ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Functional index via GetGenValue: simulates a functional index by marking a column
    ///     as non-persisted computed (PersistComputedValue = false). The index infrastructure
    ///     already evaluates GetGenValue at index-build time via IndexDef.GetColVal.
    /// PT-br: Indice funcional via GetGenValue: simula um indice funcional marcando uma coluna
    ///        como computada nao persistida (PersistComputedValue = false). A infraestrutura de
    ///        indices ja avalia GetGenValue no momento da construcao do indice via IndexDef.GetColVal.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void DirectApi_FunctionalIndexViaGetGenValue_ShouldLookup()
    {
        var db = new MySqlDbMock();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("first_name", DbType.String, false);
        tb.AddColumn("last_name", DbType.String, false);

        // Simulates: INDEX idx_full_name ((CONCAT(first_name, ' ', last_name)))
        var fullNameCol = tb.AddColumn("full_name", DbType.String, false);
        fullNameCol.GetGenValue = (row, _) => $"{row[1]} {row[2]}";
        fullNameCol.PersistComputedValue = false; // non-persisted = functional index semantics

        var idxDef = tb.CreateIndex("ix_full_name", ["full_name"]);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = "Doe" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Jane", [2] = "Smith" });
        tb.Add(new Dictionary<int, object?> { [0] = 3, [1] = "John", [2] = "Smith" });

        var ix = tb.Lookup(idxDef, new IndexKey("John Doe"));
        ix.Should().NotBeNull();
        ix.Should().ContainKey(0);

        ix = tb.Lookup(idxDef, new IndexKey("Jane Smith"));
        ix.Should().ContainKey(1);

        ix = tb.Lookup(idxDef, new IndexKey("John Smith"));
        ix.Should().ContainKey(2);
    }

    /// <summary>
    /// EN: Unique constraint on a functional index via GetGenValue rejects duplicate computed values.
    /// PT-br: A restricao unique em um indice funcional via GetGenValue rejeita valores computados duplicados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void DirectApi_UniqueFunctionalIndex_ShouldRejectDuplicates()
    {
        var db = new MySqlDbMock();
        var tb = db.AddTable("users");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("email", DbType.String, false);

        var domainCol = tb.AddColumn("email_domain", DbType.String, false);
        domainCol.GetGenValue = (row, _) => ((string)row[1]!).Split('@')[1];
        domainCol.PersistComputedValue = false;

        tb.CreateIndex("ix_email_domain", ["email_domain"], unique: true);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "a@gmail.com" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "b@outlook.com" });

        Action act = () => tb.Add(new Dictionary<int, object?> { [0] = 3, [1] = "c@gmail.com" });
        act.Should().Throw<MySqlMockException>()
            .Which.Message.Should().Contain("duplicada");
    }

    /// <summary>
    /// EN: Insert followed by lookup through a functional index returns the correct row when
    ///     the computed column does not persist its value.
    /// PT-br: Insert seguido de consulta por indice funcional retorna a linha correta quando
    ///        a coluna computada nao persiste seu valor.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void DirectApi_InsertThenLookup_ShouldFindRow()
    {
        var db = new MySqlDbMock();
        var tb = db.AddTable("documents");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("title", DbType.String, false);

        var upperCol = tb.AddColumn("title_upper", DbType.String, false);
        upperCol.GetGenValue = (row, _) => ((string)row[1]!).ToUpperInvariant();
        upperCol.PersistComputedValue = false;

        var idxDef = tb.CreateIndex("ix_title_upper", ["title_upper"]);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Hello World" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Foo Bar" });

        var ix = tb.Lookup(idxDef, new IndexKey("HELLO WORLD"));
        ix.Should().ContainKey(0);

        ix = tb.Lookup(idxDef, new IndexKey("FOO BAR"));
        ix.Should().ContainKey(1);
    }

    /// <summary>
    /// EN: SQL parser now handles functional index expressions with ((expression)) syntax.
    /// PT-br: O parser SQL agora trata expressoes de indice funcional com sintaxe ((expressao)).
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Parser_CreateFunctionalIndex_WithUpper_ShouldCreateIndex()
    {
        using var connection = CreateOpenConnection();
        CreateTableWithDocuments(connection);

        using var cmd = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_upper_title ON documents " +
                "((UPPER(title)))"
        };
        cmd.ExecuteNonQuery();

        var table = connection.Db.GetTable("documents");
        table.Indexes.Should().ContainKey("idx_upper_title");
    }

    /// <summary>
    /// EN: Functional index created via SQL parse UPPER(name), insert, and lookup returns the
    ///     computed value.
    /// PT-br: Indice funcional criado via SQL parseia UPPER(name), insere e consulta retorna o
    ///        valor computado.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Parser_CreateFunctionalIndex_WithUpper_InsertAndLookup()
    {
        using var connection = CreateOpenConnection();
        CreateTableWithDocuments(connection);

        using var cmd = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_upper_title ON documents ((UPPER(title)))"
        };
        cmd.ExecuteNonQuery();

        using var insert = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO documents (id, title) VALUES (1, 'Hello World')"
        };
        insert.ExecuteNonQuery();

        var table = connection.Db.GetTable("documents");
        var idxDef = table.Indexes["idx_upper_title"];

        var ix = table.Lookup(idxDef, new IndexKey("HELLO WORLD"));
        ix.Should().NotBeNull();
        ix.Should().ContainKey(0);
    }

    /// <summary>
    /// EN: Functional index with CONCAT expression via SQL parser evaluates correctly.
    /// PT-br: Indice funcional com expressao CONCAT via parser SQL avalia corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Parser_CreateFunctionalIndex_WithConcat_ShouldWork()
    {
        using var connection = CreateOpenConnectionWithConcatTable();

        using var cmd = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_full_name ON users " +
                "((CONCAT(first_name, ' ', last_name)))"
        };
        cmd.ExecuteNonQuery();

        using var insert = new MySqlCommandMock(connection)
        {
            CommandText = "INSERT INTO users (id, first_name, last_name) VALUES (1, 'John', 'Doe')"
        };
        insert.ExecuteNonQuery();

        var table = connection.Db.GetTable("users");
        var idxDef = table.Indexes["idx_full_name"];

        var ix = table.Lookup(idxDef, new IndexKey("John Doe"));
        ix.Should().NotBeNull();
        ix.Should().ContainKey(0);
    }

    /// <summary>
    /// EN: Full end-to-end functional index on JSON expression is parsed and creates the index.
    ///     The JSON path expression CAST(entities->'$[*].name' AS CHAR(255) ARRAY) is MySQL-specific.
    /// PT-br: Indice funcional completo em expressao JSON e parseado e cria o indice.
    ///        A expressao JSON path CAST(entities->'$[*].name' AS CHAR(255) ARRAY) e especifica do MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void EndToEnd_CreateFunctionalIndex_OnJsonExpression_ShouldCreateIndex()
    {
        using var connection = CreateOpenConnection();

        using var createTable = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE TABLE test_entities (" +
                "id BINARY(16) PRIMARY KEY, " +
                "entities JSON, " +
                "content JSON NOT NULL, " +
                "timestamp_utc DATETIME(6) NOT NULL" +
                ")"
        };
        createTable.ExecuteNonQuery();

        using var createIndex = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_entities ON test_entities " +
                "((CAST(entities->'$[*].name' AS CHAR(255) ARRAY)))"
        };

        createIndex.ExecuteNonQuery();
        var table = connection.Db.GetTable("test_entities");
        table.Indexes.Should().ContainKey("idx_entities");
    }

    /// <summary>
    /// EN: Functional index with json_extract() and _utf8mb4 charset introducer is parsed and
    ///     creates the index. This matches MySQL's SHOW CREATE TABLE output exactly.
    /// PT-br: Indice funcional com json_extract() e introducer _utf8mb4 e parseado e cria o
    ///        indice. Corresponde exatamente a saida de SHOW CREATE TABLE do MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Parser_CreateFunctionalIndex_WithJsonExtractAndUtf8mb4_ShouldCreateIndex()
    {
        using var connection = CreateOpenConnection();

        using var createTable = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE TABLE test_entities (" +
                "id BINARY(16) PRIMARY KEY, " +
                "entities JSON, " +
                "content JSON NOT NULL" +
                ")"
        };
        createTable.ExecuteNonQuery();

        using var createIndex = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_entities ON test_entities " +
                "((CAST(json_extract(entities, _utf8mb4'$[*].name') AS CHAR(255) ARRAY)))"
        };

        createIndex.ExecuteNonQuery();
        var table = connection.Db.GetTable("test_entities");
        table.Indexes.Should().ContainKey("idx_entities");
    }

    /// <summary>
    /// EN: Functional index with json_extract() and _utf8mb4 introducer evaluates correctly
    ///     on insert and lookup.
    /// PT-br: Indice funcional com json_extract() e introducer _utf8mb4 avalia corretamente
    ///        em insercao e consulta.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Parser_CreateFunctionalIndex_WithJsonExtractAndUtf8mb4_InsertAndLookup()
    {
        using var connection = CreateOpenConnection();

        using var createTable = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE TABLE test_entities (" +
                "id INT PRIMARY KEY, " +
                "entities JSON" +
                ")"
        };
        createTable.ExecuteNonQuery();

        using var createIndex = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE INDEX idx_entities ON test_entities " +
                "((CAST(json_extract(entities, _utf8mb4'$.name') AS CHAR(255))))"
        };
        createIndex.ExecuteNonQuery();

        using var insert = new MySqlCommandMock(connection)
        {
            CommandText =
                "INSERT INTO test_entities (id, entities) VALUES " +
                "(1, '{\"name\": \"Alice\"}')"
        };
        insert.ExecuteNonQuery();

        var table = connection.Db.GetTable("test_entities");
        var idxDef = table.Indexes["idx_entities"];
        var ix = table.Lookup(idxDef, new IndexKey("\"Alice\""));
        ix.Should().NotBeNull();
        ix.Should().ContainKey(0);

        // Second insert with different value
        using var insert2 = new MySqlCommandMock(connection)
        {
            CommandText =
                "INSERT INTO test_entities (id, entities) VALUES " +
                "(2, '{\"name\": \"Bob\"}')"
        };
        insert2.ExecuteNonQuery();

        ix = table.Lookup(idxDef, new IndexKey("\"Bob\""));
        ix.Should().ContainKey(1);
    }

    /// <summary>
    /// EN: AddFunctionalIndexColumn extension method sets up a functional index column
    ///     via the public API (used by ConsoleGenerator output).
    /// PT-br: O metodo de extensao AddFunctionalIndexColumn configura uma coluna de indice
    ///        funcional via API publica (usado pela saida do ConsoleGenerator).
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void DirectApi_AddFunctionalIndexColumn_ShouldEvaluateExpression()
    {
        var db = new MySqlDbMock();
        var tb = db.AddTable("items");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("data", DbType.String, false);

        var funcCol = tb.AddFunctionalIndexColumn(
            "__func_idx_0__",
            "UPPER(data)",
            db);

        funcCol.Should().NotBeNull();
        funcCol.GetGenValue.Should().NotBeNull("GetGenValue must be set");
        funcCol.PersistComputedValue.Should().BeFalse();

        var testRow = new Dictionary<int, object?> { [0] = 1, [1] = "hello" };
        var result = funcCol.GetGenValue!(testRow, tb);
        result.Should().NotBeNull("GetGenValue should evaluate UPPER(data)");
        result.Should().Be("HELLO");

        var idxDef = tb.CreateIndex("ix_upper_data", ["__func_idx_0__"]);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = "hello" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = "world" });

        var ix = tb.Lookup(idxDef, new IndexKey("HELLO"));
        ix.Should().NotBeNull("lookup for HELLO should find row 0");
        ix.Should().ContainKey(0);

        ix = tb.Lookup(idxDef, new IndexKey("WORLD"));
        ix.Should().NotBeNull("lookup for WORLD should find row 1");
        ix.Should().ContainKey(1);
    }

    /// <summary>
    /// EN: AddFunctionalIndexColumn with JSON expression evaluates correctly for
    ///     json_extract path lookups.
    /// PT-br: AddFunctionalIndexColumn com expressao JSON avalia corretamente para
    ///        consultas de caminho json_extract.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void DirectApi_AddFunctionalIndexColumn_WithJsonExtract_ShouldLookup()
    {
        var db = new MySqlDbMock();
        var tb = db.AddTable("docs");
        tb.AddColumn("id", DbType.Int32, false);
        tb.AddColumn("payload", DbType.String, false);

        var funcCol = tb.AddFunctionalIndexColumn(
            "__func_idx_0__",
            "json_extract(payload, '$.status')",
            db);

        var idxDef = tb.CreateIndex("ix_status", ["__func_idx_0__"]);

        tb.Add(new Dictionary<int, object?> { [0] = 1, [1] = @"{""status"": ""active""}" });
        tb.Add(new Dictionary<int, object?> { [0] = 2, [1] = @"{""status"": ""inactive""}" });

        var ix = tb.Lookup(idxDef, new IndexKey("\"active\""));
        ix.Should().NotBeNull("lookup for active should not be null");
        ix.Should().ContainKey(0);

        ix = tb.Lookup(idxDef, new IndexKey("\"inactive\""));
        ix.Should().NotBeNull("lookup for inactive should not be null");
        ix.Should().ContainKey(1);
    }

    /// <summary>
    /// EN: Version 84 (MySQL 8.4) creates functional indexes via SQL.
    /// PT-br: Versao 84 (MySQL 8.4) cria indices funcionais via SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlFunctionalIndex")]
    public void Version_84_ShouldCreateFunctionalIndexes()
    {
        var db = new MySqlDbMock(84);
        using var connection = new MySqlConnectionMock(db);
        connection.Open();

        using var cmd = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE TABLE test (id INT PRIMARY KEY, name VARCHAR(100))"
        };
        cmd.ExecuteNonQuery();

        cmd.CommandText =
            "CREATE INDEX ix_upper ON test ((UPPER(name)))";
        cmd.ExecuteNonQuery();

        db.GetTable("test").Indexes.Should().ContainKey("ix_upper");
    }

    // ---------------- helpers ----------------

    private static MySqlConnectionMock CreateOpenConnection()
    {
        var db = new MySqlDbMock();
        var cnn = new MySqlConnectionMock(db);
        cnn.Open();
        return cnn;
    }

    private static void CreateTableWithDocuments(MySqlConnectionMock connection)
    {
        using var cmd = new MySqlCommandMock(connection)
        {
            CommandText =
                "CREATE TABLE documents (" +
                "id INT PRIMARY KEY, " +
                "title VARCHAR(200)" +
                ")"
        };
        cmd.ExecuteNonQuery();
    }

    private static MySqlConnectionMock CreateOpenConnectionWithConcatTable()
    {
        var db = new MySqlDbMock();
        var users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("first_name", DbType.String, false);
        users.AddColumn("last_name", DbType.String, false);

        var cnn = new MySqlConnectionMock(db);
        cnn.Open();
        return cnn;
    }
}
