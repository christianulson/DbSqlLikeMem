namespace DbSqlLikeMem.Firebird.Test.Views;

/// <summary>
/// EN: Covers Firebird CREATE VIEW execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de CREATE VIEW no motor simulado Firebird.
/// </summary>
public sealed class FirebirdCreateViewEngineTests : XUnitTestBase
{
    private readonly FirebirdConnectionMock connection;
    private readonly ITableMock users;

    /// <summary>
    /// EN: Creates the Firebird users table used by the view tests.
    /// PT: Cria a tabela users do Firebird usada pelos testes de view.
    /// </summary>
    public FirebirdCreateViewEngineTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new FirebirdDbMock();
        users = db.AddTable("users");
        users.AddColumn("id", DbType.Int32, false);
        users.AddColumn("name", DbType.String, false);
        users.AddColumn("tenantid", DbType.Int32, false);
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "John", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob", [2] = 10 });
        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Jane", [2] = 20 });

        connection = new FirebirdConnectionMock(db);
        connection.Open();
    }

    /// <summary>
    /// EN: Verifies a view returns rows projected from the base table.
    /// PT: Verifica se a view retorna linhas projetadas a partir da tabela base.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void CreateView_ThenSelectFromView_ShouldReturnExpectedRows()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = """
CREATE VIEW v10 AS
SELECT id, name FROM users WHERE tenantid = 10;
""";
            create.ExecuteNonQuery();
        }

        using var cmd = new FirebirdCommandMock(connection)
        {
            CommandText = "SELECT id, name FROM v10 ORDER BY id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        var names = new List<string>();
        while (reader.Read())
        {
            ids.Add(reader.GetInt32(0));
            names.Add(reader.GetString(1));
        }

        Assert.Equal([1, 2], ids);
        Assert.Equal(["John", "Bob"], names);
    }

    /// <summary>
    /// EN: Verifies view reads reflect later base table changes.
    /// PT: Verifica se leituras da view refletem alteracoes posteriores na tabela base.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void View_ShouldReflectBaseTableChanges()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE VIEW v_all AS SELECT id FROM users ORDER BY id;";
            create.ExecuteNonQuery();
        }

        users.Add(new Dictionary<int, object?> { [0] = 4, [1] = "Zoe", [2] = 10 });

        using var cmd = new FirebirdCommandMock(connection)
        {
            CommandText = "SELECT id FROM v_all ORDER BY id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        Assert.Equal([1, 2, 3, 4], ids);
    }

    /// <summary>
    /// EN: Verifies CREATE OR ALTER VIEW replaces the stored definition.
    /// PT: Verifica se CREATE OR ALTER VIEW substitui a definicao armazenada.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void CreateOrAlterView_ShouldChangeDefinition()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE VIEW v AS SELECT id FROM users WHERE tenantid = 10;";
            create.ExecuteNonQuery();
        }

        using (var replace = new FirebirdCommandMock(connection))
        {
            replace.CommandText = "CREATE OR ALTER VIEW v AS SELECT id FROM users WHERE tenantid = 20;";
            replace.ExecuteNonQuery();
        }

        using var cmd = new FirebirdCommandMock(connection)
        {
            CommandText = "SELECT id FROM v ORDER BY id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        Assert.Equal([3], ids);
    }

    /// <summary>
    /// EN: Verifies RECREATE VIEW replaces the stored definition.
    /// PT: Verifica se RECREATE VIEW substitui a definicao armazenada.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void RecreateView_ShouldChangeDefinition()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE VIEW vre AS SELECT id FROM users WHERE tenantid = 10;";
            create.ExecuteNonQuery();
        }

        using (var replace = new FirebirdCommandMock(connection))
        {
            replace.CommandText = "RECREATE VIEW vre AS SELECT id FROM users WHERE tenantid = 20;";
            replace.ExecuteNonQuery();
        }

        using var cmd = new FirebirdCommandMock(connection)
        {
            CommandText = "SELECT id FROM vre ORDER BY id"
        };

        using var reader = cmd.ExecuteReader();
        var ids = new List<int>();
        while (reader.Read())
            ids.Add(reader.GetInt32(0));

        Assert.Equal([3], ids);
    }

    /// <summary>
    /// EN: Verifies dropping a view removes its definition.
    /// PT: Verifica se remover uma view exclui sua definicao.
    /// </summary>
    [Fact]
    [Trait("Category", "Views")]
    public void DropView_ShouldRemoveDefinition()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE VIEW vdrop AS SELECT id FROM users;";
            create.ExecuteNonQuery();
        }

        using (var drop = new FirebirdCommandMock(connection))
        {
            drop.CommandText = "DROP VIEW vdrop;";
            drop.ExecuteNonQuery();
        }

        var ex = Assert.ThrowsAny<Exception>(() =>
        {
            using var cmd = new FirebirdCommandMock(connection)
            {
                CommandText = "SELECT * FROM vdrop"
            };
            cmd.ExecuteReader();
        });

        Assert.NotNull(ex);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        connection.Dispose();
        base.Dispose(disposing);
    }
}
