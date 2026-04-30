namespace DbSqlLikeMem.SqlServer.Test.Strategy;

/// <summary>
/// EN: Covers schema-level backup and restore when table work runs in parallel on a thread-safe SQL Server database.
/// PT: Cobre backup e restore em nivel de schema quando o trabalho das tabelas executa em paralelo em um banco SQL Server thread-safe.
/// </summary>
public sealed class SchemaParallelizationTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies backup, restore, and backup clearing keep table data consistent across multiple tables.
    /// PT: Verifica se backup, restore e limpeza de backup mantem os dados das tabelas consistentes em multiplas tabelas.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void BackupRestoreAndClear_ShouldWorkWithMultipleTables_WhenThreadSafe()
    {
        var db = new SqlServerDbMock
        {
            ThreadSafe = true
        };

        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false)
        ]);
        db.AddTable("Orders", [
            new("OrderId", DbType.Int32, false),
            new("UserId", DbType.Int32, false)
        ]);

        var schema = Assert.IsType<SqlServerSchemaMock>(db["DefaultSchema"]);
        var users = db.GetTable("Users");
        var orders = db.GetTable("Orders");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Alice" });
        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1 });

        schema.BackupAllTablesBestEffort();

        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2 });

        schema.RestoreAllTablesBestEffort();

        Assert.Single(users);
        Assert.Single(orders);
        Assert.Equal("Alice", users[0][1]);
        Assert.Equal(1, orders[0][1]);

        schema.ClearBackupAllTablesBestEffort();

        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carol" });
        schema.RestoreAllTablesBestEffort();

        Assert.Equal(2, users.Count);
        Assert.Equal("Carol", users[1][1]);
    }

    /// <summary>
    /// EN: Verifies backup and restore stay consistent across multiple schemas when thread safety is enabled.
    /// PT: Verifica se backup e restore permanecem consistentes em multiplos schemas quando a thread safety esta habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void BackupRestoreAndClear_ShouldWorkAcrossSchemas_WhenThreadSafe()
    {
        var db = new SqlServerDbMock
        {
            ThreadSafe = true
        };

        var defaultUsers = db.AddTable("users");
        defaultUsers.AddColumn("id", DbType.Int32, false);
        defaultUsers.AddColumn("name", DbType.String, false);

        db.CreateSchema("Archive");
        var archiveUsers = db.AddTable("users_archive", schemaName: "Archive");
        archiveUsers.AddColumn("id", DbType.Int32, false);
        archiveUsers.AddColumn("name", DbType.String, false);

        defaultUsers.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });
        archiveUsers.Add(new Dictionary<int, object?> { [0] = 100, [1] = "Zed" });

        db.BackupAllTablesBestEffort();

        defaultUsers.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });
        archiveUsers.Add(new Dictionary<int, object?> { [0] = 101, [1] = "Amy" });

        db.RestoreAllTablesBestEffort();

        Assert.Single(defaultUsers);
        Assert.Single(archiveUsers);
        Assert.Equal("Ana", defaultUsers[0][1]);
        Assert.Equal("Zed", archiveUsers[0][1]);

        db.ClearBackupAllTablesBestEffort();

        defaultUsers.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carol" });
        archiveUsers.Add(new Dictionary<int, object?> { [0] = 102, [1] = "Neo" });
        db.RestoreAllTablesBestEffort();

        Assert.Equal(2, defaultUsers.Count);
        Assert.Equal(2, archiveUsers.Count);
        Assert.Equal("Carol", defaultUsers[1][1]);
        Assert.Equal("Neo", archiveUsers[1][1]);
    }

    /// <summary>
    /// EN: Verifies volatile reset clears rows and identities across multiple schemas in thread-safe mode.
    /// PT: Verifica se o reset volatil limpa linhas e identidades em multiplos schemas no modo thread-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void ResetVolatileData_ShouldWorkAcrossSchemas_WhenThreadSafe()
    {
        var db = new SqlServerDbMock
        {
            ThreadSafe = true
        };

        var defaultUsers = db.AddTable("users");
        defaultUsers.AddColumn("id", DbType.Int32, false, identity: true);
        defaultUsers.AddColumn("name", DbType.String, false);

        db.CreateSchema("Archive");
        var archiveUsers = db.AddTable("users_archive", schemaName: "Archive");
        archiveUsers.AddColumn("id", DbType.Int32, false, identity: true);
        archiveUsers.AddColumn("name", DbType.String, false);

        defaultUsers.Add(new Dictionary<int, object?> { [1] = "Ana" });
        archiveUsers.Add(new Dictionary<int, object?> { [1] = "Zed" });
        Assert.Equal(2, defaultUsers.NextIdentity);
        Assert.Equal(2, archiveUsers.NextIdentity);

        db.ResetVolatileData();

        Assert.Empty(defaultUsers);
        Assert.Empty(archiveUsers);
        Assert.Equal(1, defaultUsers.NextIdentity);
        Assert.Equal(1, archiveUsers.NextIdentity);
    }
}
