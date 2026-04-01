using FluentAssertions;

namespace DbSqlLikeMem.MySql.Test.Strategy;

/// <summary>
/// EN: Covers schema-level backup and restore when table work runs in parallel on a thread-safe MySQL database.
/// PT: Cobre backup e restore em nivel de schema quando o trabalho das tabelas executa em paralelo em um banco MySQL thread-safe.
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
        var db = new MySqlDbMock
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

        var schema = db["DefaultSchema"].Should().BeOfType<MySqlSchemaMock>().Subject;
        var users = db.GetTable("Users");
        var orders = db.GetTable("Orders");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Alice" });
        orders.Add(new Dictionary<int, object?> { [0] = 10, [1] = 1 });

        schema.BackupAllTablesBestEffort();

        users.Add(new Dictionary<int, object?> { [0] = 2, [1] = "Bob" });
        orders.Add(new Dictionary<int, object?> { [0] = 11, [1] = 2 });

        schema.RestoreAllTablesBestEffort();

        users.Should().ContainSingle();
        orders.Should().ContainSingle();
        users[0][1].Should().Be("Alice");
        orders[0][1].Should().Be(1);

        schema.ClearBackupAllTablesBestEffort();

        users.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carol" });
        schema.RestoreAllTablesBestEffort();

        users.Should().HaveCount(2);
        users[1][1].Should().Be("Carol");
    }

    /// <summary>
    /// EN: Verifies backup and restore stay consistent across multiple schemas when thread safety is enabled.
    /// PT: Verifica se backup e restore permanecem consistentes em multiplos schemas quando a thread safety esta habilitada.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void BackupRestoreAndClear_ShouldWorkAcrossSchemas_WhenThreadSafe()
    {
        var db = new MySqlDbMock
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

        defaultUsers.Should().ContainSingle();
        archiveUsers.Should().ContainSingle();
        defaultUsers[0][1].Should().Be("Ana");
        archiveUsers[0][1].Should().Be("Zed");

        db.ClearBackupAllTablesBestEffort();

        defaultUsers.Add(new Dictionary<int, object?> { [0] = 3, [1] = "Carol" });
        archiveUsers.Add(new Dictionary<int, object?> { [0] = 102, [1] = "Neo" });
        db.RestoreAllTablesBestEffort();

        defaultUsers.Should().HaveCount(2);
        archiveUsers.Should().HaveCount(2);
        defaultUsers[1][1].Should().Be("Carol");
        archiveUsers[1][1].Should().Be("Neo");
    }

    /// <summary>
    /// EN: Verifies volatile reset clears rows and identities across multiple schemas in thread-safe mode.
    /// PT: Verifica se o reset volatil limpa linhas e identidades em multiplos schemas no modo thread-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Concurrency")]
    public void ResetVolatileData_ShouldWorkAcrossSchemas_WhenThreadSafe()
    {
        var db = new MySqlDbMock
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
        defaultUsers.NextIdentity.Should().Be(2);
        archiveUsers.NextIdentity.Should().Be(2);

        db.ResetVolatileData();

        defaultUsers.Should().BeEmpty();
        archiveUsers.Should().BeEmpty();
        defaultUsers.NextIdentity.Should().Be(1);
        archiveUsers.NextIdentity.Should().Be(1);
    }
}
