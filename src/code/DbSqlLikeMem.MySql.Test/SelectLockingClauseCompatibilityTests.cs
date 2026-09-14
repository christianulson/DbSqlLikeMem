namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Compatibility tests for MySQL locking clauses (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE)
/// used by the Krnl-AI stores.
/// PT-br: Testes de compatibilidade das cl�usulas de lock do MySQL (FOR UPDATE, FOR SHARE, LOCK IN SHARE MODE)
/// usadas pelos stores do Krnl-AI.
/// </summary>
public sealed class SelectLockingClauseCompatibilityTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    private static MySqlCommandMock CreateSelectCommand(MySqlConnectionMock cnn, string tail)
    {
        cnn.Define("moments");
        cnn.Column<string>("moments", "moment_id");
        cnn.Column<string>("moments", "metadata_json");
        cnn.Seed("moments", null,
            ["m1", """{"tenant_id":"t1"}"""],
            ["m2", """{"tenant_id":"t2"}"""]);

        return new MySqlCommandMock(cnn)
        {
            CommandText = $"SELECT metadata_json FROM moments WHERE moment_id = 'm1' {tail}"
        };
    }

    /// <summary>Parses and executes SELECT ... FOR UPDATE.</summary>
    [Fact]
    public void SelectForUpdate_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        using var cmd = CreateSelectCommand(cnn, "FOR UPDATE");

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("""{"tenant_id":"t1"}""", reader.GetString(0));
        Assert.False(reader.Read());
    }

    /// <summary>Parses and executes SELECT ... FOR SHARE.</summary>
    [Fact]
    public void SelectForShare_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        using var cmd = CreateSelectCommand(cnn, "FOR SHARE");

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("""{"tenant_id":"t1"}""", reader.GetString(0));
    }

    /// <summary>Parses and executes SELECT ... LOCK IN SHARE MODE.</summary>
    [Fact]
    public void SelectLockInShareMode_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        using var cmd = CreateSelectCommand(cnn, "LOCK IN SHARE MODE");

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("""{"tenant_id":"t1"}""", reader.GetString(0));
    }

    /// <summary>Parses and executes SELECT ... FOR UPDATE NOWAIT.</summary>
    [Fact]
    public void SelectForUpdateNowait_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        using var cmd = CreateSelectCommand(cnn, "FOR UPDATE NOWAIT");

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("""{"tenant_id":"t1"}""", reader.GetString(0));
    }

    /// <summary>Parses and executes SELECT ... FOR UPDATE SKIP LOCKED.</summary>
    [Fact]
    public void SelectForUpdateSkipLocked_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        using var cmd = CreateSelectCommand(cnn, "FOR UPDATE SKIP LOCKED");

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("""{"tenant_id":"t1"}""", reader.GetString(0));
    }

    /// <summary>Parses and executes SELECT ... ORDER BY ... LIMIT ... FOR UPDATE.</summary>
    [Fact]
    public void SelectForUpdate_WithOrderByAndLimit_ShouldParseAndReturnRows()
    {
        using var cnn = new MySqlConnectionMock();
        cnn.Define("moments");
        cnn.Column<string>("moments", "moment_id");
        cnn.Column<int>("moments", "sequence");
        cnn.Seed("moments", null,
            ["m1", 1],
            ["m2", 2]);
        using var cmd = new MySqlCommandMock(cnn)
        {
            CommandText = "SELECT moment_id FROM moments ORDER BY sequence DESC LIMIT 1 FOR UPDATE"
        };

        using var reader = cmd.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("m2", reader.GetString(0));
    }
}