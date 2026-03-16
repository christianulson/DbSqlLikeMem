namespace DbSqlLikeMem.Sqlite.Test;

/// <summary>
/// EN: Verifies schema sequences can be registered and retrieved through the database and connection.
/// PT: Verifica se sequences de schema podem ser registradas e recuperadas pelo banco e pela conexao.
/// </summary>
public sealed class SequenceRegistrationTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies AddSequence stores the sequence inside the target schema and keeps its numeric settings.
    /// PT: Verifica se AddSequence armazena a sequence no schema alvo e preserva suas configuracoes numericas.
    /// </summary>
    [Fact]
    public void AddSequence_ShouldRegisterSequenceInsideSchema()
    {
        var db = new SqliteDbMock();
        db.CreateSchema("app");

        var sequence = db.AddSequence("seq_orders", startValue: 100, incrementBy: 5, currentValue: 115, schemaName: "app");

        Assert.True(db.TryGetSequence("seq_orders", out var found, "app"));
        Assert.NotNull(found);
        Assert.Same(sequence, found);
        Assert.Equal(100, found!.StartValue);
        Assert.Equal(5, found.IncrementBy);
        Assert.Equal(115, found.CurrentValue);

        var schema = Assert.IsAssignableFrom<ISchemaMock>(((IReadOnlyDictionary<string, ISchemaMock>)db)["app"]);
        Assert.True(schema.Sequences.ContainsKey("seq_orders"));
    }

    /// <summary>
    /// EN: Verifies the connection forwards sequence registration to its current schema.
    /// PT: Verifica se a conexao encaminha o registro de sequence para seu schema atual.
    /// </summary>
    [Fact]
    public void ConnectionAddSequence_ShouldUseCurrentSchema()
    {
        var db = new SqliteDbMock();
        db.CreateSchema("app");
        using var connection = new SqliteConnectionMock(db, "app");

        var sequence = connection.AddSequence("seq_users", startValue: 10, incrementBy: 2);

        Assert.True(connection.TryGetSequence("seq_users", out var found));
        Assert.NotNull(found);
        Assert.Same(sequence, found);
        Assert.Equal(10, found!.StartValue);
        Assert.Equal(2, found.IncrementBy);
        Assert.Null(found.CurrentValue);
    }
}
