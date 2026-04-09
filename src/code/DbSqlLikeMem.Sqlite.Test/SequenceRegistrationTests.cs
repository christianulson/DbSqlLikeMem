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

        db.TryGetSequence("seq_orders", out var found, "app").Should().BeTrue();
        found.Should().NotBeNull();
        found.Should().BeSameAs(sequence);
        found!.StartValue.Should().Be(100);
        found.IncrementBy.Should().Be(5);
        found.CurrentValue.Should().Be(115);

        var schema = ((IReadOnlyDictionary<string, ISchemaMock>)db)["app"];
        schema.Sequences.ContainsKey("seq_orders").Should().BeTrue();
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

        connection.TryGetSequence("seq_users", out var found).Should().BeTrue();
        found.Should().NotBeNull();
        found.Should().BeSameAs(sequence);
        found!.StartValue.Should().Be(10);
        found.IncrementBy.Should().Be(2);
        found.CurrentValue.Should().BeNull();
    }
}
