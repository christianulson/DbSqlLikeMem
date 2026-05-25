namespace DbSqlLikeMem.Firebird.Test.Strategy;

/// <summary>
/// EN: Contains trigger behavior tests for the Firebird strategy.
/// PT-br: Contém testes de comportamento de gatilhos para a estratégia Firebird.
/// </summary>
public sealed class FirebirdTriggerStrategyTests
{
    /// <summary>
    /// EN: Ensures that an AFTER INSERT trigger is executed for a regular Firebird table.
    /// PT-br: Garante que um gatilho AFTER INSERT seja executado para uma tabela Firebird regular.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void NonTemporaryTable_ShouldExecuteAfterInsertTrigger()
    {
        var db = new FirebirdDbMock();
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);

        var calls = 0;
        var triggerTable = Assert.IsAssignableFrom<TableMock>(table);
        triggerTable.AddTrigger(TableTriggerEvent.AfterInsert, _ => calls++);

        using var connection = new FirebirdConnectionMock(db);
        using var cmd = new FirebirdCommandMock(connection) { CommandText = "INSERT INTO users (id) VALUES (1)" };
        cmd.ExecuteNonQuery();

        Assert.Equal(1, calls);
    }
}
