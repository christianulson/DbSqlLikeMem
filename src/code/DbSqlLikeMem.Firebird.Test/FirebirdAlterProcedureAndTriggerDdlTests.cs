namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird ALTER PROCEDURE and ALTER TRIGGER execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de ALTER PROCEDURE e ALTER TRIGGER no motor simulado Firebird.
/// </summary>
public sealed class FirebirdAlterProcedureAndTriggerDdlTests : XUnitTestBase
{
    private readonly FirebirdDbMock db;
    private readonly FirebirdConnectionMock connection;
    private readonly ITableMock users;

    /// <summary>
    /// EN: Creates the Firebird database objects used by the alter tests.
    /// PT: Cria os objetos de banco Firebird usados pelos testes de alter.
    /// </summary>
    public FirebirdAlterProcedureAndTriggerDdlTests(ITestOutputHelper helper) : base(helper)
    {
        db = new FirebirdDbMock();
        users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        connection = new FirebirdConnectionMock(db);
        connection.Open();
    }

    /// <summary>
    /// EN: Verifies ALTER PROCEDURE replaces the stored signature and allows CALL execution.
    /// PT: Verifica se ALTER PROCEDURE substitui a assinatura armazenada e permite execucao via CALL.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AlterProcedure_ShouldReplaceSignatureAndAllowCall()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE PROCEDURE sp_echo(IN tenantId INT) BEGIN END";
            create.ExecuteNonQuery();
        }

        using (var alter = new FirebirdCommandMock(connection))
        {
            alter.CommandText = "ALTER PROCEDURE sp_echo(IN tenantId INT, IN suffix INT) BEGIN END";
            alter.ExecuteNonQuery();
        }

        Assert.True(db.TryGetProcedure("sp_echo", out var procedure));
        Assert.NotNull(procedure);
        Assert.Collection(procedure!.RequiredIn,
            _ => { },
            _ => { });
        Assert.Equal("tenantId", procedure.RequiredIn[0].Name, ignoreCase: true);
        Assert.Equal("suffix", procedure.RequiredIn[1].Name, ignoreCase: true);
    }

    /// <summary>
    /// EN: Verifies ALTER TRIGGER replaces the trigger event and keeps table DML working.
    /// PT: Verifica se ALTER TRIGGER substitui o evento do trigger e mantém o DML da tabela funcionando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AlterTrigger_ShouldReplaceEventAndAllowInsert()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE TRIGGER trg_users_ai AFTER INSERT ON Users BEGIN END";
            create.ExecuteNonQuery();
        }

        Assert.True(users.HasTriggers(TableTriggerEvent.AfterInsert));

        using (var alter = new FirebirdCommandMock(connection))
        {
            alter.CommandText = "ALTER TRIGGER trg_users_ai BEFORE INSERT ON Users BEGIN END";
            alter.ExecuteNonQuery();
        }

        Assert.False(users.HasTriggers(TableTriggerEvent.AfterInsert));
        Assert.True(users.HasTriggers(TableTriggerEvent.BeforeInsert));

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (3, 'Carol')";
            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        Assert.Single(users);
    }

    /// <summary>
    /// EN: Disposes the Firebird connection used by the alter tests.
    /// PT: Descarta a conexao Firebird usada pelos testes de alter.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        connection.Dispose();
        base.Dispose(disposing);
    }
}
