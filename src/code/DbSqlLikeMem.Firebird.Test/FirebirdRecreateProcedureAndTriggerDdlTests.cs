namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird RECREATE PROCEDURE and TRIGGER execution scenarios in the mock engine.
/// PT-br: Cobre cenarios de execucao de RECREATE PROCEDURE e TRIGGER no motor simulado Firebird.
/// </summary>
public sealed class FirebirdRecreateProcedureAndTriggerDdlTests : XUnitTestBase
{
    private readonly FirebirdDbMock db;
    private readonly FirebirdConnectionMock connection;
    private readonly ITableMock users;

    /// <summary>
    /// EN: Creates the Firebird database objects used by the recreate tests.
    /// PT-br: Cria os objetos de banco Firebird usados pelos testes de recreate.
    /// </summary>
    public FirebirdRecreateProcedureAndTriggerDdlTests(ITestOutputHelper helper) : base(helper)
    {
        db = new FirebirdDbMock();
        users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        connection = new FirebirdConnectionMock(db);
        connection.Open();
    }

    /// <summary>
    /// EN: Verifies RECREATE PROCEDURE replaces the stored signature and allows CALL execution.
    /// PT-br: Verifica se RECREATE PROCEDURE substitui a assinatura armazenada e permite execucao via CALL.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void RecreateProcedure_ShouldReplaceSignatureAndAllowCall()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE PROCEDURE sp_echo(IN tenantId INT) BEGIN END";
            create.ExecuteNonQuery();
        }

        using (var replace = new FirebirdCommandMock(connection))
        {
            replace.CommandText = "RECREATE PROCEDURE sp_echo(IN tenantId INT, IN suffix INT) BEGIN END";
            replace.ExecuteNonQuery();
        }

        Assert.True(db.TryGetProcedure("sp_echo", out var procedure));
        Assert.NotNull(procedure);
        Assert.Collection(procedure!.RequiredIn,
            _ => { },
            _ => { });
        Assert.Equal("tenantId", procedure.RequiredIn[0].Name, ignoreCase: true);
        Assert.Equal("suffix", procedure.RequiredIn[1].Name, ignoreCase: true);

        using var command = new FirebirdCommandMock(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_echo"
        };

        var tenantId = command.CreateParameter();
        tenantId.ParameterName = "@tenantId";
        tenantId.DbType = DbType.Int32;
        tenantId.Value = 10;
        command.Parameters.Add(tenantId);

        var suffix = command.CreateParameter();
        suffix.ParameterName = "@suffix";
        suffix.DbType = DbType.Int32;
        suffix.Value = 20;
        command.Parameters.Add(suffix);

        var affectedRows = command.ExecuteNonQuery();

        Assert.Equal(0, affectedRows);
    }

    /// <summary>
    /// EN: Verifies RECREATE TRIGGER replaces the trigger event and keeps table DML working.
    /// PT-br: Verifica se RECREATE TRIGGER substitui o evento do trigger e mantém o DML da tabela funcionando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void RecreateTrigger_ShouldReplaceEventAndAllowInsert()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE TRIGGER trg_users_ai AFTER INSERT ON Users BEGIN END";
            create.ExecuteNonQuery();
        }

        Assert.True(users.HasTriggers(TableTriggerEvent.AfterInsert));
        Assert.False(users.HasTriggers(TableTriggerEvent.BeforeInsert));

        using (var replace = new FirebirdCommandMock(connection))
        {
            replace.CommandText = "RECREATE TRIGGER trg_users_ai BEFORE INSERT ON Users BEGIN END";
            replace.ExecuteNonQuery();
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
    /// EN: Disposes the Firebird connection used by the recreate tests.
    /// PT-br: Descarta a conexao Firebird usada pelos testes de recreate.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        connection.Dispose();
        base.Dispose(disposing);
    }
}
