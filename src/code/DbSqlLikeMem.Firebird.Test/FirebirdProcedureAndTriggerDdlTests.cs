namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird CREATE OR ALTER PROCEDURE and TRIGGER execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de CREATE OR ALTER PROCEDURE e TRIGGER no motor simulado Firebird.
/// </summary>
public sealed class FirebirdProcedureAndTriggerDdlTests
{
    /// <summary>
    /// EN: Verifies CREATE OR ALTER PROCEDURE registers the procedure and allows CALL execution.
    /// PT: Verifica se CREATE OR ALTER PROCEDURE registra a procedure e permite execucao via CALL.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void CreateOrAlterProcedure_ShouldRegisterProcedureAndAllowCall()
    {
        var db = new FirebirdDbMock();
        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE OR ALTER PROCEDURE sp_echo(IN tenantId INT) BEGIN END";
            create.ExecuteNonQuery();
        }

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

        var affectedRows = command.ExecuteNonQuery();

        Assert.Equal(0, affectedRows);
        Assert.True(db.TryGetProcedure("sp_echo", out var procedure));
        Assert.NotNull(procedure);
        Assert.Single(procedure!.RequiredIn);
    }

    /// <summary>
    /// EN: Verifies CREATE OR ALTER PROCEDURE accepts a default input parameter and allows CALL execution without the argument.
    /// PT: Verifica se CREATE OR ALTER PROCEDURE aceita um parametro de entrada padrao e permite execucao via CALL sem o argumento.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void CreateOrAlterProcedure_ShouldAllowCallWithoutDefaultedInput()
    {
        var db = new FirebirdDbMock();
        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE OR ALTER PROCEDURE sp_echo(IN tenantId INT = 1) BEGIN END";
            create.ExecuteNonQuery();
        }

        using var command = new FirebirdCommandMock(connection)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_echo"
        };

        var affectedRows = command.ExecuteNonQuery();

        Assert.Equal(0, affectedRows);
        Assert.True(db.TryGetProcedure("sp_echo", out var procedure));
        Assert.NotNull(procedure);
        Assert.Empty(procedure!.RequiredIn);
        Assert.Single(procedure.OptionalIn);
        Assert.Equal("tenantId", procedure.OptionalIn[0].Name, ignoreCase: true);
        Assert.Equal(1, Convert.ToInt32(procedure.OptionalIn[0].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies CREATE OR ALTER TRIGGER registers the trigger and keeps table DML working.
    /// PT: Verifica se CREATE OR ALTER TRIGGER registra o trigger e mantém o DML da tabela funcionando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void CreateOrAlterTrigger_ShouldRegisterTriggerAndAllowInsert()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE OR ALTER TRIGGER trg_users_ai AFTER INSERT ON Users BEGIN END";
            create.ExecuteNonQuery();
        }

        Assert.True(users.HasTriggers(TableTriggerEvent.AfterInsert));

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (3, 'Carol')";
            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        Assert.Single(users);
    }
}
