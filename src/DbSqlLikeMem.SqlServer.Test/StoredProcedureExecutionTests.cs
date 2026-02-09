namespace DbSqlLikeMem.SqlServer.Test;

public sealed class StoredProcedureExecutionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    // helper: cria parâmetro MySQL do seu mock (ajuste o tipo se necessário)
    private static SqlParameter P(string name, object? value, DbType dbType, ParameterDirection dir = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith('@')
                ? name
                : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = dir
        };

    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        // cadastra contrato da procedure
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        using var cmd = new SqlServerCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_add_user"
        };

        cmd.Parameters.Add(P("p_name", "John", DbType.String));
        cmd.Parameters.Add(P("p_email", "john@x.com", DbType.String));

        // Act
        var affected = cmd.ExecuteNonQuery();

        // Assert
        // sem corpo: você decide se retorna 0 ou 1. Eu recomendo 0.
        Assert.Equal(0, affected);
    }

    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        using var cmd = new SqlServerCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_add_user"
        };

        cmd.Parameters.Add(P("p_name", "John", DbType.String));
        // faltou p_email

        // Act + Assert
        var ex = Assert.Throws<SqlServerMockException>(() => cmd.ExecuteNonQuery());

        // 1318 = "Incorrect number of arguments for PROCEDURE ..."
        Assert.Equal(1318, ex.ErrorCode);
    }

    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        using var cmd = new SqlServerCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_add_user"
        };

        cmd.Parameters.Add(P("p_name", "John", DbType.String));
        cmd.Parameters.Add(P("p_email", null, DbType.String)); // DBNull

        // Act + Assert
        var ex = Assert.Throws<SqlServerMockException>(() => cmd.ExecuteNonQuery());

        // 1048 = "Column cannot be null" (não é perfeito p/ SP, mas é um bom comportamento)
        Assert.Equal(1048, ex.ErrorCode);
    }

    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_create_token", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_userid", DbType.Int32),
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("o_token", DbType.String),
                new ProcParam("o_status", DbType.Int32),
            ],
            ReturnParam: null
        ));

        using var cmd = new SqlServerCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_create_token"
        };

        cmd.Parameters.Add(P("p_userid", 1, DbType.Int32));
        cmd.Parameters.Add(P("o_token", null, DbType.String, ParameterDirection.Output));
        cmd.Parameters.Add(P("o_status", null, DbType.Int32, ParameterDirection.Output));

        // Act
        cmd.ExecuteNonQuery();

        // Assert
        // como você não tem corpo, o mock pode setar:
        // - string: "" (ou null)
        // - int: 0
        // Eu recomendo setar defaults “tipo-safe”.
        Assert.Equal(string.Empty, ((SqlParameter)cmd.Parameters["@o_token"]).Value);
        Assert.Equal(0, ((SqlParameter)cmd.Parameters["@o_status"]).Value);
    }

    [Fact]
    public void ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_ping", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        using var cmd = new SqlServerCommandMock(c)
        {
            CommandText = "CALL sp_ping(@p_id)"
        };
        cmd.Parameters.Add(P("p_id", 123, DbType.Int32));

        // Act
        using var r = cmd.ExecuteReader();

        // Assert
        // sem corpo: resultado vazio, mas a chamada “funciona”
        Assert.Equal(0, r.FieldCount);
        Assert.False(r.Read());
    }

    [Fact]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldWork()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        // Act
        var affected = c.Execute(
            "sp_add_user",
            new { p_name = "John", p_email = "john@x.com" },
            commandType: CommandType.StoredProcedure
        );

        // Assert
        Assert.Equal(0, affected);
    }

    [Fact]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam()
    {
        // Arrange
        using var c = new SqlServerConnectionMock();
        c.Open();

        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        // Act + Assert
        var ex = Assert.Throws<SqlServerMockException>(() =>
            c.Execute(
                "sp_add_user",
                new { p_name = "John" }, // faltou p_email
                commandType: CommandType.StoredProcedure
            )
        );

        Assert.Equal(1318, ex.ErrorCode);
    }
}