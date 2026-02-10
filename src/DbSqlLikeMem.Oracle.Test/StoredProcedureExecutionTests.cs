namespace DbSqlLikeMem.Oracle.Test;

public sealed class StoredProcedureExecutionTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    // helper: cria parâmetro MySQL do seu mock (ajuste o tipo se necessário)
    private static OracleParameter P(string name, object? value, DbType dbType, ParameterDirection dir = ParameterDirection.Input)
        => new()
        {
            ParameterName = name.StartsWith('@')
                ? name
                : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
            Direction = dir
        };

    /// <summary>
    /// EN: Tests ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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

        using var cmd = new OracleCommandMock(c)
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

    /// <summary>
    /// EN: Tests ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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

        using var cmd = new OracleCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_add_user"
        };

        cmd.Parameters.Add(P("p_name", "John", DbType.String));
        // faltou p_email

        // Act + Assert
        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());

        // 1318 = "Incorrect number of arguments for PROCEDURE ..."
        Assert.Equal(1318, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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

        using var cmd = new OracleCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_add_user"
        };

        cmd.Parameters.Add(P("p_name", "John", DbType.String));
        cmd.Parameters.Add(P("p_email", null, DbType.String)); // DBNull

        // Act + Assert
        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());

        // 1048 = "Column cannot be null" (não é perfeito p/ SP, mas é um bom comportamento)
        Assert.Equal(1048, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue behavior.
    /// PT: Testa o comportamento de ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue.
    /// </summary>
    [Fact]
    public void ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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

        using var cmd = new OracleCommandMock(c)
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
        Assert.Equal(string.Empty, ((OracleParameter)cmd.Parameters["@o_token"]).Value);
        Assert.Equal(0, ((OracleParameter)cmd.Parameters["@o_status"]).Value);
    }

    /// <summary>
    /// EN: Tests ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset behavior.
    /// PT: Testa o comportamento de ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset.
    /// </summary>
    [Fact]
    public void ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset()
    {
        // Arrange
        using var c = new OracleConnectionMock();
        c.Open();

        c.AddProdecure("sp_ping", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null
        ));

        using var cmd = new OracleCommandMock(c)
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

    /// <summary>
    /// EN: Tests DapperExecute_CommandTypeStoredProcedure_ShouldWork behavior.
    /// PT: Testa o comportamento de DapperExecute_CommandTypeStoredProcedure_ShouldWork.
    /// </summary>
    [Fact]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldWork()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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

    /// <summary>
    /// EN: Tests DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam behavior.
    /// PT: Testa o comportamento de DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam.
    /// </summary>
    [Fact]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam()
    {
        // Arrange
        using var c = new OracleConnectionMock();
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
        var ex = Assert.Throws<OracleMockException>(() =>
            c.Execute(
                "sp_add_user",
                new { p_name = "John" }, // faltou p_email
                commandType: CommandType.StoredProcedure
            )
        );

        Assert.Equal(1318, ex.ErrorCode);
    }
}
