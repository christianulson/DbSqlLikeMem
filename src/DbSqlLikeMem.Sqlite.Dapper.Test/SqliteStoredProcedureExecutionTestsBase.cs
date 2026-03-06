namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: SQLite-specific stored procedure contract tests preserving the provider's parameter-direction limitations.
/// PT: Testes de contrato específicos de stored procedure para SQLite, preservando as limitações do provedor em direções de parâmetro.
/// </summary>
public abstract class SqliteStoredProcedureExecutionTestsBase(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates and opens the SQLite mock connection used by stored procedure contract tests.
    /// PT: Cria e abre a conexao simulada do SQLite usada pelos testes de contrato de stored procedure.
    /// </summary>
    protected abstract SqliteConnectionMock CreateOpenConnection();

    /// <summary>
    /// EN: Creates a command configured to execute a stored procedure on the SQLite mock connection.
    /// PT: Cria um comando configurado para executar uma stored procedure na conexao simulada do SQLite.
    /// </summary>
    protected abstract SqliteCommandMock CreateStoredProcedureCommand(SqliteConnectionMock connection, string procedureName);

    /// <summary>
    /// EN: Creates a text command for executing raw SQL against the SQLite mock connection.
    /// PT: Cria um comando de texto para executar SQL bruto na conexao simulada do SQLite.
    /// </summary>
    protected abstract SqliteCommandMock CreateTextCommand(SqliteConnectionMock connection, string commandText);

    /// <summary>
    /// EN: Creates a SQLite parameter adapted to the provider limitations around parameter direction.
    /// PT: Cria um parametro SQLite adaptado as limitacoes do provedor em torno da direcao do parametro.
    /// </summary>
    protected virtual SqliteParameter CreateParameter(string name, object? value, DbType dbType, ParameterDirection direction = ParameterDirection.Input)
    {
        var parameter = new SqliteParameter
        {
            ParameterName = name.StartsWith("@") ? name : "@" + name,
            Value = value ?? DBNull.Value,
            DbType = dbType,
        };

        if (direction == ParameterDirection.Input)
            parameter.Direction = direction;

        return parameter;
    }

    /// <summary>
    /// EN: Verifies stored procedure execution succeeds when all required input parameters are provided.
    /// PT: Verifica se a execucao da stored procedure funciona quando todos os parametros de entrada obrigatorios sao fornecidos.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldValidateRequiredInputs()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));
        cmd.Parameters.Add(CreateParameter("p_email", "john@x.com", DbType.String));

        Assert.Equal(0, cmd.ExecuteNonQuery());
    }

    /// <summary>
    /// EN: Verifies stored procedure execution throws when a required input parameter is missing.
    /// PT: Verifica se a execucao da stored procedure lanca excecao quando falta um parametro de entrada obrigatorio.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenMissingRequiredInput()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));

        var ex = Assert.Throws<SqliteMockException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1318, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Verifies stored procedure execution throws when a required input parameter is null.
    /// PT: Verifica se a execucao da stored procedure lanca excecao quando um parametro de entrada obrigatorio e nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldThrow_WhenRequiredInputIsNull()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_add_user");
        cmd.Parameters.Add(CreateParameter("p_name", "John", DbType.String));
        cmd.Parameters.Add(CreateParameter("p_email", null, DbType.String));

        var ex = Assert.Throws<SqliteMockException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1048, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Verifies stored procedure execution populates output parameters with default values.
    /// PT: Verifica se a execucao da stored procedure preenche parametros de saida com valores padrao.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldPopulateOutParameters_DefaultValue()
    {
        using var c = CreateOpenConnection();
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
            ReturnParam: null));

        using var cmd = CreateStoredProcedureCommand(c, "sp_create_token");
        cmd.Parameters.Add(CreateParameter("p_userid", 1, DbType.Int32));
        cmd.Parameters.Add(CreateParameter("o_token", null, DbType.String, ParameterDirection.Output));
        cmd.Parameters.Add(CreateParameter("o_status", null, DbType.Int32, ParameterDirection.Output));

        cmd.ExecuteNonQuery();

        Assert.Equal(string.Empty, ((SqliteParameter)cmd.Parameters["@o_token"]).Value);
        Assert.Equal(0, ((SqliteParameter)cmd.Parameters["@o_status"]).Value);
    }

    /// <summary>
    /// EN: Verifies CALL syntax validates parameters and returns an empty result set when appropriate.
    /// PT: Verifica se a sintaxe CALL valida os parametros e retorna um conjunto de resultados vazio quando apropriado.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteReader_CallSyntax_ShouldValidateAndReturnEmptyResultset()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_ping", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var cmd = CreateTextCommand(c, "CALL sp_ping(@p_id)");
        cmd.Parameters.Add(CreateParameter("p_id", 123, DbType.Int32));

        using var r = cmd.ExecuteReader();

        Assert.Equal(0, r.FieldCount);
        Assert.False(r.Read());
    }

    /// <summary>
    /// EN: Verifies return value parameters remain unset when the provider does not support that direction.
    /// PT: Verifica se parametros de valor de retorno permanecem sem valor quando o provedor nao suporta essa direcao.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldKeepReturnValueUnset_WhenProviderDoesNotSupportDirection()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_with_status", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: new ProcParam("ret", DbType.Int32)));

        using var command = CreateStoredProcedureCommand(c, "sp_with_status");
        command.Parameters.Add(CreateParameter("p_id", 1, DbType.Int32));
        command.Parameters.Add(CreateParameter("ret", null, DbType.Int32, ParameterDirection.ReturnValue));

        command.ExecuteNonQuery();

        Assert.Equal(DBNull.Value, command.Parameters["@ret"].Value);
    }

    /// <summary>
    /// EN: Verifies execution does not throw when the provider cannot represent output direction semantics.
    /// PT: Verifica se a execucao nao lanca excecao quando o provedor nao consegue representar a semantica de direcao de saida.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void ExecuteNonQuery_StoredProcedure_ShouldNotThrow_WhenProviderCannotRepresentOutputDirection()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_with_input", new ProcedureDef(
            RequiredIn: [new ProcParam("p_id", DbType.Int32)],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        using var command = CreateStoredProcedureCommand(c, "sp_with_input");
        command.Parameters.Add(CreateParameter("p_id", 1, DbType.Int32, ParameterDirection.Output));

        Assert.Equal(0, command.ExecuteNonQuery());
    }

    /// <summary>
    /// EN: Verifies Dapper can execute stored procedures through CommandType.StoredProcedure.
    /// PT: Verifica se o Dapper consegue executar stored procedures por meio de CommandType.StoredProcedure.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldWork()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        var affected = c.Execute(
            "sp_add_user",
            new { p_name = "John", p_email = "john@x.com" },
            commandType: CommandType.StoredProcedure);

        Assert.Equal(0, affected);
    }

    /// <summary>
    /// EN: Verifies Dapper stored procedure execution throws when a required parameter is missing.
    /// PT: Verifica se a execucao de stored procedure via Dapper lanca excecao quando falta um parametro obrigatorio.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureExecution")]
    public void DapperExecute_CommandTypeStoredProcedure_ShouldThrow_OnMissingParam()
    {
        using var c = CreateOpenConnection();
        c.AddProdecure("sp_add_user", new ProcedureDef(
            RequiredIn:
            [
                new ProcParam("p_name", DbType.String),
                new ProcParam("p_email", DbType.String),
            ],
            OptionalIn: [],
            OutParams: [],
            ReturnParam: null));

        var ex = Assert.Throws<SqliteMockException>(() =>
            c.Execute(
                "sp_add_user",
                new { p_name = "John" },
                commandType: CommandType.StoredProcedure));

        Assert.Equal(1318, ex.ErrorCode);
    }
}
