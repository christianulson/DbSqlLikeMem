namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Shared stored procedure signature validation tests executed by provider-specific derived classes.
/// PT: Testes compartilhados de validação de assinatura de procedures executados por classes derivadas de cada provedor.
/// </summary>
public abstract class StoredProcedureSignatureTestsBase<TSqlMockException>(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
    where TSqlMockException : SqlMockException
{
    /// <summary>
    /// EN: Creates a provider-specific mock connection used by stored procedure signature tests.
    /// PT: Cria uma conexão simulada específica do provedor usada pelos testes de assinatura de procedure.
    /// </summary>
    protected abstract DbConnectionMockBase CreateConnection();

    /// <summary>
    /// EN: Verifies stored procedure calls validate required input and output parameters.
    /// PT: Verifica se chamadas de procedure validam parametros de entrada e saida obrigatorios.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldValidateRequiredInAndOutParams()
    {
        using var c = CreateConnection();

        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [new ProcParam("note", DbType.String, Required: false)],
            OutParams: [new ProcParam("resultCode", DbType.Int32, Required: true)],
            ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0)
            ));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.Output);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(0, Convert.ToInt32(
            cmd.Parameters["resultCode"].Value,
            CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls populate mixed typed output parameters and return values.
    /// PT: Verifica se chamadas de procedure preenchem parametros de saida de tipos mistos e valores de retorno.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateMixedTypedOutParams()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [new ProcParam("note", DbType.String, Required: false)],
            OutParams:
            [
                new ProcParam("resultCode", DbType.Int32, Required: true),
                new ProcParam("token", DbType.Guid, Required: true),
                new ProcParam("isActive", DbType.Boolean, Required: true),
                new ProcParam("expiresAt", DbType.DateTime, Required: true),
                new ProcParam("message", DbType.String, Required: true)
            ],
            ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0)
            ));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "note", DbType.String, "hello", ParameterDirection.Input);
        AddParameter(cmd, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "token", DbType.Guid, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "isActive", DbType.Boolean, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "expiresAt", DbType.DateTime, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "message", DbType.String, DBNull.Value, ParameterDirection.Output);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(0, Convert.ToInt32(cmd.Parameters["resultCode"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(Guid.Empty, cmd.Parameters["token"].Value);
        Assert.False(Convert.ToBoolean(cmd.Parameters["isActive"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(DateTime.MinValue, Convert.ToDateTime(cmd.Parameters["expiresAt"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(string.Empty, Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep input-output parameters readable and writable.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output legiveis e gravaveis.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("counter", DbType.Int32, Required: true)
            ],
            OptionalIn:
            [
                new ProcParam("note", DbType.String, Required: false)
            ],
            OutParams:
            [
                new ProcParam("counter", DbType.Int32, Required: true),
                new ProcParam("resultCode", DbType.Int32, Required: true),
                new ProcParam("message", DbType.String, Required: true)
            ],
            ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0)
            ));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "counter", DbType.Int32, 41, ParameterDirection.InputOutput);
        AddParameter(cmd, "note", DbType.String, "hello", ParameterDirection.Input);
        AddParameter(cmd, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "message", DbType.String, DBNull.Value, ParameterDirection.Output);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(41, Convert.ToInt32(cmd.Parameters["counter"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(0, Convert.ToInt32(cmd.Parameters["resultCode"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(string.Empty, Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep temporal input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output temporais inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateTemporalInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("effectiveAt", DbType.DateTime, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("effectiveAt", DbType.DateTime, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        var effectiveAt = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "effectiveAt", DbType.DateTime, effectiveAt, ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(effectiveAt, Convert.ToDateTime(cmd.Parameters["effectiveAt"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep GUID input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output GUID inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateGuidInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("token", DbType.Guid, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("token", DbType.Guid, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        var token = Guid.Parse("11111111-2222-3333-4444-555555555555");
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "token", DbType.Guid, token, ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(token, cmd.Parameters["token"].Value);
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep boolean input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output booleanos inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateBooleanInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("isActive", DbType.Boolean, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("isActive", DbType.Boolean, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "isActive", DbType.Boolean, true, ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.True(Convert.ToBoolean(cmd.Parameters["isActive"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep decimal input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output decimais inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateDecimalInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("balance", DbType.Decimal, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("balance", DbType.Decimal, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        const decimal balance = 123.45m;
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "balance", DbType.Decimal, balance, ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(balance, Convert.ToDecimal(cmd.Parameters["balance"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep string input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output de texto inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateStringInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("message", DbType.String, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("message", DbType.String, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "message", DbType.String, "hello", ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal("hello", Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep DateTimeOffset input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output DateTimeOffset inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateDateTimeOffsetInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("scheduledAt", DbType.DateTimeOffset, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("scheduledAt", DbType.DateTimeOffset, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        var scheduledAt = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "scheduledAt", DbType.DateTimeOffset, scheduledAt, ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(scheduledAt, cmd.Parameters["scheduledAt"].Value);
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep fixed-length text input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output de texto de comprimento fixo inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateFixedLengthTextInputOutputParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("tag", DbType.StringFixedLength, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("tag", DbType.StringFixedLength, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "tag", DbType.StringFixedLength, "ABCDE", ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal("ABCDE", Convert.ToString(cmd.Parameters["tag"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep numeric, ANSI text, and binary input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output numericos, texto ANSI e binarios inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateNumericAnsiBinaryInputOutputParams()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn:
            [
                new ProcParam("tenantId", DbType.Int32, Required: true),
                new ProcParam("bigCounter", DbType.Int64, Required: true),
                new ProcParam("ratio", DbType.Double, Required: true),
                new ProcParam("payload", DbType.Binary, Required: true),
                new ProcParam("label", DbType.AnsiString, Required: true)
            ],
            OptionalIn: [],
            OutParams:
            [
                new ProcParam("bigCounter", DbType.Int64, Required: true),
                new ProcParam("ratio", DbType.Double, Required: true),
                new ProcParam("payload", DbType.Binary, Required: true),
                new ProcParam("label", DbType.AnsiString, Required: true)
            ],
            ReturnParam: null));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        var payload = new byte[] { 1, 2, 3, 4 };
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        AddParameter(cmd, "bigCounter", DbType.Int64, 1234567890123L, ParameterDirection.InputOutput);
        AddParameter(cmd, "ratio", DbType.Double, 12.5d, ParameterDirection.InputOutput);
        AddParameter(cmd, "payload", DbType.Binary, payload, ParameterDirection.InputOutput);
        AddParameter(cmd, "label", DbType.AnsiString, "ansi", ParameterDirection.InputOutput);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(1234567890123L, Convert.ToInt64(cmd.Parameters["bigCounter"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(12.5d, Convert.ToDouble(cmd.Parameters["ratio"].Value, CultureInfo.InvariantCulture));
        Assert.Equal(payload, Assert.IsType<byte[]>(cmd.Parameters["payload"].Value));
        Assert.Equal("ansi", Convert.ToString(cmd.Parameters["label"].Value, CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Verifies stored procedure calls fail when a required parameter is missing.
    /// PT: Verifica se chamadas de procedure falham quando um parametro obrigatorio esta ausente.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldThrowWhenMissingRequiredParam()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = c.CreateCommand();
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.CommandText = "sp_demo";

        var ex = Assert.Throws<TSqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1318, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Verifies CALL statements are validated against the registered procedure definition.
    /// PT: Verifica se instrucoes CALL sao validadas contra a definicao da procedure registrada.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void CallStatement_ShouldValidateAgainstRegisteredProcedure()
    {
        using var c = CreateConnection();
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = c.CreateCommand();
        cmd.CommandText = "CALL sp_demo(@tenantId)";
        AddParameter(cmd, "tenantId", DbType.Int32, 10, ParameterDirection.Input);

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
    }

    private static void AddParameter(
        DbCommand cmd,
        string name,
        DbType dbType,
        object? value,
        ParameterDirection direction)
    {
        var parameter = cmd.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = dbType;
        TrySetDirection(parameter, direction);
        parameter.Value = value;
        cmd.Parameters.Add(parameter);
    }

    private static void TrySetDirection(DbParameter parameter, ParameterDirection direction)
    {
        try
        {
            parameter.Direction = direction;
        }
        catch (ArgumentException) when (parameter.GetType().FullName == "Microsoft.Data.Sqlite.SqliteParameter")
        {
            // Microsoft.Data.Sqlite does not support non-input directions.
            // Keep the default direction so shared signature tests can still run.
        }
    }
}

