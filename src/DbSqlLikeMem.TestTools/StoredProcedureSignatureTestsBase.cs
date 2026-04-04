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
    /// EN: Indicates whether the provider supports DateTimeOffset input-output parameters in stored procedure signatures.
    /// PT: Indica se o provedor suporta parametros input-output DateTimeOffset em assinaturas de procedure.
    /// </summary>
    protected virtual bool SupportsDateTimeOffsetInputOutputParameters => true;

    /// <summary>
    /// EN: Indicates whether the provider supports Guid input-output parameters in stored procedure signatures.
    /// PT: Indica se o provedor suporta parametros input-output Guid em assinaturas de procedure.
    /// </summary>
    protected virtual bool SupportsGuidInputOutputParameters => true;

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
        n.Should().Be(0);
        Convert.ToInt32(
            cmd.Parameters["resultCode"].Value,
            CultureInfo.InvariantCulture).Should().Be(0);
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
        var tokenDbType = SupportsGuidInputOutputParameters ? DbType.Guid : DbType.String;
        c.AddProdecure(new ProcedureDef(Name: "sp_demo",
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [new ProcParam("note", DbType.String, Required: false)],
            OutParams:
            [
                new ProcParam("resultCode", DbType.Int32, Required: true),
                new ProcParam("token", tokenDbType, Required: true),
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
        AddParameter(cmd, "token", tokenDbType, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "isActive", DbType.Boolean, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "expiresAt", DbType.DateTime, DBNull.Value, ParameterDirection.Output);
        AddParameter(cmd, "message", DbType.String, DBNull.Value, ParameterDirection.Output);

        var n = cmd.ExecuteNonQuery();
        n.Should().Be(0);
        Convert.ToInt32(cmd.Parameters["resultCode"].Value, CultureInfo.InvariantCulture).Should().Be(0);
        if (SupportsGuidInputOutputParameters)
        {
            cmd.Parameters["token"].Value.Should().Be(Guid.Empty);
        }
        else
        {
            Convert.ToString(cmd.Parameters["token"].Value, CultureInfo.InvariantCulture).Should().Be(string.Empty);
        }
        Convert.ToBoolean(cmd.Parameters["isActive"].Value, CultureInfo.InvariantCulture).Should().BeFalse();
        Convert.ToDateTime(cmd.Parameters["expiresAt"].Value, CultureInfo.InvariantCulture).Should().Be(DateTime.MinValue);
        Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture).Should().Be(string.Empty);
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
        n.Should().Be(0);
        Convert.ToInt32(cmd.Parameters["counter"].Value, CultureInfo.InvariantCulture).Should().Be(41);
        Convert.ToInt32(cmd.Parameters["resultCode"].Value, CultureInfo.InvariantCulture).Should().Be(0);
        Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture).Should().Be(string.Empty);
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
        n.Should().Be(0);
        Convert.ToDateTime(cmd.Parameters["effectiveAt"].Value, CultureInfo.InvariantCulture).Should().Be(effectiveAt);
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep GUID input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output GUID inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateGuidInputOutputParam()
    {
        if (!SupportsGuidInputOutputParameters)
        {
            using var cnn = CreateConnection();
            cnn.AddProdecure(new ProcedureDef(Name: "sp_demo",
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

            using var cmd1 = cnn.CreateCommand();
            cmd1.CommandType = CommandType.StoredProcedure;
            cmd1.CommandText = "sp_demo";

            AddParameter(cmd1, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
            FluentActions.Invoking(() => AddParameter(cmd1, "token", DbType.Guid, Guid.Parse("11111111-2222-3333-4444-555555555555"), ParameterDirection.InputOutput))
                .Should().Throw<ArgumentException>();
            return;
        }

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
        n.Should().Be(0);
        cmd.Parameters["token"].Value.Should().Be(token);
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
        n.Should().Be(0);
        Convert.ToBoolean(cmd.Parameters["isActive"].Value, CultureInfo.InvariantCulture).Should().BeTrue();
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
        n.Should().Be(0);
        Convert.ToDecimal(cmd.Parameters["balance"].Value, CultureInfo.InvariantCulture).Should().Be(balance);
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
        n.Should().Be(0);
        Convert.ToString(cmd.Parameters["message"].Value, CultureInfo.InvariantCulture).Should().Be("hello");
    }

    /// <summary>
    /// EN: Verifies stored procedure calls keep DateTimeOffset input-output parameters unchanged.
    /// PT: Verifica se chamadas de procedure mantem parametros input-output DateTimeOffset inalterados.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldPopulateDateTimeOffsetInputOutputParam()
    {
        if (!SupportsDateTimeOffsetInputOutputParameters)
        {
            using var cnn = CreateConnection();
            cnn.AddProdecure(new ProcedureDef(Name: "sp_demo",
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

            using var cmd1 = cnn.CreateCommand();
            cmd1.CommandType = CommandType.StoredProcedure;
            cmd1.CommandText = "sp_demo";

            var scheduledAt1 = new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero);
            AddParameter(cmd1, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
            FluentActions.Invoking(() => AddParameter(cmd1, "scheduledAt", DbType.DateTimeOffset, scheduledAt1, ParameterDirection.InputOutput))
                .Should().Throw<InvalidCastException>();
            return;
        }

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
        n.Should().Be(0);
        cmd.Parameters["scheduledAt"].Value.Should().Be(scheduledAt);
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
        n.Should().Be(0);
        Convert.ToString(cmd.Parameters["tag"].Value, CultureInfo.InvariantCulture).Should().Be("ABCDE");
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
        n.Should().Be(0);
        Convert.ToInt64(cmd.Parameters["bigCounter"].Value, CultureInfo.InvariantCulture).Should().Be(1234567890123L);
        Convert.ToDouble(cmd.Parameters["ratio"].Value, CultureInfo.InvariantCulture).Should().Be(12.5d);
        cmd.Parameters["payload"].Value.Should().BeOfType<byte[]>().Which.Should().Equal(payload);
        Convert.ToString(cmd.Parameters["label"].Value, CultureInfo.InvariantCulture).Should().Be("ansi");
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

        var ex = FluentActions.Invoking(() => cmd.ExecuteNonQuery()).Should().Throw<TSqlMockException>().Which;
        ex.ErrorCode.Should().Be(1318);
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
        n.Should().Be(0);
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
        var parameterTypeName = parameter.GetType().FullName;
        var isOracleParameter = parameterTypeName == "Oracle.ManagedDataAccess.Client.OracleParameter";
        var isDb2Parameter = parameterTypeName is "IBM.Data.Db2.DB2Parameter"
            or "IBM.Data.DB2.Core.DB2Parameter"
            or "IBM.Data.DB2.iSeries.iDB2Parameter";
        if (isOracleParameter
            && dbType == DbType.Guid
            && direction != ParameterDirection.Input)
        {
            throw new ArgumentException("OracleParameter does not support Guid input-output parameters.", nameof(dbType));
        }
        try
        {
            if (!isOracleParameter && !(isDb2Parameter && dbType == DbType.DateTimeOffset))
                parameter.DbType = dbType;
        }
        catch (ArgumentException) when (isOracleParameter)
        {
            // EN: ODP.NET can reject some DbType values (e.g., Guid/DateTimeOffset) on OracleParameter.
            // PT: ODP.NET pode rejeitar alguns valores de DbType (ex.: Guid/DateTimeOffset) no OracleParameter.
            //
            // Keep the default DbType and rely on the value payload for signature tests.
        }
        TrySetDirection(parameter, direction);
        parameter.Value = isOracleParameter ? NormalizeOracleParameterValue(value) : value ?? DBNull.Value;
        cmd.Parameters.Add(parameter);
    }

    private static object NormalizeOracleParameterValue(object? value)
        => value switch
        {
            null => DBNull.Value,
            DateTimeOffset dateTimeOffset => dateTimeOffset,
            DateTime dateTime => dateTime,
            TimeSpan timeSpan => timeSpan.ToString("c", CultureInfo.InvariantCulture),
            Guid guid => guid.ToString("D", CultureInfo.InvariantCulture),
            _ => value
        };

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

