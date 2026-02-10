namespace DbSqlLikeMem.Oracle.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests StoredProcedure_ShouldValidateRequiredInAndOutParams behavior.
    /// PT: Testa o comportamento de StoredProcedure_ShouldValidateRequiredInAndOutParams.
    /// </summary>
    [Fact]
    public void StoredProcedure_ShouldValidateRequiredInAndOutParams()
    {
        using var c = new OracleConnectionMock();

        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [new ProcParam("note", DbType.String, Required: false)],
            OutParams: [new ProcParam("resultCode", DbType.Int32, Required: true)],
            ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0)
            ));

        using var cmd = new OracleCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_demo"
        };
        cmd.Parameters.Add(new OracleParameter { ParameterName = "tenantId", DbType = DbType.Int32, Value = 10 });
        cmd.Parameters.Add(new OracleParameter { ParameterName = "resultCode", DbType = DbType.Int32, Direction = ParameterDirection.Output, Value = DBNull.Value });

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(0, Convert.ToInt32(
            cmd.Parameters["resultCode"].Value,
            CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Tests StoredProcedure_ShouldThrowWhenMissingRequiredParam behavior.
    /// PT: Testa o comportamento de StoredProcedure_ShouldThrowWhenMissingRequiredParam.
    /// </summary>
    [Fact]
    public void StoredProcedure_ShouldThrowWhenMissingRequiredParam()
    {
        using var c = new OracleConnectionMock();
        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = new OracleCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_demo"
        };

        var ex = Assert.Throws<OracleMockException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1318, ex.ErrorCode);
    }

    /// <summary>
    /// EN: Tests CallStatement_ShouldValidateAgainstRegisteredProcedure behavior.
    /// PT: Testa o comportamento de CallStatement_ShouldValidateAgainstRegisteredProcedure.
    /// </summary>
    [Fact]
    public void CallStatement_ShouldValidateAgainstRegisteredProcedure()
    {
        using var c = new OracleConnectionMock();
        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = new OracleCommandMock(c)
        {
            CommandText = "CALL sp_demo(@tenantId)"
        };
        cmd.Parameters.Add(new OracleParameter { ParameterName = "tenantId", DbType = DbType.Int32, Value = 10 });

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
    }
}
