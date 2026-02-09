namespace DbSqlLikeMem.Npgsql.Test;

public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Fact]
    public void StoredProcedure_ShouldValidateRequiredInAndOutParams()
    {
        using var c = new NpgsqlConnectionMock();

        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [new ProcParam("note", DbType.String, Required: false)],
            OutParams: [new ProcParam("resultCode", DbType.Int32, Required: true)],
            ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0)
            ));

        using var cmd = new NpgsqlCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_demo"
        };
        cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "tenantId", DbType = DbType.Int32, Value = 10 });
        cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "resultCode", DbType = DbType.Int32, Direction = ParameterDirection.Output, Value = DBNull.Value });

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
        Assert.Equal(0, Convert.ToInt32(
            cmd.Parameters["resultCode"].Value,
            CultureInfo.InvariantCulture));
    }

    [Fact]
    public void StoredProcedure_ShouldThrowWhenMissingRequiredParam()
    {
        using var c = new NpgsqlConnectionMock();
        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = new NpgsqlCommandMock(c)
        {
            CommandType = CommandType.StoredProcedure,
            CommandText = "sp_demo"
        };

        var ex = Assert.Throws<NpgsqlMockException>(() => cmd.ExecuteNonQuery());
        Assert.Equal(1318, ex.ErrorCode);
    }

    [Fact]
    public void CallStatement_ShouldValidateAgainstRegisteredProcedure()
    {
        using var c = new NpgsqlConnectionMock();
        c.AddProdecure("sp_demo", new ProcedureDef(
            RequiredIn: [new ProcParam("tenantId", DbType.Int32, Required: true)],
            OptionalIn: [],
            OutParams: []));

        using var cmd = new NpgsqlCommandMock(c)
        {
            CommandText = "CALL sp_demo(@tenantId)"
        };
        cmd.Parameters.Add(new NpgsqlParameter { ParameterName = "tenantId", DbType = DbType.Int32, Value = 10 });

        var n = cmd.ExecuteNonQuery();
        Assert.Equal(0, n);
    }
}
