using System.Data.Common;

namespace DbSqlLikeMem.Test;

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
    /// PT: Cria uma conexão mock específica do provedor usada pelos testes de assinatura de procedure.
    /// </summary>
    protected abstract DbConnectionMockBase CreateConnection();

    /// <summary>
    /// EN: Tests StoredProcedure_ShouldValidateRequiredInAndOutParams behavior.
    /// PT: Testa o comportamento de StoredProcedure_ShouldValidateRequiredInAndOutParams.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldValidateRequiredInAndOutParams()
    {
        using var c = CreateConnection();

        c.AddProdecure("sp_demo", new ProcedureDef(
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
    /// EN: Tests StoredProcedure_ShouldThrowWhenMissingRequiredParam behavior.
    /// PT: Testa o comportamento de StoredProcedure_ShouldThrowWhenMissingRequiredParam.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void StoredProcedure_ShouldThrowWhenMissingRequiredParam()
    {
        using var c = CreateConnection();
        c.AddProdecure("sp_demo", new ProcedureDef(
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
    /// EN: Tests CallStatement_ShouldValidateAgainstRegisteredProcedure behavior.
    /// PT: Testa o comportamento de CallStatement_ShouldValidateAgainstRegisteredProcedure.
    /// </summary>
    [Fact]
    [Trait("Category", "StoredProcedureSignature")]
    public void CallStatement_ShouldValidateAgainstRegisteredProcedure()
    {
        using var c = CreateConnection();
        c.AddProdecure("sp_demo", new ProcedureDef(
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
            // Microsoft.Data.Sqlite does not support Output/ReturnValue directions.
            // Keep the default direction so shared signature tests can still run.
        }
    }
}
