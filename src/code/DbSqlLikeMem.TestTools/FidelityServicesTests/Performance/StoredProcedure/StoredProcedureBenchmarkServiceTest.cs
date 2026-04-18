namespace DbSqlLikeMem.TestTools.Performance;

/// <summary>
/// EN: Executes the benchmark stored-procedure workflow against the current provider.
/// PT: Executa o fluxo de benchmark de procedimento armazenado contra o provedor atual.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context for the current benchmark run. PT: Contexto do cenario para a execucao atual do benchmark.</param>
public class StoredProcedureBenchmarkServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Registers a benchmark procedure on mock connections and executes it with input and output parameters.
    /// PT: Registra um procedimento de benchmark em conexoes mock e o executa com parametros de entrada e saida.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        if (Repo.Cnn is DbConnectionMockBase mockConnection)
        {
            var procedure = new ProcedureDef(
                "sp_benchmark",
                RequiredIn:
                [
                    new ProcParam("tenantId", DbType.Int32, Required: true)
                ],
                OptionalIn:
                [
                    new ProcParam("note", DbType.String, Required: false)
                ],
                OutParams:
                [
                    new ProcParam("counter", DbType.Int32, Required: true),
                    new ProcParam("message", DbType.String, Required: true)
                ],
                ReturnParam: new ProcParam("resultCode", DbType.Int32, Value: 0));

            mockConnection.AddProdecure(procedure);
        }

        using var command = Repo.Cnn.CreateCommand();
        command.CommandType = CommandType.StoredProcedure;
        command.CommandText = "sp_benchmark";

        Repo.Dialect.AddParameter(command, "tenantId", DbType.Int32, 10, ParameterDirection.Input);
        Repo.Dialect.AddParameter(command, "note", DbType.String, "benchmark", ParameterDirection.Input);
        Repo.Dialect.AddParameter(command, "counter", DbType.Int32, DBNull.Value, ParameterDirection.Output);
        Repo.Dialect.AddParameter(command, "message", DbType.String, DBNull.Value, ParameterDirection.Output);
        var resultCodeApplied = Repo.Dialect.AddParameter(command, "resultCode", DbType.Int32, DBNull.Value, ParameterDirection.ReturnValue);

        var affected = await command.ExecuteNonQueryAsync();
        GC.KeepAlive(command.Parameters["counter"].Value);
        GC.KeepAlive(command.Parameters["message"].Value);
        if (!resultCodeApplied)
        {
            command.Parameters["resultCode"].Value = 0;
        }

        GC.KeepAlive(command.Parameters["resultCode"].Value);
        return affected;
    }

}
