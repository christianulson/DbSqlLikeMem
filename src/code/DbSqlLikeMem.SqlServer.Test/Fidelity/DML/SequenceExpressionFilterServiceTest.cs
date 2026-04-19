namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Reads a SQL Server sequence value inside a filtered query over the users table for fidelity coverage.
/// PT: Le um valor de sequence SQL Server dentro de uma consulta filtrada na tabela de usuarios para cobertura de fidelidade.
/// </summary>
public sealed class SequenceExpressionFilterServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the generated values from a filtered NEXT VALUE FOR query executed twice.
    /// PT: Retorna os valores gerados de uma consulta filtrada com NEXT VALUE FOR executada duas vezes.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        var first = await ExecuteScalarLongAsync($"SELECT NEXT VALUE FOR {Context.Seq} FROM {Context.TbUsersFullName} WHERE Id = 1");
        var second = await ExecuteScalarLongAsync($"SELECT NEXT VALUE FOR {Context.Seq} FROM {Context.TbUsersFullName} WHERE Id = 1");
        return new[] { first, second };
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
