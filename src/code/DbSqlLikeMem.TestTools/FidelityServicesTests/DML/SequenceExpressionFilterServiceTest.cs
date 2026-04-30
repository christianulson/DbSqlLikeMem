namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Reads sequence values inside a filtered query over the users table for fidelity coverage.
/// PT: Le valores de sequence dentro de uma consulta filtrada na tabela de usuarios para cobertura de fidelidade.
/// </summary>
public sealed class SequenceExpressionFilterServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the generated values from a filtered sequence query executed twice.
    /// PT: Retorna os valores gerados de uma consulta filtrada com sequence executada duas vezes.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        var nextSequenceValueExpression = Repo.Dialect.NextSequenceValueExpression(Context);
        var first = await ExecuteScalarLongAsync($"SELECT {nextSequenceValueExpression} FROM {Context.TbUsersFullName} WHERE Id = 1");
        var second = await ExecuteScalarLongAsync($"SELECT {nextSequenceValueExpression} FROM {Context.TbUsersFullName} WHERE Id = 1");
        return new[] { first, second };
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
