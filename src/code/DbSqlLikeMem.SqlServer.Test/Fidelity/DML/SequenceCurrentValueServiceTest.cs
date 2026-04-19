namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Reads SQL Server sequence current_value before and after generated values for fidelity coverage.
/// PT: Le o current_value da sequence SQL Server antes e depois de valores gerados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceCurrentValueServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the current sequence value before consumption and after each generated value.
    /// PT: Retorna o valor atual da sequence antes do consumo e depois de cada valor gerado.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        var currentBefore = await ExecuteScalarLongAsync(Repo.Dialect.CurrentSequenceValue(Context));
        var first = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
        var currentAfterFirst = await ExecuteScalarLongAsync(Repo.Dialect.CurrentSequenceValue(Context));
        var second = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
        var currentAfterSecond = await ExecuteScalarLongAsync(Repo.Dialect.CurrentSequenceValue(Context));

        return new[] { currentBefore, first, currentAfterFirst, second, currentAfterSecond };
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
