namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Changes a SQL Server sequence increment and reads the generated values for fidelity coverage.
/// PT: Altera o incremento de uma sequence SQL Server e le os valores gerados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceIncrementByServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the sequence values before and after changing the increment size.
    /// PT: Retorna os valores da sequence antes e depois de alterar o tamanho do incremento.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceIncrementByAsync();

    /// <summary>
    /// EN: Returns the sequence values before and after changing the increment size.
    /// PT: Retorna os valores da sequence antes e depois de alterar o tamanho do incremento.
    /// </summary>
    public async Task<long[]> RunSequenceIncrementByAsync()
    {
        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 10 INCREMENT BY 1");
        try
        {
            var first = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));

            await ExecuteNonQueryAsync($"ALTER SEQUENCE {Context.Seq} INCREMENT BY 3");

            var second = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
            var third = await ExecuteScalarLongAsync(Repo.Dialect.NextSequenceValue(Context));
            return new[] { first, second, third };
        }
        finally
        {
            await ExecuteNonQueryAsync(Repo.Dialect.DropSequence(Context));
        }
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
