namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a cycling PostgreSQL sequence and reads the wrapped values for fidelity coverage.
/// PT: Cria uma sequence PostgreSQL com ciclo e le os valores reiniciados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceCycleServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the sequence values observed before and after the cycle wraps.
    /// PT: Retorna os valores da sequence observados antes e depois da retomada do ciclo.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceCycleAsync();

    /// <summary>
    /// EN: Returns the sequence values observed before and after the cycle wraps.
    /// PT: Retorna os valores da sequence observados antes e depois da retomada do ciclo.
    /// </summary>
    public async Task<long[]> RunSequenceCycleAsync()
    {
        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 1 INCREMENT BY 1 MINVALUE 1 MAXVALUE 2 CYCLE");
        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var third = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        await ExecuteNonQueryAsync($"DROP SEQUENCE {Context.Seq}");
        return new[] { first, second, third };
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
