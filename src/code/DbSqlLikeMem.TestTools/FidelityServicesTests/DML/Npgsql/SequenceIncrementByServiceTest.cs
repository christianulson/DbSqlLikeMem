namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Changes the PostgreSQL sequence increment and reads the next generated values for fidelity coverage.
/// PT: Altera o incremento da sequence PostgreSQL e le os proximos valores gerados para cobertura de fidelidade.
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
        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");

        using (var alter = Repo.Cnn.CreateCommand())
        {
            alter.CommandText = $"ALTER SEQUENCE {Context.Seq} INCREMENT BY 3";
            await alter.ExecuteNonQueryAsync();
        }

        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var third = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
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
