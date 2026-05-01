namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a PostgreSQL sequence with IF NOT EXISTS and reads the generated values for fidelity coverage.
/// PT-br: Cria uma sequence PostgreSQL com IF NOT EXISTS e le os valores gerados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceCreateIfNotExistsServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the first two values produced after an idempotent sequence creation.
    /// PT-br: Retorna os dois primeiros valores produzidos apos uma criacao idempotente de sequence.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceCreateIfNotExistsAsync();

    /// <summary>
    /// EN: Returns the first two values produced after an idempotent sequence creation.
    /// PT-br: Retorna os dois primeiros valores produzidos apos uma criacao idempotente de sequence.
    /// </summary>
    public async Task<long[]> RunSequenceCreateIfNotExistsAsync()
    {
        await ExecuteNonQueryAsync($"CREATE SEQUENCE IF NOT EXISTS {Context.Seq} START WITH 13 INCREMENT BY 2");
        await ExecuteNonQueryAsync($"CREATE SEQUENCE IF NOT EXISTS {Context.Seq} START WITH 99 INCREMENT BY 9");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        return new[] { first, second };
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
