namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Drops a PostgreSQL sequence twice with IF EXISTS and verifies the command stays idempotent.
/// PT: Remove uma sequence PostgreSQL duas vezes com IF EXISTS e verifica se o comando continua idempotente.
/// </summary>
public sealed class SequenceDropIfExistsServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the first generated sequence value and whether the second IF EXISTS drop completed.
    /// PT: Retorna o primeiro valor gerado da sequence e se a segunda remocao IF EXISTS foi concluida.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        await ExecuteNonQueryAsync($"DROP SEQUENCE IF EXISTS {Context.Seq}");
        await ExecuteNonQueryAsync($"DROP SEQUENCE IF EXISTS {Context.Seq}");
        return new[] { first, 1L };
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
