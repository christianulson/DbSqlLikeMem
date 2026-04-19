namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a bounded PostgreSQL sequence and verifies the maximum value is enforced.
/// PT: Cria uma sequence PostgreSQL limitada e verifica se o valor maximo e imposto.
/// </summary>
public sealed class SequenceMaxValueServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the generated values and whether the sequence stopped at the configured maximum.
    /// PT: Retorna os valores gerados e se a sequence parou no maximo configurado.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 5 INCREMENT BY 1 MINVALUE 5 MAXVALUE 7 NO CYCLE");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var third = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");

        var stoppedAtMaximum = false;
        try
        {
            await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("maximum value", StringComparison.OrdinalIgnoreCase))
        {
            stoppedAtMaximum = true;
        }

        await ExecuteNonQueryAsync($"DROP SEQUENCE {Context.Seq}");
        return new[] { first, second, third, stoppedAtMaximum ? 1L : 0L };
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
