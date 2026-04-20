namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a PostgreSQL sequence with a lower bound and verifies the minimum value is enforced.
/// PT: Cria uma sequence PostgreSQL com limite inferior e verifica se o valor minimo e imposto.
/// </summary>
public sealed class SequenceMinValueServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the generated values and whether the sequence stopped at the configured minimum.
    /// PT: Retorna os valores gerados e se a sequence parou no minimo configurado.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 5 INCREMENT BY -2 MINVALUE 1 MAXVALUE 5 NO CYCLE");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var third = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");

        var stoppedAtMinimum = false;
        try
        {
            await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("minimum value", StringComparison.OrdinalIgnoreCase))
        {
            stoppedAtMinimum = true;
        }

        await ExecuteNonQueryAsync($"DROP SEQUENCE {Context.Seq}");
        return new[] { first, second, third, stoppedAtMinimum ? 1L : 0L };
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
