namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates a PostgreSQL sequence that counts downward and reads the generated values for fidelity coverage.
/// PT: Cria uma sequence PostgreSQL que conta para baixo e le os valores gerados para cobertura de fidelidade.
/// </summary>
public sealed class SequenceDescendingServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the values produced by a descending sequence before cleanup.
    /// PT: Retorna os valores produzidos por uma sequence descendente antes da limpeza.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        _ = args;

        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 5 INCREMENT BY -2 MINVALUE 1 MAXVALUE 5 NO CYCLE");
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
