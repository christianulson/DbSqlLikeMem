namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Applies setval with is_called false and reads the session-local sequence state for PostgreSQL fidelity.
/// PT: Aplica setval com is_called false e le o estado local da sessao para fidelidade de PostgreSQL.
/// </summary>
public sealed class SequenceSetValServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the observed sequence values before and after setval without calling nextval again first.
    /// PT: Retorna os valores observados da sequence antes e depois de setval sem chamar nextval novamente antes.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var initial = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var lastAfterInitial = await ExecuteScalarLongAsync("SELECT lastval()");
        var setResult = await ExecuteScalarLongAsync($"SELECT setval('{Context.Seq}', 40, false)");
        var lastAfterSet = await ExecuteScalarLongAsync("SELECT lastval()");
        var nextAfterSet = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        var lastAfterNext = await ExecuteScalarLongAsync("SELECT lastval()");
        return new[] { initial, lastAfterInitial, setResult, lastAfterSet, nextAfterSet, lastAfterNext };
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
