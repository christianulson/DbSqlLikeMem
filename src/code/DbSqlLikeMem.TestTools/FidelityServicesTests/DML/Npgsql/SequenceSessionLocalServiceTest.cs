namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Reads PostgreSQL session-local sequence values from two independent connections.
/// PT: Lê valores de sequence locais da sessão PostgreSQL a partir de duas conexões independentes.
/// </summary>
public sealed class SequenceSessionLocalServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the observed session-local sequence values for currval or lastval across two connections.
    /// PT: Retorna os valores observados de sequence locais da sessao para currval ou lastval em duas conexoes.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceSessionLocalAsync(args.Length > 0 && args[0] is bool flag && flag);

    /// <summary>
    /// EN: Returns the observed session-local sequence values for currval or lastval across two connections.
    /// PT: Retorna os valores observados de sequence locais da sessao para currval ou lastval em duas conexoes.
    /// </summary>
    public async Task<long[]> RunSequenceSessionLocalAsync(bool useLastVal)
    {
        await EnsureOpenAsync(Repo);
        using var second = Repo.CloneWithSharedDatabase();
        await EnsureOpenAsync(second);

        return useLastVal
            ? await RunLastValAsync(second)
            : await RunCurrValAsync(second);
    }

    private async Task<long[]> RunCurrValAsync(RepoService second)
    {
        var firstMissing = await CaptureSessionMissingAsync(Repo, $"SELECT currval('{Context.Seq}')");
        var firstNext = await ExecuteScalarLongAsync(Repo, $"SELECT nextval('{Context.Seq}')");
        var firstCurr = await ExecuteScalarLongAsync(Repo, $"SELECT currval('{Context.Seq}')");
        var secondMissing = await CaptureSessionMissingAsync(second, $"SELECT currval('{Context.Seq}')");
        var secondNext = await ExecuteScalarLongAsync(second, $"SELECT nextval('{Context.Seq}')");
        var secondCurr = await ExecuteScalarLongAsync(second, $"SELECT currval('{Context.Seq}')");
        var firstCurrAfterSecond = await ExecuteScalarLongAsync(Repo, $"SELECT currval('{Context.Seq}')");

        return new[] { firstMissing, firstNext, firstCurr, secondMissing, secondNext, secondCurr, firstCurrAfterSecond };
    }

    private async Task<long[]> RunLastValAsync(RepoService second)
    {
        var firstMissing = await CaptureSessionMissingAsync(Repo, "SELECT lastval()");
        var firstNext = await ExecuteScalarLongAsync(Repo, $"SELECT nextval('{Context.Seq}')");
        var firstLast = await ExecuteScalarLongAsync(Repo, "SELECT lastval()");
        var secondMissing = await CaptureSessionMissingAsync(second, "SELECT lastval()");
        var secondNext = await ExecuteScalarLongAsync(second, $"SELECT nextval('{Context.Seq}')");
        var secondLast = await ExecuteScalarLongAsync(second, "SELECT lastval()");
        var firstLastAfterSecond = await ExecuteScalarLongAsync(Repo, "SELECT lastval()");

        return new[] { firstMissing, firstNext, firstLast, secondMissing, secondNext, secondLast, firstLastAfterSecond };
    }

    private static async Task<long> ExecuteScalarLongAsync(RepoService repo, string sql)
    {
        using var command = repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task<long> CaptureSessionMissingAsync(RepoService repo, string sql)
    {
        try
        {
            _ = await ExecuteScalarLongAsync(repo, sql);
            return 0L;
        }
        catch (Exception ex)
        {
            var message = ex.Message;
            var isSessionMissing =
                ex is InvalidOperationException
                || (ex.GetType().Name == "PostgresException" && (
                    message.Contains("not yet defined", StringComparison.OrdinalIgnoreCase)
                    || message.Contains("Sequence not found", StringComparison.OrdinalIgnoreCase)));

            if (isSessionMissing)
            {
                return 1L;
            }

            throw;
        }
    }

    private static async Task EnsureOpenAsync(RepoService repo)
    {
        if (repo.Cnn.State == ConnectionState.Open)
            return;

        await repo.Cnn.OpenAsync();
    }
}
