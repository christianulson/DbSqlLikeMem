namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Service that tests upsert operations by inserting a row and then updating it with a second upsert.
/// PT-br: Serviço que testa operações de upsert, inserindo uma linha e depois atualizando-a com um segundo upsert.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationUpsertInsertThenUpdateServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row with upsert and then updates the same row with a second upsert.
    /// PT-br: Insere uma linha com upsert e depois atualiza a mesma linha com um segundo upsert.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        if (!Repo.Dialect.SupportsUpsert)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the upsert benchmark.");
        }

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Upsert(Context, 1, "Alice"));

        var inserted = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1)), CultureInfo.InvariantCulture);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.Upsert(Context, 1, "Alice-v2"));

        var updated = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, 1)), CultureInfo.InvariantCulture);
        GC.KeepAlive(inserted);

        return updated;
    }
}
