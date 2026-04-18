using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertParallelUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts the requested number of user rows in parallel and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario em paralelo e valida a contagem final.
    /// </summary>
    /// <param name="args">EN: The row count and optional start id. PT: A contagem de linhas e o id inicial opcional.</param>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var rowCount = (int)args[0];
        var startId = args.Length > 1 ? (int)args[1] : 1;
        var expectedCount = args.Length > 2 ? (int)args[2] : rowCount;

        var tasks = Enumerable.Range(0, rowCount)
            .Select(async offset =>
            {
                var id = startId + offset;
                using var parallelRepo = Repo.Clone();
                await ExecuteParameterizedInsertOnConnectionAsync(parallelRepo, id);
            })
            .ToArray();

        await Task.WhenAll(tasks);

        var lst = await Repo.ExecuteReaderAsync(Repo.Dialect.SelectAll(Context.TbUsersFullName));

        if (lst.Count != 1
            || lst[0].Count != expectedCount)
            throw new InvalidOperationException($"Expected {expectedCount} rows for {Repo.Dialect.DisplayName}, got {JsonSerializer.Serialize(lst)}.");

        return lst[0].Count;
    }

    private Task<int> ExecuteParameterizedInsertOnConnectionAsync(
        RepoService parallelRepo,
        int id)
        => parallelRepo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, $"User-{id}"));
}
