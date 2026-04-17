using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts the requested number of user rows and validates the final count.
    /// PT: Insere a quantidade solicitada de linhas de usuario e valida a contagem final.
    /// </summary>
    /// <returns>EN: The final row count. PT: A contagem final de linhas.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var rowCount = (int)args[0];
        var startId = args.Length > 1 ? (int)args[1] : 1;
        var expectedCount = args.Length > 2 ? (int)args[2] : rowCount;
        await InsertSequentialRows(rowCount, startId);

        var lst = await Repo.ExecuteReaderAsync(Repo.Dialect.SelectAll(Context.TbUsersFullName));

        if (lst.Count != 1
            || lst[0].Count != expectedCount)
            throw new InvalidOperationException($"Expected {expectedCount} rows for {Repo.Dialect.DisplayName}, got {JsonSerializer.Serialize(lst)}.");

        return lst;
    }

    private async Task InsertSequentialRows(int rowCount, int startId)
    {
        for (var i = 0; i < rowCount; i++)
        {
            var id = startId + i;
            var name = rowCount == 1 ? "Alice" : $"User-{id}";
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, name));
        }
    }
}
