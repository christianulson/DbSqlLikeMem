namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes the MariaDB RETURNING insert workflow and validates the returned row count.
/// PT: Executa o fluxo INSERT RETURNING do MariaDB e valida a contagem de linhas retornadas.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchInsertReturningServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the MariaDB RETURNING insert workflow and validates the returned row count.
    /// PT: Executa o fluxo INSERT RETURNING do MariaDB e valida a contagem de linhas retornadas.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var d = Repo.Dialect;
        var rows = await Repo.ExecuteReaderAsync(d.InsertUserReturning(Context, 1, "Alice"));
        if (rows.Count != 1
            || rows[0].Count != 1)
        {
            throw new InvalidOperationException($"Unexpected RETURNING rowcount for {d.DisplayName}: {rows}.");
        }

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(d.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected RETURNING insert persistence for {d.DisplayName}: {count}.");
        }

        GC.KeepAlive(rows);
        GC.KeepAlive(count);
        return rows;
    }
}
