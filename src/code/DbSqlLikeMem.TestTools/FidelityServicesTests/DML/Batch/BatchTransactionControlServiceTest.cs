namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes a batch inside a transaction and keeps the table name alive after commit.
/// PT: Executa um lote dentro de uma transação e mantém o nome da tabela vivo após o commit.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class BatchTransactionControlServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Executes a batch inside a transaction and keeps the table name alive after commit.
    /// PT: Executa um lote dentro de uma transação e mantém o nome da tabela vivo após o commit.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        using var tx = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 1, "Alice"), tx);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, 2, "Bob"), tx);
        tx.Commit();
        GC.KeepAlive(Context.TbUsersFullName);
        return Context.TbUsersFullName;
    }
}
