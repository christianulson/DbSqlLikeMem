namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes a batch inside a transaction and keeps the table name alive after commit.
/// PT-br: Executa um lote dentro de uma transação e mantém o nome da tabela vivo após o commit.
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
    /// PT-br: Executa um lote dentro de uma transação e mantém o nome da tabela vivo após o commit.
    /// </summary>
    /// <param name="args">EN: Optional user ids for the inserted rows. PT-br: Ids opcionais de usuario para as linhas inseridas.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstUserId = args.Length > 0 ? (int)args[0] : 1;
        var secondUserId = args.Length > 1 ? (int)args[1] : 2;

        using var tx = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, firstUserId, "Alice"), tx);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, secondUserId, "Bob"), tx);
        tx.Commit();
        GC.KeepAlive(Context.TbUsersFullName);
        return Context.TbUsersFullName;
    }
}
