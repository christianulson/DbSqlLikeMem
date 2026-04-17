namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertRowCountUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a single user row and returns the affected-row count.
    /// PT: Insere uma linha de usuario e retorna a contagem de linhas afetadas.
    /// </summary>
    /// <param name="args">EN: The users table name, scenario token, and optional insert id. PT: O nome da tabela de usuarios, o token do cenario e o id de insert opcional.</param>
    /// <returns>EN: The affected-row count reported by the provider. PT: A contagem de linhas afetadas informada pelo provedor.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 2 ? (int)args[2] : 1;
        var affected = await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, "Alice"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }
}
