namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Executes shared insert workflows and validates row counts for the current provider.
/// PT-br: Executa fluxos compartilhados de insert e valida contagens de linhas para o provedor atual.
/// </summary>
public class InsertRowCountUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a single user row and returns the affected-row count.
    /// PT-br: Insere uma linha de usuario e retorna a contagem de linhas afetadas.
    /// </summary>
    /// <param name="args">EN: Optional insert id, or the legacy users table name and scenario token followed by an optional id. PT-br: Id de insert opcional, ou o nome da tabela de usuarios e o token do cenario seguidos por um id opcional.</param>
    /// <returns>EN: The affected-row count reported by the provider. PT-br: A contagem de linhas afetadas informada pelo provedor.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length switch
        {
            > 2 => (int)args[2],
            > 0 when args[0] is int directId => directId,
            _ => 1
        };
        var affected = await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUser(Context, id, "Alice"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }
}
