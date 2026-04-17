namespace DbSqlLikeMem.TestTools.TemporaryTable;

/// <summary>
/// EN: Creates and drops the temporary users table used by rollback and isolation workflows.
/// PT: Cria e remove a tabela temporaria de usuarios usada pelos fluxos de rollback e isolamento.
/// </summary>
public class TemporaryUsersScenario(
        RepoService repo,
        FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <summary>
    /// EN: Creates the temporary users table required by the workflow.
    /// PT: Cria a tabela temporaria de usuarios exigida pelo fluxo.
    /// </summary>
    public virtual Task CreateScenarioAsync()
    {
        return Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateTemporaryUsersTable(Context));
    }

    /// <summary>
    /// EN: Drops the temporary users table created for the workflow.
    /// PT: Remove a tabela temporaria de usuarios criada para o fluxo.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTemporaryUsersTable(Context));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {

        }
    }

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
