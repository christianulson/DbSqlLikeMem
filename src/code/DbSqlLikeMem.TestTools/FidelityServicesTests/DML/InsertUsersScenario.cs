namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the users table used by insert workflows.
/// PT-br: Cria e remove a tabela de usuarios usada pelos fluxos de insert.
/// </summary>
public class InsertUsersScenario(
    RepoService repo,
    FidelityTestContext context
) : BaseScenario(repo, context),
    ITestScenario
{
    /// <summary>
    /// EN: Creates the users table required by the insert workflow.
    /// PT-br: Cria a tabela de usuarios exigida pelo fluxo de insert.
    /// </summary>
    public Task CreateScenarioAsync()
    => Repo.ExecuteNonQueryAsync(Repo.Dialect.CreateUsersTable(Context));

    /// <summary>
    /// EN: Drops the users table created for the insert workflow.
    /// PT-br: Remove a tabela de usuarios criada para o fluxo de insert.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
        }
        catch (Exception ex) when (ShouldIgnoreDropException(ex))
        {
        }
    }

    private static bool ShouldIgnoreDropException(Exception ex)
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
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase)
            || message.Contains("lock conflict on no wait transaction", StringComparison.OrdinalIgnoreCase)
            || message.Contains("is in use", StringComparison.OrdinalIgnoreCase);
    }
}
