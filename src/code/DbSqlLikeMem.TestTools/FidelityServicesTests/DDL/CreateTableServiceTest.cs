namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Executes the users-table creation command for DDL scenarios.
/// PT: Executa o comando de criacao da tabela de usuarios para cenarios DDL.
/// </summary>
public class CreateTableServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Creates the users table used by the scenario.
    /// PT: Cria a tabela de usuarios usada pelo cenario.
    /// </summary>
    /// <param name="args"></param>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var sql = Repo.Dialect.CreateUsersTable(Context);
        return await Repo.ExecuteNonQueryAsync(sql);
    }
}
