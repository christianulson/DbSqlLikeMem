namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Executes the table-drop command for DDL scenarios.
/// PT-br: Executa o comando de remocao de tabela para cenarios DDL.
/// </summary>
public class DropTableServiceTest(
       RepoService repo,
       FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Drops the requested table for the current provider.
    /// PT-br: Remove a tabela solicitada para o provedor atual.
    /// </summary>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var sql = Repo.Dialect.DropTable(Context.TbUsersFullName);
        return await Repo.ExecuteNonQueryAsync(sql);
    }
}
