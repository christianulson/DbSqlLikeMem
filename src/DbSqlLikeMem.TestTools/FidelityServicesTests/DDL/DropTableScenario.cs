namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Drops the users table used by the cleanup scenario.
/// PT: Remove a tabela de usuarios usada pelo cenario de limpeza.
/// </summary>
public class DropTableScenario<T>
    : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// EN: Seeds the users table so the drop scenario has a table to remove.
    /// PT: Preenche a tabela de usuarios para que o cenario de remocao tenha uma tabela para excluir.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.CreateUsersTable((string)pars[0], (string)pars[2]));
    }

    /// <summary>
    /// EN: Leaves the drop step empty because the cleanup scenario does not need extra setup.
    /// PT: Deixa a etapa de remocao vazia porque o cenario de limpeza nao precisa de preparacao extra.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {

    }
}
