namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Creates the users table before creating the related orders table with a foreign key.
/// PT: Cria a tabela de usuarios antes de criar a tabela de pedidos relacionada com chave estrangeira.
/// </summary>
public class CreateTableWithFKScenario<T>
     : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// EN: Seeds the parent users table required by the foreign-key scenario.
    /// PT: Preenche a tabela pai de usuarios exigida pelo cenario de chave estrangeira.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.CreateUsersTable((string)pars[0], (string)pars[1]));
    }

    /// <summary>
    /// EN: Drops the related orders table before removing the parent users table.
    /// PT: Remove a tabela relacionada de pedidos antes de remover a tabela pai de usuarios.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[1],(string)pars[2]));
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[0], (string)pars[2]));
    }
}
