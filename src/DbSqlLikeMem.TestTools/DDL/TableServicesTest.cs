namespace DbSqlLikeMem.TestTools.DDL;

/// <summary>
/// EN: Creates the users table for DDL scenarios without an associated foreign key.
/// PT: Cria a tabela de usuarios para cenarios DDL sem chave estrangeira associada.
/// </summary>
public class CreateTableScenario<T> : ITestScenario<T>
     where T : DbConnection
{
    /// <summary>
    /// EN: Keeps the create-table scenario focused on the users table definition.
    /// PT: Mantem o cenario de create-table focado na definicao da tabela de usuarios.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
    { }

    /// <summary>
    /// EN: Drops the users table created by the scenario.
    /// PT: Remove a tabela de usuarios criada pelo cenario.
    /// </summary>
    /// <param name="service"></param>
    /// <param name="pars"></param>
    /// <exception cref="NotImplementedException"></exception>
    public virtual void DropScenario(BaseServiceTest<T> service, params object[] pars)
    {
        service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[0], (string)pars[1]));
    }
}

/// <summary>
/// EN: Executes the users-table creation command for DDL scenarios.
/// PT: Executa o comando de criacao da tabela de usuarios para cenarios DDL.
/// </summary>
public class CreateTableServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users table used by the scenario.
    /// PT: Cria a tabela de usuarios usada pelo cenario.
    /// </summary>
    /// <param name="pars"></param>
    public virtual void RunTest(params object[] pars)
    {
        var sql = Dialect.CreateUsersTable((string)pars[0], (string)pars[1]);
        ExecuteNonQuery(sql);
    }
}

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
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
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

/// <summary>
/// EN: Executes the orders-table creation command for the foreign-key scenario.
/// PT: Executa o comando de criacao da tabela de pedidos para o cenario de chave estrangeira.
/// </summary>
public class CreateTableWithFKServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the orders table that references the users table.
    /// PT: Cria a tabela de pedidos que referencia a tabela de usuarios.
    /// </summary>
    /// <param name="pars"></param>
    public void RunTest(params object[] pars)
    {
        ExecuteNonQuery(Dialect.CreateOrdersTable((string)pars[1], (string)pars[0], (string)pars[2]));
    }
}

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
    public void CreateSenario(BaseServiceTest<T> service, params object[] pars)
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

/// <summary>
/// EN: Executes the table-drop command for DDL scenarios.
/// PT: Executa o comando de remocao de tabela para cenarios DDL.
/// </summary>
public class DropTableServiceTest<T>(
        T connection,
        ITestScenario<T> testScenario,
        ProviderSqlDialect dialect
    ) : BaseServiceTest<T>(connection, testScenario, dialect),
        IBaseServiceTest
    where T : DbConnection
{
    /// <summary>
    /// EN: Drops the requested table for the current provider.
    /// PT: Remove a tabela solicitada para o provedor atual.
    /// </summary>
    /// <param name="pars"></param>
    public void RunTest(params object[] pars)
    {
        var sql = Dialect.DropTable((string)pars[0], (string)pars[1]);
        ExecuteNonQuery(sql);
    }
}
