namespace DbSqlLikeMem.TestTools.DDL;

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
        var usersTable = $"{(string)pars[0]}_{(string)pars[2]}";
        ExecuteNonQuery(Dialect.CreateOrdersTable((string)pars[1], usersTable, (string)pars[2]));
    }
}
