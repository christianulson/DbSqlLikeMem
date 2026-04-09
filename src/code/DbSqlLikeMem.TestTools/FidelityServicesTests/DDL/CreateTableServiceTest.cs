namespace DbSqlLikeMem.TestTools.DDL;

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
