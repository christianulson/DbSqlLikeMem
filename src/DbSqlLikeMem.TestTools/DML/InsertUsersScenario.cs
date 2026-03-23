namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the users table used by insert workflows.
/// PT: Cria e remove a tabela de usuarios usada pelos fluxos de insert.
/// </summary>
public class InsertUsersScenario<T>(
    ProviderSqlDialect dialect
    ) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users table required by the insert workflow.
    /// PT: Cria a tabela de usuarios exigida pelo fluxo de insert.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));
    }

    /// <summary>
    /// EN: Drops the users table created for the insert workflow.
    /// PT: Remove a tabela de usuarios criada para o fluxo de insert.
    /// </summary>
    /// <param name="service">EN: The shared test service used to execute SQL. PT: O servico de teste compartilhado usado para executar SQL.</param>
    /// <param name="pars">EN: The scenario parameters. PT: Os parametros do cenario.</param>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        service.ExecuteNonQuery(dialect.DropTable(users, uId));
    }
}
