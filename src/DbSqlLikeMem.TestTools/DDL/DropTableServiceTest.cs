namespace DbSqlLikeMem.TestTools.DDL;

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
