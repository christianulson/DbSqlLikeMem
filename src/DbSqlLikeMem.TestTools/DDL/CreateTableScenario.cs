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
    public void CreateScenario(BaseServiceTest<T> service, params object[] pars)
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
