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
        try
        {
            service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[1], (string)pars[2]));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }

        try
        {
            service.ExecuteNonQuery(service.Dialect.DropTable((string)pars[0], (string)pars[2]));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }
    }

    private static bool IsMissingTableException(Exception ex)
    {
        var message = ex.GetBaseException().Message;
        return message.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("doesnt exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not exist", StringComparison.OrdinalIgnoreCase)
            || message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
            || message.Contains("not found", StringComparison.OrdinalIgnoreCase)
            || message.Contains("ora-00942", StringComparison.OrdinalIgnoreCase)
            || message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase)
            || message.Contains("given key was not present", StringComparison.OrdinalIgnoreCase);
    }
}
