namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the users and orders tables used by the join workflow.
/// PT: Cria e remove as tabelas de usuarios e pedidos usadas pelo fluxo de junção.
/// </summary>
public sealed class UsersOrdersScenario<T>(
    ProviderSqlDialect dialect,
    (int id, string name)[]? seedUsers = null,
    (int id, int userId, string note)[]? seedOrders = null) : ITestScenario<T>
    where T : DbConnection
{
    /// <summary>
    /// EN: Creates the users and orders tables and seeds the join data.
    /// PT: Cria as tabelas de usuarios e pedidos e preenche os dados da junção.
    /// </summary>
    public void CreateScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var uId = (string)pars[2];
        var usersSeed = seedUsers ?? [(1, "Alice")];
        var ordersSeed = seedOrders ?? [(10, 1, "A"), (11, 1, "B")];

        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));
        service.ExecuteNonQuery(dialect.CreateOrdersTable(orders, users, uId));

        foreach (var (id, name) in usersSeed)
        {
            service.ExecuteNonQuery(dialect.InsertUser(users, id, name));
        }

        foreach (var (id, userId, note) in ordersSeed)
        {
            service.ExecuteNonQuery(dialect.InsertOrder(orders, users, id, userId, note));
        }
    }

    /// <summary>
    /// EN: Drops the orders table first and then the users table.
    /// PT: Remove primeiro a tabela de pedidos e depois a tabela de usuarios.
    /// </summary>
    public void DropScenario(
        BaseServiceTest<T> service,
        params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var uId = (string)pars[2];

        service.ExecuteNonQuery(dialect.DropTable(orders, uId));
        service.ExecuteNonQuery(dialect.DropTable(users, uId));
    }
}
