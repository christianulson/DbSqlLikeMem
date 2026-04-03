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
        var usersTable = ResolveScenarioTableName(users, uId);
        var ordersTable = ResolveScenarioTableName(orders, uId);
        var currentTimestampExpr = dialect.TemporalCurrentTimestampExpression();

        service.ExecuteNonQuery(dialect.CreateUsersTable(users, uId));
        service.ExecuteNonQuery(dialect.CreateOrdersTable(orders, usersTable, uId));

        foreach (var (id, name) in usersSeed)
        {
            service.ExecuteNonQuery(dialect.InsertUser(usersTable, id, name));
        }

        foreach (var (id, userId, note) in ordersSeed)
        {
            var orderNumber = $"o-{id}";
            service.ExecuteNonQuery(dialect.InsertOrder(ordersTable, usersTable, id, userId, note, orderNumber, 0.00m, 1, false, currentTimestampExpr));
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

    private string ResolveScenarioTableName(string tableName, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? $"{tableName}_{uId}".ToLowerInvariant()
            : $"{tableName}_{uId}";
}
