namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Creates and drops the users and orders tables used by the join workflow.
/// PT-br: Cria e remove as tabelas de usuarios e pedidos usadas pelo fluxo de junção.
/// </summary>
public sealed class UsersOrdersScenario : BaseScenario, ITestScenario
{
    private readonly (int id, string name)[]? seedUsers;
    private readonly (int id, int userId, string note)[]? seedOrders;
    private readonly (int id, int userId, string note, decimal amount, int quantity, bool isPaid)[]? seedOrdersWithMetrics;

    /// <summary>
    /// EN: Creates a users-and-orders scenario with custom users and default empty orders.
    /// PT-br: Cria um cenário de usuarios e pedidos com usuarios customizados e pedidos vazios por padrão.
    /// </summary>
    public UsersOrdersScenario(
        RepoService repo,
        FidelityTestContext context,
        (int id, string name)[]? seedUsers = null,
        (int id, int userId, string note)[]? seedOrders = null)
        : base(repo, context)
    {
        this.seedUsers = seedUsers;
        this.seedOrders = seedOrders;
    }

    /// <summary>
    /// EN: Creates a users-and-orders scenario with custom users and metric-aware orders.
    /// PT-br: Cria um cenário de usuarios e pedidos com usuarios customizados e pedidos com metricas.
    /// </summary>
    public UsersOrdersScenario(
        RepoService repo,
        FidelityTestContext context,
        (int id, string name)[]? seedUsers,
        (int id, int userId, string note, decimal amount, int quantity, bool isPaid)[]? seedOrders)
        : base(repo, context)
    {
        this.seedUsers = seedUsers;
        seedOrdersWithMetrics = seedOrders;
    }

    /// <summary>
    /// EN: Creates a users-and-orders scenario with custom users and default empty orders.
    /// PT-br: Cria um cenário de usuarios e pedidos com usuarios customizados e pedidos vazios por padrão.
    /// </summary>
    public UsersOrdersScenario(
        RepoService repo,
        FidelityTestContext context,
        (int id, string name)[]? seedUsers)
        : this(repo, context, seedUsers, Array.Empty<(int id, int userId, string note)>())
    {
    }

    /// <summary>
    /// EN: Creates the users and orders tables and seeds the join data.
    /// PT-br: Cria as tabelas de usuarios e pedidos e preenche os dados da junção.
    /// </summary>
    public async Task CreateScenarioAsync()
    {
        var usersSeed = seedUsers ?? [(1, "Alice")];
        var d = Repo.Dialect;

        var currentTimestampExpr = d.Provider is ProviderId.SqlServer or ProviderId.SqlAzure
            ? "SYSUTCDATETIME()"
            : d.TemporalCurrentTimestampExpression();

        await Repo.ExecuteNonQueryAsync(d.CreateUsersTable(Context));
        await Repo.ExecuteNonQueryStatementsAsync(d.CreateOrdersTable(Context));

        foreach (var (id, name) in usersSeed)
        {
            await Repo.ExecuteNonQueryAsync(d.InsertUser(Context, id, name));
        }

        if (seedOrdersWithMetrics is not null)
        {
            foreach (var (id, userId, note, amount, quantity, isPaid) in seedOrdersWithMetrics)
            {
                var orderNumber = $"o-{id}";
                await Repo.ExecuteNonQueryAsync(d.InsertOrder(Context, id, userId, note, orderNumber, amount, quantity, isPaid, currentTimestampExpr));
            }

            return;
        }

        var ordersSeed = seedOrders ?? [(10, 1, "A"), (11, 1, "B")];
        foreach (var (id, userId, note) in ordersSeed)
        {
            var orderNumber = $"o-{id}";
            await Repo.ExecuteNonQueryAsync(d.InsertOrder(Context, id, userId, note, orderNumber, 0.00m, 1, false, currentTimestampExpr));
        }
    }

    /// <summary>
    /// EN: Drops the orders table first and then the users table.
    /// PT-br: Remove primeiro a tabela de pedidos e depois a tabela de usuarios.
    /// </summary>
    public async Task DropScenarioAsync()
    {
        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbOrdersFullName));
        }
        catch (Exception ex) when (IsMissingTableException(ex))
        {
        }

        try
        {
            await Repo.ExecuteNonQueryAsync(Repo.Dialect.DropTable(Context.TbUsersFullName));
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
