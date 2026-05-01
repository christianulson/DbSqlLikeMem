using DbSqlLikeMem.TestTools.DDL;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;
using DbSqlLikeMem.TestTools.TemporaryTable;

namespace DbSqlLikeMem.TestTools.Benchmarks;

/// <summary>
/// EN: Creates benchmark scenarios shared by benchmark sessions and fidelity tests.
/// PT: Cria cenarios de benchmark compartilhados por sessoes de benchmark e testes de fidelidade.
/// </summary>
public static class BenchmarkScenarioFactory
{
    /// <summary>
    /// EN: Creates a temporary-table scenario for the supplied provider dialect.
    /// PT: Cria um cenario de tabela temporaria para o dialeto do provedor informado.
    /// </summary>
    /// <returns>EN: A temporary-table scenario instance. PT: Uma instancia de cenario de tabela temporaria.</returns>
    public static TemporaryTableScenario CreateTemporaryTableScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates a temporary users scenario for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios temporarios para o dialeto do provedor informado.
    /// </summary>
    /// <returns>EN: A temporary users scenario instance. PT: Uma instancia de cenario de usuarios temporarios.</returns>
    public static TemporaryUsersScenario CreateTemporaryUsersScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates the create-table scenario used by DDL benchmarks.
    /// PT: Cria o cenario de create-table usado pelos benchmarks de DDL.
    /// </summary>
    /// <returns>EN: A create-table scenario instance. PT: Uma instancia de cenario de create-table.</returns>
    public static CreateTableScenario CreateTableScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates the create-table-with-foreign-key scenario used by DDL benchmarks.
    /// PT: Cria o cenario de create-table com chave estrangeira usado pelos benchmarks de DDL.
    /// </summary>
    /// <returns>EN: A create-table-with-foreign-key scenario instance. PT: Uma instancia de cenario de create-table com chave estrangeira.</returns>
    public static CreateTableWithFKScenario CreateTableWithFKScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates the drop-table scenario used by DDL benchmarks.
    /// PT: Cria o cenario de drop-table usado pelos benchmarks de DDL.
    /// </summary>
    /// <returns>EN: A drop-table scenario instance. PT: Uma instancia de cenario de drop-table.</returns>
    public static DropTableScenario CreateDropTableScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates the select-table scenario used by query benchmarks.
    /// PT: Cria o cenario de select-table usado pelos benchmarks de consulta.
    /// </summary>
    /// <returns>EN: A select-table scenario instance. PT: Uma instancia de cenario de select-table.</returns>
    public static SelectTableScenario CreateSelectTableScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates a users scenario with optional seed rows for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios com linhas iniciais opcionais para o dialeto do provedor informado.
    /// </summary>
    /// <param name="repo"></param>
    /// <param name="context"></param>
    /// <param name="seedRows">EN: The rows used to seed the users table. PT: As linhas usadas para popular a tabela de usuarios.</param>
    /// <returns>EN: A users scenario instance. PT: Uma instancia de cenario de usuarios.</returns>
    public static UsersScenario CreateUsersScenario(
        RepoService repo,
        FidelityTestContext context,
        params (int id, string name)[] seedRows)
        => new(repo, context, [seedRows.Cast<object?>()]);

    /// <summary>
    /// EN: Creates the insert-users scenario used by batch and execution-plan benchmarks.
    /// PT: Cria o cenario de insert de usuarios usado pelos benchmarks de batch e plano de execucao.
    /// </summary>
    /// <returns>EN: An insert-users scenario instance. PT: Uma instancia de cenario de insert de usuarios.</returns>
    public static InsertUsersScenario CreateInsertUsersScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates the sequence scenario used by sequence benchmarks.
    /// PT: Cria o cenario de sequencia usado pelos benchmarks de sequencia.
    /// </summary>
    /// <returns>EN: A sequence scenario instance. PT: Uma instancia de cenario de sequencia.</returns>
    public static SequenceScenario CreateSequenceScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates a no-op scenario for services that only need a connection handle.
    /// PT: Cria um cenario sem operacao para services que precisam apenas de uma conexao.
    /// </summary>
    /// <returns>EN: A no-op scenario instance. PT: Uma instancia de cenario sem operacao.</returns>
    public static NoopScenario CreateNoopScenario(RepoService repo, FidelityTestContext context)
        => new(repo, context);

    /// <summary>
    /// EN: Creates a users-and-orders scenario with optional seed rows for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios e pedidos com linhas iniciais opcionais para o dialeto do provedor informado.
    /// </summary>
    /// <param name="repo"></param>
    /// <param name="context"></param>
    /// <param name="seedUsers">EN: The rows used to seed the users table. PT: As linhas usadas para popular a tabela de usuarios.</param>
    /// <param name="seedOrders">EN: The rows used to seed the orders table. PT: As linhas usadas para popular a tabela de pedidos.</param>
    /// <returns>EN: A users-and-orders scenario instance. PT: Uma instancia de cenario de usuarios e pedidos.</returns>
    public static UsersOrdersScenario CreateUsersOrdersScenario(
        RepoService repo,
        FidelityTestContext context,
        (int id, string name)[]? seedUsers = null,
        (int id, int userId, string note)[]? seedOrders = null)
        => new(repo, context, seedUsers, seedOrders);

    /// <summary>
    /// EN: Creates a users-and-orders scenario with seeded order metrics for join benchmarks.
    /// PT: Cria um cenario de usuarios e pedidos com metricas de pedidos para benchmarks de join.
    /// </summary>
    /// <param name="repo"></param>
    /// <param name="context"></param>
    /// <param name="seedUsers">EN: The rows used to seed the users table. PT: As linhas usadas para popular a tabela de usuarios.</param>
    /// <param name="seedOrders">EN: The rows used to seed the orders table, including amount and quantity values. PT: As linhas usadas para popular a tabela de pedidos, incluindo valores de amount e quantity.</param>
    /// <returns>EN: A users-and-orders scenario instance. PT: Uma instancia de cenario de usuarios e pedidos.</returns>
    public static UsersOrdersScenario CreateUsersOrdersScenarioWithMetrics(
        RepoService repo,
        FidelityTestContext context,
        (int id, string name)[]? seedUsers = null,
        (int id, int userId, string note, decimal amount, int quantity, bool isPaid)[]? seedOrders = null)
        => new(repo, context, seedUsers, seedOrders);
}
