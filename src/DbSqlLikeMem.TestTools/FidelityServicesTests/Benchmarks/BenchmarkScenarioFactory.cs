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
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <returns>EN: A temporary-table scenario instance. PT: Uma instancia de cenario de tabela temporaria.</returns>
    public static TemporaryTableScenario<TConnection> CreateTemporaryTableScenario<TConnection>(ProviderSqlDialect dialect)
        where TConnection : DbConnection
        => new(dialect);

    /// <summary>
    /// EN: Creates a temporary users scenario for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios temporarios para o dialeto do provedor informado.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <returns>EN: A temporary users scenario instance. PT: Uma instancia de cenario de usuarios temporarios.</returns>
    public static TemporaryUsersScenario<TConnection> CreateTemporaryUsersScenario<TConnection>(ProviderSqlDialect dialect)
        where TConnection : DbConnection
        => new(dialect);

    /// <summary>
    /// EN: Creates the create-table scenario used by DDL benchmarks.
    /// PT: Cria o cenario de create-table usado pelos benchmarks de DDL.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <returns>EN: A create-table scenario instance. PT: Uma instancia de cenario de create-table.</returns>
    public static CreateTableScenario<TConnection> CreateTableScenario<TConnection>()
        where TConnection : DbConnection
        => new();

    /// <summary>
    /// EN: Creates the select-table scenario used by query benchmarks.
    /// PT: Cria o cenario de select-table usado pelos benchmarks de consulta.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <returns>EN: A select-table scenario instance. PT: Uma instancia de cenario de select-table.</returns>
    public static SelectTableScenario<TConnection> CreateSelectTableScenario<TConnection>(ProviderSqlDialect dialect)
        where TConnection : DbConnection
        => new(dialect);

    /// <summary>
    /// EN: Creates a users scenario with optional seed rows for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios com linhas iniciais opcionais para o dialeto do provedor informado.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <param name="seedRows">EN: The rows used to seed the users table. PT: As linhas usadas para popular a tabela de usuarios.</param>
    /// <returns>EN: A users scenario instance. PT: Uma instancia de cenario de usuarios.</returns>
    public static UsersScenario<TConnection> CreateUsersScenario<TConnection>(
        ProviderSqlDialect dialect,
        params (int id, string name)[] seedRows)
        where TConnection : DbConnection
        => new(dialect, seedRows);

    /// <summary>
    /// EN: Creates the insert-users scenario used by batch and execution-plan benchmarks.
    /// PT: Cria o cenario de insert de usuarios usado pelos benchmarks de batch e plano de execucao.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <returns>EN: An insert-users scenario instance. PT: Uma instancia de cenario de insert de usuarios.</returns>
    public static InsertUsersScenario<TConnection> CreateInsertUsersScenario<TConnection>(ProviderSqlDialect dialect)
        where TConnection : DbConnection
        => new(dialect);

    /// <summary>
    /// EN: Creates the sequence scenario used by sequence benchmarks.
    /// PT: Cria o cenario de sequencia usado pelos benchmarks de sequencia.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <returns>EN: A sequence scenario instance. PT: Uma instancia de cenario de sequencia.</returns>
    public static SequenceScenario<TConnection> CreateSequenceScenario<TConnection>(ProviderSqlDialect dialect)
        where TConnection : DbConnection
        => new(dialect);

    /// <summary>
    /// EN: Creates a no-op scenario for services that only need a connection handle.
    /// PT: Cria um cenario sem operacao para services que precisam apenas de uma conexao.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <returns>EN: A no-op scenario instance. PT: Uma instancia de cenario sem operacao.</returns>
    public static NoopScenario<TConnection> CreateNoopScenario<TConnection>()
        where TConnection : DbConnection
        => new();

    /// <summary>
    /// EN: Creates a users-and-orders scenario with optional seed rows for the supplied provider dialect.
    /// PT: Cria um cenario de usuarios e pedidos com linhas iniciais opcionais para o dialeto do provedor informado.
    /// </summary>
    /// <typeparam name="TConnection">EN: The connection type bound to the scenario. PT: O tipo de conexao vinculado ao cenario.</typeparam>
    /// <param name="dialect">EN: The provider dialect used by the scenario. PT: O dialeto do provedor usado pelo cenario.</param>
    /// <param name="seedUsers">EN: The rows used to seed the users table. PT: As linhas usadas para popular a tabela de usuarios.</param>
    /// <param name="seedOrders">EN: The rows used to seed the orders table. PT: As linhas usadas para popular a tabela de pedidos.</param>
    /// <returns>EN: A users-and-orders scenario instance. PT: Uma instancia de cenario de usuarios e pedidos.</returns>
    public static UsersOrdersScenario<TConnection> CreateUsersOrdersScenario<TConnection>(
        ProviderSqlDialect dialect,
        (int id, string name)[]? seedUsers = null,
        (int id, int userId, string note)[]? seedOrders = null)
        where TConnection : DbConnection
        => new(dialect, seedUsers, seedOrders);
}
