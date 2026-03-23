using DbSqlLikeMem.TestTools.Benchmarks;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.TemporaryTable;
using DbSqlLikeMem.TestTools.Query;
using System.Globalization;

namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Generates a unique temporary table name for the users table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de usuários usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary users table name. PT-br: Um nome único de tabela temporária de usuários.</returns>
    protected virtual string NewUsersTableName() => $"USR";

    /// <summary>
    /// EN: Generates a unique temporary table name for the orders table used by a benchmark run.
    /// PT-br: Gera um nome único de tabela temporária para a tabela de pedidos usada em uma execução de benchmark.
    /// </summary>
    /// <returns>EN: A unique temporary orders table name. PT-br: Um nome único de tabela temporária de pedidos.</returns>
    protected virtual string NewOrdersTableName() => $"ORD";

    /// <summary>
    /// EN: Generates a unique temporary sequence name for sequence-based benchmark operations.
    /// PT-br: Gera um nome único de sequência temporária para operações de benchmark baseadas em sequência.
    /// </summary>
    /// <returns>EN: A unique temporary sequence name. PT-br: Um nome único de sequência temporária.</returns>
    protected virtual string NewSequenceName() => $"SEQ_{NextToken()}";

    protected virtual string NewSavepointName() => $"SP_{NextToken()}";

    protected static string NextToken() => Interlocked.Increment(ref _objectCounter).ToString("x8", CultureInfo.InvariantCulture).ToUpperInvariant();

    /// <summary>
    /// EN: Measures the cost of opening a new database connection.
    /// PT-br: Mede o custo de abrir uma nova conexão de banco de dados.
    /// </summary>
    protected virtual void RunConnectionOpen()
    {
        using var connection = CreateConnection();
        connection.Open();
        GC.KeepAlive(connection.State);
    }

    private InsertUsersServiceTest<DbConnection> CreateInsertUsersService(
        DbConnection connection,
        Func<DbConnection>? connectionFactory = null)
        => new(connection, BenchmarkScenarioFactory.CreateInsertUsersScenario<DbConnection>(Dialect), Dialect, connectionFactory);

    private DmlMutationServiceTest<DbConnection> CreateMutationService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private BatchServiceTest<DbConnection> CreateBatchService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private QueryServiceTest<DbConnection> CreateQueryService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private ExecutionPlanServiceTest<DbConnection> CreateExecutionPlanService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private DebugTraceServiceTest<DbConnection> CreateDebugTraceService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario)
        => new(connection, scenario, Dialect);

    private ConnectionLifecycleServiceTest<DbConnection> CreateConnectionLifecycleService(
        DbConnection connection)
        => new(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>(), Dialect);

    protected TemporaryTableServiceTest<DbConnection> CreateTemporaryTableService(
        DbConnection connection,
        ITestScenario<DbConnection> scenario,
        Func<DbConnection>? connectionFactory = null)
        => new(connection, scenario, Dialect, connectionFactory);

    private SchemaSnapshotServiceTest<DbConnection> CreateSchemaSnapshotService(
        DbConnection connection)
        => new(connection, BenchmarkScenarioFactory.CreateNoopScenario<DbConnection>(), Dialect);

    protected static int ExecuteNonQuery(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteNonQuery();
    }

    protected static object? ExecuteScalar(
        DbConnection connection,
        string sql,
        DbTransaction? transaction = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        return command.ExecuteScalar();
    }

}
