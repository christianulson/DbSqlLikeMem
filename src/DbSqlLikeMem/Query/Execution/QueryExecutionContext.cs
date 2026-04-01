namespace DbSqlLikeMem;

/// <summary>
/// EN: Carries all shared state required to execute a SQL query or DML statement against the in-memory mock database.
/// PT: Transporta todo o estado compartilhado necessário para executar uma query SQL ou instrução DML contra o banco de dados mock em memória.
/// </summary>
internal sealed class QueryExecutionContext
{
    /// <summary>
    /// EN: SQL dialect rules applied to this execution.
    /// PT: Regras do dialeto SQL aplicadas a esta execução.
    /// </summary>
    public ISqlDialect Dialect { get; }

    /// <summary>
    /// EN: Connection that originated the query execution.
    /// PT: Conexão que originou a execução da query.
    /// </summary>
    public DbConnectionMockBase Connection { get; }

    /// <summary>
    /// EN: ADO.NET parameters bound to the current command.
    /// PT: Parâmetros ADO.NET vinculados ao comando atual.
    /// </summary>
    public IDataParameterCollection Parameters { get; }

    /// <summary>
    /// EN: ADO.NET parameters cast to DbParameterCollection for strategy compatibility.
    /// PT: Parâmetros ADO.NET convertidos para DbParameterCollection para compatibilidade com as strategies.
    /// </summary>
    public DbParameterCollection DbParameters { get; }

    /// <summary>
    /// EN: In-memory database backing this execution.
    /// PT: Banco de dados em memória que sustenta esta execução.
    /// </summary>
    public DbMock Database { get; }

    /// <summary>
    /// EN: Metrics collector associated with the executing connection.
    /// PT: Coletor de métricas associado à conexão em execução.
    /// </summary>
    public DbMetrics Metrics { get; }

    /// <summary>
    /// EN: Pre-computed flag indicating whether metrics collection is active.
    /// PT: Flag pré-calculada indicando se a coleta de métricas está ativa.
    /// </summary>
    public bool MetricsEnabled { get; }

    /// <summary>
    /// EN: Indicates whether execution-plan capture is active for this execution.
    /// PT: Indica se a captura de planos de execução está ativa para esta execução.
    /// </summary>
    public bool CaptureExecutionPlans { get; }

    /// <summary>
    /// EN: Indicates whether affected-row data snapshots should be captured during DML operations.
    /// PT: Indica se os snapshots de dados de linhas afetadas devem ser capturados durante operações DML.
    /// </summary>
    public bool CaptureAffectedRowSnapshots { get; }

    /// <summary>
    /// EN: Indicates whether thread-safe locking should be used during table mutations.
    /// PT: Indica se bloqueio thread-safe deve ser usado durante mutações de tabelas.
    /// </summary>
    public bool ThreadSafe { get; }

    /// <summary>
    /// EN: Raw SQL text of the command being executed, when available.
    /// PT: Texto SQL bruto do comando sendo executado, quando disponível.
    /// </summary>
    public string? CurrentQueryText { get; }

    /// <summary>
    /// EN: Simulated network latency in milliseconds for this connection.
    /// PT: Latência de rede simulada em milissegundos para esta conexão.
    /// </summary>
    public int SimulatedLatencyMs { get; }

    /// <summary>
    /// EN: Simulated network failure probability for this connection.
    /// PT: Probabilidade de falha de rede simulada para esta conexão.
    /// </summary>
    public double DropProbability { get; }

    /// <summary>
    /// EN: Indicates whether a transaction was active when this execution context was created.
    /// PT: Indica se havia uma transacao ativa quando este contexto de execucao foi criado.
    /// </summary>
    public bool HasActiveTransaction { get; }

    /// <summary>
    /// EN: Creates a query execution context from a connection, dialect, and parameter collection.
    /// PT: Cria um contexto de execução de query a partir de uma conexão, dialeto e coleção de parâmetros.
    /// </summary>
    public QueryExecutionContext(
        DbConnectionMockBase connection,
        ISqlDialect dialect,
        IDataParameterCollection parameters)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(parameters, nameof(parameters));

        Connection = connection;
        Dialect = dialect;
        Parameters = parameters;
        DbParameters = (DbParameterCollection)parameters;
        Database = connection.Db;
        Metrics = connection.Metrics;
        MetricsEnabled = connection.Metrics.Enabled;
        CaptureExecutionPlans = connection.Db.CaptureExecutionPlans;
        CaptureAffectedRowSnapshots = connection.CaptureAffectedRowSnapshots;
        ThreadSafe = connection.Db.ThreadSafe;
        CurrentQueryText = connection.GetCurrentQueryText();
        SimulatedLatencyMs = connection.SimulatedLatencyMs;
        DropProbability = connection.DropProbability;
        HasActiveTransaction = connection.HasActiveTransaction;
    }

    /// <summary>
    /// EN: Creates a context from a connection using its current execution dialect.
    /// PT: Cria um contexto a partir de uma conexão usando o dialeto de execução atual.
    /// </summary>
    public static QueryExecutionContext FromConnection(
        DbConnectionMockBase connection,
        DbParameterCollection parameters)
        => new(connection, connection.ExecutionDialect, parameters);

    /// <summary>
    /// EN: Builds a mock runtime context snapshot for execution-plan formatting.
    /// PT: Constrói um snapshot de contexto de runtime mock para formatação de planos de execução.
    /// </summary>
    public SqlPlanMockRuntimeContext BuildPlanRuntimeContext()
        => new(SimulatedLatencyMs, DropProbability, ThreadSafe);

    /// <summary>
    /// EN: Creates an AST executor scoped to the dialect and parameters of this context.
    /// PT: Cria um executor de AST com escopo no dialeto e parâmetros deste contexto.
    /// </summary>
    public IAstQueryExecutor CreateExecutor()
        => AstQueryExecutorFactory.Create(Dialect, Connection, Parameters);

    public NotSupportedException NotSupported(string feature)
        => Dialect.NotSupported(feature);
}
