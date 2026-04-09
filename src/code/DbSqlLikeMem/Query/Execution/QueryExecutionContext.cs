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

    private int _positionalParameterCursor;

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
    /// EN: Snapshot of local time captured when the execution context was created.
    /// PT: Snapshot do horario local capturado quando o contexto de execucao foi criado.
    /// </summary>
    internal DateTime EvaluationLocalNow { get; }

    /// <summary>
    /// EN: Snapshot of UTC time captured when the execution context was created.
    /// PT: Snapshot do horario UTC capturado quando o contexto de execucao foi criado.
    /// </summary>
    internal DateTime EvaluationUtcNow { get; }

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
        EvaluationLocalNow = DateTime.Now;
        EvaluationUtcNow = DateTime.UtcNow;
    }

    private QueryExecutionContext(
        QueryExecutionContext source,
        int positionalParameterCursor)
    {
        Connection = source.Connection;
        Dialect = source.Dialect;
        Parameters = source.Parameters;
        DbParameters = source.DbParameters;
        Database = source.Database;
        Metrics = source.Metrics;
        MetricsEnabled = source.MetricsEnabled;
        CaptureExecutionPlans = source.CaptureExecutionPlans;
        CaptureAffectedRowSnapshots = source.CaptureAffectedRowSnapshots;
        ThreadSafe = source.ThreadSafe;
        CurrentQueryText = source.CurrentQueryText;
        SimulatedLatencyMs = source.SimulatedLatencyMs;
        DropProbability = source.DropProbability;
        HasActiveTransaction = source.HasActiveTransaction;
        EvaluationLocalNow = source.EvaluationLocalNow;
        EvaluationUtcNow = source.EvaluationUtcNow;
        _positionalParameterCursor = positionalParameterCursor;
    }

    /// <summary>
    /// EN: Resets the positional parameter cursor used to resolve SQL `?` placeholders in order.
    /// PT: Reinicia o cursor de parametros posicionais usado para resolver placeholders SQL `?` em ordem.
    /// </summary>
    internal void ResetPositionalParameterCursor()
        => _positionalParameterCursor = 0;

    /// <summary>
    /// EN: Creates a shallow copy that preserves the current positional-parameter cursor.
    /// PT: Cria uma copia rasa que preserva o cursor atual de parametros posicionais.
    /// </summary>
    internal QueryExecutionContext Fork()
        => new(this, _positionalParameterCursor);

    /// <summary>
    /// EN: Advances the positional-parameter cursor by a fixed number of consumed placeholders.
    /// PT: Avanca o cursor de parametros posicionais por uma quantidade fixa de placeholders consumidos.
    /// </summary>
    internal void AdvancePositionalParameterCursor(int count)
    {
        if (count > 0)
            _positionalParameterCursor += count;
    }

    /// <summary>
    /// EN: Resolves the next positional parameter value for the current expression evaluation.
    /// PT: Resolve o proximo valor de parametro posicional para a avaliacao atual da expressao.
    /// </summary>
    /// <returns>EN: The next positional parameter value, or null when none is available. PT: O proximo valor de parametro posicional, ou null quando nao houver nenhum.</returns>
    internal object? ResolveNextPositionalParameter()
    {
        TryResolveNextPositionalParameter(out var value);
        return value;
    }

    /// <summary>
    /// EN: Resolves the next positional parameter value and reports whether a value was available.
    /// PT: Resolve o proximo valor de parametro posicional e informa se havia um valor disponivel.
    /// </summary>
    /// <returns>EN: True when a positional parameter was consumed. PT: Verdadeiro quando um parametro posicional foi consumido.</returns>
    internal bool TryResolveNextPositionalParameter(out object? value)
    {
        var positionalParameters = new List<IDataParameter>();
        foreach (IDataParameter parameter in Parameters)
        {
            if (IsPositionalParameter(parameter.ParameterName))
                positionalParameters.Add(parameter);
        }

        if (positionalParameters.Count == 0)
        {
            value = null;
            return false;
        }

        var index = _positionalParameterCursor++;
        if (index >= positionalParameters.Count)
        {
            value = null;
            return false;
        }

        value = positionalParameters[index].Value;
        if (value is DBNull)
            value = null;

        return true;
    }

    /// <summary>
    /// EN: Resolves a named or positional parameter token against the current command parameters.
    /// PT: Resolve um token de parametro nomeado ou posicional contra os parametros atuais do comando.
    /// </summary>
    internal bool TryResolveParameter(string parameterToken, out object? value)
    {
        value = null;

        if (string.IsNullOrWhiteSpace(parameterToken))
            return false;

        if (parameterToken == "?")
            return TryResolveNextPositionalParameter(out value);

        var normalized = parameterToken.TrimStart('@', ':', '?');

        if (TryResolveParameterFromCollection(normalized, out value))
            return true;

        foreach (IDataParameter parameter in Parameters)
        {
            var candidate = parameter.ParameterName?.TrimStart('@', ':', '?');
            if (string.Equals(candidate, normalized, StringComparison.OrdinalIgnoreCase))
            {
                value = parameter.Value is DBNull ? null : parameter.Value;
                return true;
            }
        }

        return false;
    }

    private bool TryResolveParameterFromCollection(string normalizedParameterName, out object? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(normalizedParameterName))
            return false;

        var candidates = new[]
        {
            normalizedParameterName,
            "@" + normalizedParameterName,
            ":" + normalizedParameterName,
            "?" + normalizedParameterName
        };

        foreach (var candidate in candidates)
        {
            var index = DbParameters.IndexOf(candidate);
            if (index < 0)
                continue;

            if (DbParameters[index] is not IDataParameter parameter)
                continue;

            value = parameter.Value is DBNull ? null : parameter.Value;
            return true;
        }

        return false;
    }

    private static bool IsPositionalParameter(string? parameterName)
    {
        if (string.IsNullOrWhiteSpace(parameterName))
            return true;

        var normalized = parameterName!.Trim().TrimStart('@', ':', '?');
        if (normalized.Length == 0)
            return true;

        if (normalized.All(char.IsDigit))
            return true;

        if (normalized[0] is not ('p' or 'P'))
            return false;

        var hasDigit = false;
        for (var i = 1; i < normalized.Length; i++)
        {
            var ch = normalized[i];
            if (char.IsDigit(ch))
            {
                hasDigit = true;
                continue;
            }

            if (ch != '_')
                return false;
        }

        return hasDigit;
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
