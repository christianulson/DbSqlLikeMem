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

    private readonly Dictionary<string, object?> _temporalZeroArgIdentifierCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, object?> _temporalZeroArgCallCache = new(StringComparer.OrdinalIgnoreCase);
    private readonly bool _oracleEmptyStringAsNull;
    private readonly bool _sqliteDecimalRoundTrip;
    private readonly Dictionary<string, object?> _namedParameterValues;
    private readonly object?[] _positionalParameterValues;
    private readonly object?[] _orderedParameterValues;
    private int _positionalParameterScopeDepth;
    private int _orderedParameterCursor;

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
        var providerName = connection.ProviderExecutionDialect.Name;
        _oracleEmptyStringAsNull = string.Equals(providerName, "oracle", StringComparison.OrdinalIgnoreCase);
        _sqliteDecimalRoundTrip = string.Equals(providerName, "sqlite", StringComparison.OrdinalIgnoreCase);
        BuildParameterLookupCaches(parameters, out _namedParameterValues, out _positionalParameterValues, out _orderedParameterValues);
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
        _temporalZeroArgIdentifierCache = source._temporalZeroArgIdentifierCache;
        _temporalZeroArgCallCache = source._temporalZeroArgCallCache;
        _oracleEmptyStringAsNull = source._oracleEmptyStringAsNull;
        _sqliteDecimalRoundTrip = source._sqliteDecimalRoundTrip;
        _namedParameterValues = source._namedParameterValues;
        _positionalParameterValues = source._positionalParameterValues;
        _orderedParameterValues = source._orderedParameterValues;
        _positionalParameterCursor = positionalParameterCursor;
        _orderedParameterCursor = source._orderedParameterCursor;
    }

    internal bool TryGetCachedTemporalZeroArgIdentifierValue(string functionName, out object? value)
        => _temporalZeroArgIdentifierCache.TryGetValue(functionName, out value);

    internal void CacheTemporalZeroArgIdentifierValue(string functionName, object? value)
    {
        if (!_temporalZeroArgIdentifierCache.ContainsKey(functionName))
            _temporalZeroArgIdentifierCache[functionName] = value;
    }

    internal bool TryGetCachedTemporalZeroArgCallValue(string functionName, out object? value)
        => _temporalZeroArgCallCache.TryGetValue(functionName, out value);

    internal void CacheTemporalZeroArgCallValue(string functionName, object? value)
    {
        if (!_temporalZeroArgCallCache.ContainsKey(functionName))
            _temporalZeroArgCallCache[functionName] = value;
    }

    /// <summary>
    /// EN: Resets the positional parameter cursor used to resolve SQL `?` placeholders in order.
    /// PT: Reinicia o cursor de parametros posicionais usado para resolver placeholders SQL `?` em ordem.
    /// </summary>
    internal void ResetPositionalParameterCursor()
    {
        _positionalParameterCursor = 0;
        _orderedParameterCursor = 0;
    }

    /// <summary>
    /// EN: Begins a positional-parameter evaluation scope and resets the cursor when entering the outermost scope.
    /// PT: Inicia um escopo de avaliacao de parametros posicionais e reinicia o cursor ao entrar no escopo mais externo.
    /// </summary>
    /// <returns>EN: A disposable scope that restores the nesting depth on exit. PT: Um escopo descartavel que restaura a profundidade de aninhamento ao sair.</returns>
    internal PositionalParameterScope BeginPositionalParameterScope()
    {
        if (_positionalParameterScopeDepth++ == 0)
            ResetPositionalParameterCursor();

        return new PositionalParameterScope(this);
    }

    /// <summary>
    /// EN: Indicates whether a positional-parameter evaluation scope is active.
    /// PT: Indica se um escopo de avaliacao de parametros posicionais esta ativo.
    /// </summary>
    internal bool HasPositionalParameterScope => _positionalParameterScopeDepth > 0;

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
        if (_positionalParameterValues.Length > 0)
        {
            if ((uint)_positionalParameterCursor >= (uint)_positionalParameterValues.Length)
            {
                value = null;
                return false;
            }

            value = _positionalParameterValues[_positionalParameterCursor++];
            return true;
        }

        // Some providers use positional SQL markers but only expose named ADO.NET parameters.
        // In that case we fall back to command order for '?' resolution.
        if ((uint)_orderedParameterCursor >= (uint)_orderedParameterValues.Length)
        {
            value = null;
            return false;
        }

        value = _orderedParameterValues[_orderedParameterCursor++];
        return true;
    }

    /// <summary>
    /// EN: Captures the current positional and ordered parameter cursor state.
    /// PT: Captura o estado atual dos cursores posicional e ordenado dos parametros.
    /// </summary>
    /// <returns>EN: The current cursor positions. PT: As posicoes atuais dos cursores.</returns>
    internal (int PositionalParameterCursor, int OrderedParameterCursor) SnapshotPositionalParameterState()
        => (_positionalParameterCursor, _orderedParameterCursor);

    /// <summary>
    /// EN: Restores a previously captured positional and ordered parameter cursor state.
    /// PT: Restaura um estado capturado anteriormente dos cursores posicional e ordenado dos parametros.
    /// </summary>
    /// <param name="state">EN: Captured cursor positions. PT: Posicoes capturadas dos cursores.</param>
    internal void RestorePositionalParameterState((int PositionalParameterCursor, int OrderedParameterCursor) state)
    {
        _positionalParameterCursor = state.PositionalParameterCursor < 0
            ? 0
            : state.PositionalParameterCursor > _positionalParameterValues.Length
                ? _positionalParameterValues.Length
                : state.PositionalParameterCursor;
        _orderedParameterCursor = state.OrderedParameterCursor < 0
            ? 0
            : state.OrderedParameterCursor > _orderedParameterValues.Length
                ? _orderedParameterValues.Length
                : state.OrderedParameterCursor;
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

        if (_namedParameterValues.TryGetValue(parameterToken, out value))
            return true;

        var normalized = parameterToken.TrimStart('@', ':', '?');
        if (normalized.Length == 0)
            return false;

        if (normalized.Length != parameterToken.Length
            && _namedParameterValues.TryGetValue(normalized.ToString(), out value))
        {
            return true;
        }

        return false;
    }

    private void BuildParameterLookupCaches(
        IDataParameterCollection parameters,
        out Dictionary<string, object?> namedParameterValues,
        out object?[] positionalParameterValues,
        out object?[] orderedParameterValues)
    {
        var parameterCount = parameters.Count;
        Dictionary<string, object?>? namedValues = parameterCount > 0
            ? new Dictionary<string, object?>(parameterCount * 4, StringComparer.OrdinalIgnoreCase)
            : null;
        List<object?>? positionalValues = parameterCount > 0
            ? new List<object?>(parameterCount)
            : null;
        List<object?>? orderedValues = parameterCount > 0
            ? new List<object?>(parameterCount)
            : null;

        foreach (IDataParameter parameter in parameters)
        {
            var resolvedValue = NormalizeResolvedValue(parameter.Value);
            orderedValues ??= new List<object?>(parameterCount);
            orderedValues.Add(resolvedValue);

            if (IsPositionalParameter(parameter.ParameterName))
            {
                if (positionalValues is null)
                    positionalValues = new List<object?>(parameterCount);

                positionalValues.Add(resolvedValue);
            }

            if (!string.IsNullOrWhiteSpace(parameter.ParameterName))
            {
                if (namedValues is null)
                    namedValues = new Dictionary<string, object?>(parameterCount * 5, StringComparer.OrdinalIgnoreCase);

                AddNamedParameterValueVariants(namedValues, parameter.ParameterName!, resolvedValue);
            }
        }

        namedParameterValues = namedValues ?? new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        positionalParameterValues = positionalValues is null ? Array.Empty<object?>() : [.. positionalValues];
        orderedParameterValues = orderedValues is null ? Array.Empty<object?>() : [.. orderedValues];
    }

    private static void AddNamedParameterValueVariants(
        Dictionary<string, object?> namedValues,
        string parameterName,
        object? value)
    {
        AddNamedParameterValue(namedValues, parameterName, value);

        var normalizedName = TrimParameterPrefix(parameterName);
        if (normalizedName.Length == 0)
            return;

        var canonicalName = normalizedName.Length == parameterName.Length
            ? parameterName
            : normalizedName.ToString();

        AddNamedParameterValue(namedValues, canonicalName, value);
        AddNamedParameterValue(namedValues, string.Concat("@", canonicalName), value);
        AddNamedParameterValue(namedValues, string.Concat(":", canonicalName), value);
        AddNamedParameterValue(namedValues, string.Concat("?", canonicalName), value);
    }

    private static void AddNamedParameterValue(
        Dictionary<string, object?> namedValues,
        string parameterName,
        object? value)
    {
        namedValues.TryAdd(parameterName, value);
    }

    private static ReadOnlySpan<char> TrimParameterPrefix(string parameterName)
    {
        var span = parameterName.AsSpan();
        while (span.Length > 0 && span[0] is '@' or ':' or '?')
            span = span[1..];

        return span;
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

    internal object? NormalizeResolvedValue(object? value)
    {
        if (value is DBNull)
            return null;

        if (_oracleEmptyStringAsNull
            && value is string text
            && text.Length == 0)
        {
            return null;
        }

        if (_sqliteDecimalRoundTrip
            && value is decimal decimalValue)
        {
            return Convert.ToDecimal(Convert.ToDouble(decimalValue, CultureInfo.InvariantCulture), CultureInfo.InvariantCulture);
        }

        return value;
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

    internal readonly struct PositionalParameterScope(QueryExecutionContext context) : IDisposable
    {
        public void Dispose()
        {
            if (context._positionalParameterScopeDepth > 0)
                context._positionalParameterScopeDepth--;
        }
    }
}
