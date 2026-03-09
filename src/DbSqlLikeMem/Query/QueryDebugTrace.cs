namespace DbSqlLikeMem;

/// <summary>
/// EN: Captures the runtime operator trace produced while executing a SQL command in the mock engine.
/// PT: Captura o rastro de operadores em runtime produzido durante a execucao de um comando SQL no mecanismo simulado.
/// </summary>
public sealed class QueryDebugTrace
{
    /// <summary>
    /// EN: Initializes a runtime trace with its query kind, statement context and ordered execution steps.
    /// PT: Inicializa um trace de runtime com o tipo da query, o contexto do statement e os passos ordenados de execucao.
    /// </summary>
    /// <param name="queryType">EN: Query kind represented by the trace, such as SELECT or UNION. PT: Tipo de query representado pelo trace, como SELECT ou UNION.</param>
    /// <param name="statementIndex">EN: Zero-based statement position inside the executed SQL batch. PT: Posicao do statement baseada em zero dentro do lote SQL executado.</param>
    /// <param name="sqlText">EN: SQL text associated with the captured statement, when available. PT: Texto SQL associado ao statement capturado, quando disponivel.</param>
    /// <param name="steps">EN: Ordered runtime steps captured during execution. PT: Passos de runtime ordenados capturados durante a execucao.</param>
    public QueryDebugTrace(
        string queryType,
        int statementIndex,
        string? sqlText,
        IReadOnlyList<QueryDebugTraceStep> steps)
    {
        QueryType = queryType;
        StatementIndex = Math.Max(0, statementIndex);
        SqlText = sqlText;
        Steps = steps ?? [];
        if (Steps.Count == 0)
        {
            TotalExecutionTime = TimeSpan.Zero;
            MaxInputRows = 0;
            MaxOutputRows = 0;
            OperatorSignature = string.Empty;
            FirstOperator = string.Empty;
            LastOperator = string.Empty;
            OperatorCounts = string.Empty;
            SlowestOperator = string.Empty;
            SlowestStepIndex = -1;
            SlowestStepDetails = null;
            FastestOperator = string.Empty;
            FastestStepIndex = -1;
            FastestStepDetails = null;
            WidestOperator = string.Empty;
            WidestStepIndex = -1;
            WidestStepDetails = null;
            NarrowestOperator = string.Empty;
            NarrowestStepIndex = -1;
            NarrowestStepDetails = null;
            return;
        }

        var totalTicks = 0L;
        var maxInputRows = 0;
        var maxOutputRows = 0;
        var operatorCounts = new Dictionary<string, int>(StringComparer.Ordinal);
        var signature = new System.Text.StringBuilder();

        var slowestStepIndex = 0;
        var fastestStepIndex = 0;
        var widestStepIndex = 0;
        var narrowestStepIndex = 0;

        for (var i = 0; i < Steps.Count; i++)
        {
            var step = Steps[i];
            totalTicks += step.ExecutionTime.Ticks;

            if (step.InputRows > maxInputRows)
                maxInputRows = step.InputRows;
            if (step.OutputRows > maxOutputRows)
                maxOutputRows = step.OutputRows;

            if (signature.Length > 0)
                signature.Append(" -> ");
            signature.Append(step.Operator);

            operatorCounts.TryGetValue(step.Operator, out var count);
            operatorCounts[step.Operator] = count + 1;

            if (IsSlowerStep(step, Steps[slowestStepIndex]))
                slowestStepIndex = i;
            if (IsFasterStep(step, Steps[fastestStepIndex]))
                fastestStepIndex = i;
            if (IsWiderStep(step, Steps[widestStepIndex]))
                widestStepIndex = i;
            if (IsNarrowerStep(step, Steps[narrowestStepIndex]))
                narrowestStepIndex = i;
        }

        TotalExecutionTime = TimeSpan.FromTicks(totalTicks);
        MaxInputRows = maxInputRows;
        MaxOutputRows = maxOutputRows;
        OperatorSignature = signature.ToString();
        FirstOperator = Steps[0].Operator;
        LastOperator = Steps[^1].Operator;
        OperatorCounts = string.Join(";", operatorCounts
            .OrderBy(static pair => pair.Key, StringComparer.Ordinal)
            .Select(static pair => $"{pair.Key}:{pair.Value}"));
        SlowestOperator = Steps[slowestStepIndex].Operator;
        SlowestStepIndex = slowestStepIndex;
        SlowestStepDetails = Steps[slowestStepIndex].Details;
        FastestOperator = Steps[fastestStepIndex].Operator;
        FastestStepIndex = fastestStepIndex;
        FastestStepDetails = Steps[fastestStepIndex].Details;
        WidestOperator = Steps[widestStepIndex].Operator;
        WidestStepIndex = widestStepIndex;
        WidestStepDetails = Steps[widestStepIndex].Details;
        NarrowestOperator = Steps[narrowestStepIndex].Operator;
        NarrowestStepIndex = narrowestStepIndex;
        NarrowestStepDetails = Steps[narrowestStepIndex].Details;
    }

    /// <summary>
    /// EN: Query kind represented by the runtime trace.
    /// PT: Tipo de query representado pelo trace de runtime.
    /// </summary>
    public string QueryType { get; }

    /// <summary>
    /// EN: Ordered execution steps captured for the query.
    /// PT: Passos ordenados de execucao capturados para a query.
    /// </summary>
    public IReadOnlyList<QueryDebugTraceStep> Steps { get; }

    /// <summary>
    /// EN: Zero-based statement position inside the executed SQL batch.
    /// PT: Posicao do statement baseada em zero dentro do lote SQL executado.
    /// </summary>
    public int StatementIndex { get; }

    /// <summary>
    /// EN: SQL text associated with the captured statement, when available.
    /// PT: Texto SQL associado ao statement capturado, quando disponivel.
    /// </summary>
    public string? SqlText { get; }

    /// <summary>
    /// EN: Total elapsed time accumulated across all captured operator steps.
    /// PT: Tempo total acumulado entre todos os passos de operador capturados.
    /// </summary>
    public TimeSpan TotalExecutionTime { get; }

    /// <summary>
    /// EN: Highest input row count observed across the captured operator steps.
    /// PT: Maior contagem de linhas de entrada observada entre os passos de operador capturados.
    /// </summary>
    public int MaxInputRows { get; }

    /// <summary>
    /// EN: Highest output row count observed across the captured operator steps.
    /// PT: Maior contagem de linhas de saida observada entre os passos de operador capturados.
    /// </summary>
    public int MaxOutputRows { get; }

    /// <summary>
    /// EN: Ordered operator names joined into a compact execution signature.
    /// PT: Nomes ordenados dos operadores unidos em uma assinatura compacta de execucao.
    /// </summary>
    public string OperatorSignature { get; }

    /// <summary>
    /// EN: First operator captured in the execution flow.
    /// PT: Primeiro operador capturado no fluxo de execucao.
    /// </summary>
    public string FirstOperator { get; }

    /// <summary>
    /// EN: Last operator captured in the execution flow.
    /// PT: Ultimo operador capturado no fluxo de execucao.
    /// </summary>
    public string LastOperator { get; }

    /// <summary>
    /// EN: Aggregated counts for each operator captured in the trace.
    /// PT: Contagens agregadas de cada operador capturado no trace.
    /// </summary>
    public string OperatorCounts { get; }

    /// <summary>
    /// EN: Operator with the highest captured execution time in this trace.
    /// PT: Operador com o maior tempo de execucao capturado neste trace.
    /// </summary>
    public string SlowestOperator { get; }

    /// <summary>
    /// EN: Zero-based index of the slowest captured operator step.
    /// PT: Indice baseado em zero do passo de operador mais lento capturado.
    /// </summary>
    public int SlowestStepIndex { get; }

    /// <summary>
    /// EN: Optional details captured for the slowest operator step.
    /// PT: Detalhes opcionais capturados para o passo de operador mais lento.
    /// </summary>
    public string? SlowestStepDetails { get; }

    /// <summary>
    /// EN: Operator with the lowest captured execution time in this trace.
    /// PT: Operador com o menor tempo de execucao capturado neste trace.
    /// </summary>
    public string FastestOperator { get; }

    /// <summary>
    /// EN: Zero-based index of the fastest captured operator step.
    /// PT: Indice baseado em zero do passo de operador mais rapido capturado.
    /// </summary>
    public int FastestStepIndex { get; }

    /// <summary>
    /// EN: Optional details captured for the fastest operator step.
    /// PT: Detalhes opcionais capturados para o passo de operador mais rapido.
    /// </summary>
    public string? FastestStepDetails { get; }

    /// <summary>
    /// EN: Operator with the highest captured output volume in this trace.
    /// PT: Operador com o maior volume de saida capturado neste trace.
    /// </summary>
    public string WidestOperator { get; }

    /// <summary>
    /// EN: Zero-based index of the widest captured operator step.
    /// PT: Indice baseado em zero do passo de operador com maior volume capturado.
    /// </summary>
    public int WidestStepIndex { get; }

    /// <summary>
    /// EN: Optional details captured for the widest operator step.
    /// PT: Detalhes opcionais capturados para o passo de operador com maior volume.
    /// </summary>
    public string? WidestStepDetails { get; }

    /// <summary>
    /// EN: Operator with the lowest captured output volume in this trace.
    /// PT: Operador com o menor volume de saida capturado neste trace.
    /// </summary>
    public string NarrowestOperator { get; }

    /// <summary>
    /// EN: Zero-based index of the narrowest captured operator step.
    /// PT: Indice baseado em zero do passo de operador com menor volume capturado.
    /// </summary>
    public int NarrowestStepIndex { get; }

    /// <summary>
    /// EN: Optional details captured for the narrowest operator step.
    /// PT: Detalhes opcionais capturados para o passo de operador com menor volume.
    /// </summary>
    public string? NarrowestStepDetails { get; }

    internal QueryDebugTrace WithStatementContext(int statementIndex, string? sqlText)
        => new(
            QueryType,
            statementIndex,
            sqlText,
            Steps);

    private static bool IsSlowerStep(QueryDebugTraceStep candidate, QueryDebugTraceStep current)
        => candidate.ExecutionTime > current.ExecutionTime;

    private static bool IsFasterStep(QueryDebugTraceStep candidate, QueryDebugTraceStep current)
        => candidate.ExecutionTime < current.ExecutionTime;

    private static bool IsWiderStep(QueryDebugTraceStep candidate, QueryDebugTraceStep current)
        => candidate.OutputRows > current.OutputRows
            || (candidate.OutputRows == current.OutputRows && candidate.InputRows > current.InputRows);

    private static bool IsNarrowerStep(QueryDebugTraceStep candidate, QueryDebugTraceStep current)
        => candidate.OutputRows < current.OutputRows
            || (candidate.OutputRows == current.OutputRows && candidate.InputRows < current.InputRows);
}

/// <summary>
/// EN: Describes one runtime operator step captured by <see cref="QueryDebugTrace"/>.
/// PT: Descreve um passo de operador em runtime capturado por <see cref="QueryDebugTrace"/>.
/// </summary>
public sealed class QueryDebugTraceStep
{
    /// <summary>
    /// EN: Initializes one runtime operator step with row counts, elapsed time and details.
    /// PT: Inicializa um passo de operador em runtime com contagens de linhas, tempo decorrido e detalhes.
    /// </summary>
    /// <param name="operatorName">EN: Logical operator name, such as TableScan, Filter or Limit. PT: Nome logico do operador, como TableScan, Filter ou Limit.</param>
    /// <param name="inputRows">EN: Number of rows received by the operator. PT: Numero de linhas recebidas pelo operador.</param>
    /// <param name="outputRows">EN: Number of rows produced by the operator. PT: Numero de linhas produzidas pelo operador.</param>
    /// <param name="executionTime">EN: Time spent by the operator during runtime capture. PT: Tempo gasto pelo operador durante a captura em runtime.</param>
    /// <param name="details">EN: Optional human-readable detail about the operator. PT: Detalhe opcional legivel sobre o operador.</param>
    public QueryDebugTraceStep(
        string operatorName,
        int inputRows,
        int outputRows,
        TimeSpan executionTime,
        string? details = null)
    {
        Operator = operatorName;
        InputRows = inputRows;
        OutputRows = outputRows;
        ExecutionTime = executionTime;
        Details = details;
    }

    /// <summary>
    /// EN: Logical operator name captured for this runtime step.
    /// PT: Nome logico do operador capturado neste passo de runtime.
    /// </summary>
    public string Operator { get; }

    /// <summary>
    /// EN: Number of rows received by the operator.
    /// PT: Numero de linhas recebidas pelo operador.
    /// </summary>
    public int InputRows { get; }

    /// <summary>
    /// EN: Number of rows produced by the operator.
    /// PT: Numero de linhas produzidas pelo operador.
    /// </summary>
    public int OutputRows { get; }

    /// <summary>
    /// EN: Elapsed time captured for the operator execution.
    /// PT: Tempo decorrido capturado para a execucao do operador.
    /// </summary>
    public TimeSpan ExecutionTime { get; }

    /// <summary>
    /// EN: Optional detail that helps explain the operator step.
    /// PT: Detalhe opcional que ajuda a explicar o passo do operador.
    /// </summary>
    public string? Details { get; }
}
