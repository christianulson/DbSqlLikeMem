namespace DbSqlLikeMem;

internal sealed class QueryDebugTraceBuilder(string queryType)
{
    private readonly List<QueryDebugTraceStep> _steps = [];

    public void AddStep(
        string operatorName,
        int inputRows,
        int outputRows,
        TimeSpan executionTime,
        string? details = null)
    {
        _steps.Add(new QueryDebugTraceStep(
            operatorName,
            inputRows,
            outputRows,
            executionTime,
            details));
    }

    public QueryDebugTrace Build()
        => new(queryType, 0, null, _steps.AsReadOnly());
}
