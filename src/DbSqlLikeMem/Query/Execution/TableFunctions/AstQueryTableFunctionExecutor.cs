namespace DbSqlLikeMem;

internal sealed class AstQueryTableFunctionExecutor
{
    private readonly AstQueryOpenJsonTableFunctionHandler _openJsonHandler;
    private readonly AstQueryStringSplitTableFunctionHandler _stringSplitHandler;
    private readonly AstQueryJsonTableFunctionHandler _jsonTableHandler;
    private readonly Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>> _tableFunctionHandlers;

    internal AstQueryTableFunctionExecutor(
        Func<ISqlDialect?> dialectAccessor,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialectAccessor, nameof(dialectAccessor));
        ArgumentNullExceptionCompatible.ThrowIfNull(evalExpression, nameof(evalExpression));

        _openJsonHandler = new AstQueryOpenJsonTableFunctionHandler(dialectAccessor, evalExpression);
        _stringSplitHandler = new AstQueryStringSplitTableFunctionHandler(dialectAccessor, evalExpression);
        _jsonTableHandler = new AstQueryJsonTableFunctionHandler(dialectAccessor, evalExpression);
        _tableFunctionHandlers = new Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>>(StringComparer.OrdinalIgnoreCase)
        {
            [SqlConst.OPENJSON] = ExecuteOpenJsonTableFunction,
            [SqlConst.STRING_SPLIT] = ExecuteStringSplitTableFunction,
            [SqlConst.JSON_TABLE] = ExecuteJsonTableFunction
        };
    }

    internal TableResultMock Execute(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("Table function source is missing function metadata.");
        if (!_tableFunctionHandlers.TryGetValue(function.Name, out var handler))
            throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");

        return handler(tableSource, ctes, outerRow);
    }

    private TableResultMock ExecuteOpenJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _openJsonHandler.Execute(tableSource, ctes, outerRow);

    private TableResultMock ExecuteStringSplitTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _stringSplitHandler.Execute(tableSource, ctes, outerRow);

    private TableResultMock ExecuteJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _jsonTableHandler.Execute(tableSource, ctes, outerRow);
}
