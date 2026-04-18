namespace DbSqlLikeMem;

internal sealed class AstQueryTableFunctionExecutor
{
    private readonly AstQueryOpenJsonTableFunctionHandler _openJsonHandler;
    private readonly AstQueryStringSplitTableFunctionHandler _stringSplitHandler;
    private readonly AstQueryJsonTableFunctionHandler _jsonTableHandler;
    private readonly AstQueryJsonEachTableFunctionHandler _jsonEachHandler;
    private readonly AstQueryJsonTreeTableFunctionHandler _jsonTreeHandler;
    private readonly Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>> _tableFunctionHandlers;

    internal AstQueryTableFunctionExecutor(
        QueryExecutionContext context,
        Func<SqlExpr, AstQueryExecutorBase.EvalRow, AstQueryExecutorBase.EvalGroup?, IDictionary<string, AstQueryExecutorBase.Source>, object?> evalExpression)
    {

        ArgumentNullExceptionCompatible.ThrowIfNull(evalExpression, nameof(evalExpression));

        _openJsonHandler = new AstQueryOpenJsonTableFunctionHandler(context, evalExpression);
        _stringSplitHandler = new AstQueryStringSplitTableFunctionHandler(context, evalExpression);
        _jsonTableHandler = new AstQueryJsonTableFunctionHandler(context, evalExpression);
        _jsonEachHandler = new AstQueryJsonEachTableFunctionHandler(context, evalExpression);
        _jsonTreeHandler = new AstQueryJsonTreeTableFunctionHandler(context, evalExpression);
        _tableFunctionHandlers = new Dictionary<string, Func<SqlTableSource, IDictionary<string, AstQueryExecutorBase.Source>, AstQueryExecutorBase.EvalRow?, TableResultMock>>(StringComparer.OrdinalIgnoreCase)
        {
            [SqlConst.OPENJSON] = ExecuteOpenJsonTableFunction,
            [SqlConst.STRING_SPLIT] = ExecuteStringSplitTableFunction,
            [SqlConst.JSON_TABLE] = ExecuteJsonTableFunction,
            ["json_each"] = ExecuteJsonEachTableFunction,
            ["json_tree"] = ExecuteJsonTreeTableFunction
        };
    }

    internal TableResultMock Execute(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
    {
        var function = tableSource.TableFunction ?? throw new InvalidOperationException("Table function source is missing function metadata.");
        if (function.ResolvedTableFunction?.TableExecutor is not null)
            return function.ResolvedTableFunction.TableExecutor(this, tableSource, ctes, outerRow);

        if (!_tableFunctionHandlers.TryGetValue(function.Name, out var handler))
            throw new NotSupportedException($"Table-valued function '{function.Name}' not supported yet in the mock.");

        return handler(tableSource, ctes, outerRow);
    }

    internal TableResultMock ExecuteOpenJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _openJsonHandler.Execute(tableSource, ctes, outerRow);

    internal TableResultMock ExecuteStringSplitTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _stringSplitHandler.Execute(tableSource, ctes, outerRow);

    internal TableResultMock ExecuteJsonTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _jsonTableHandler.Execute(tableSource, ctes, outerRow);

    internal TableResultMock ExecuteJsonEachTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _jsonEachHandler.Execute(tableSource, ctes, outerRow);

    internal TableResultMock ExecuteJsonTreeTableFunction(
        SqlTableSource tableSource,
        IDictionary<string, AstQueryExecutorBase.Source> ctes,
        AstQueryExecutorBase.EvalRow? outerRow)
        => _jsonTreeHandler.Execute(tableSource, ctes, outerRow);
}
