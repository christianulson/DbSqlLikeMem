namespace DbSqlLikeMem;

internal delegate bool AstQueryGeneralScalarFunctionHandler(
    QueryExecutionContext context,
    FunctionCallExpr fn,
    Func<int, object?> evalArg,
    out object? result);
