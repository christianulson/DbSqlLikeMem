namespace DbSqlLikeMem;

using System;

internal static class AstQueryPostgresUuidFunctionEvaluator
{
    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        out object? result)
    {
        _ = context;
        result = Guid.NewGuid().ToString("D");
        return true;
    }
}
