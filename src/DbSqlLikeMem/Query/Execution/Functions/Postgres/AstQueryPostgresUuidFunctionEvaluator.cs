namespace DbSqlLikeMem;

using System;

internal static class AstQueryPostgresUuidFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        out object? result)
    {
        result = Guid.NewGuid().ToString("D");
        return true;
    }
}
