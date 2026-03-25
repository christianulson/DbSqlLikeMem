namespace DbSqlLikeMem;

using System;

internal static class AstQueryPostgresUuidFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        out object? result)
    {
        if (!fn.Name.Equals("GEN_RANDOM_UUID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = Guid.NewGuid().ToString("D");
        return true;
    }
}
