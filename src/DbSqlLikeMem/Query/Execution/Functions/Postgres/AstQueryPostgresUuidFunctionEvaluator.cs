namespace DbSqlLikeMem;

using System;

internal static class AstQueryPostgresUuidFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        out object? result)
    {
        if (!dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!fn.Name.Equals("GEN_RANDOM_UUID", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = Guid.NewGuid().ToString("D");
        return true;
    }
}
