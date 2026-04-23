using DbSqlLikeMem;
using DbSqlLikeMem.Models;

namespace DbSqlLikeMem.MariaDb;

internal static partial class MariaDbScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        RegisterGeneratedScalarFunctions(dialect);
        dialect.AddScalarFunction(new DbFunctionDef(SqlConst.SUM, null, DbFunctionCapability.Aggregate)
        {
            PromotesIntegralInputsToDecimal = true
        });
        dialect.AddScalarFunction(new DbFunctionDef(SqlConst.AVG, null, DbFunctionCapability.Aggregate)
        {
            PromotesIntegralInputsToDecimal = true
        });
    }

    [ScalarFunction("LENGTHB", "BIGINT")]
    [ScalarFunction("DECODE_ORACLE", "VARCHAR")]
    [ScalarFunction("NATURAL_SORT_KEY", "VARCHAR")]
    [ScalarFunction("SFORMAT", "VARCHAR")]
    [ScalarFunction("KDF", "VARBINARY")]
    [ScalarFunction("TRIM_ORACLE", "VARCHAR")]
    [ScalarFunction("WEIGHT_STRING", "VARBINARY")]
    [ScalarFunction("JSON_COMPACT", "VARCHAR")]
    [ScalarFunction("JSON_PRETTY", "VARCHAR")]
    [ScalarFunction("JSON_DETAILED", "VARCHAR")]
    [ScalarFunction("JSON_LOOSE", "VARCHAR")]
    [ScalarFunction("JSON_NORMALIZE", "VARCHAR")]
    [ScalarFunction("JSON_EQUALS", "INT")]
    [ScalarFunction("JSON_EXISTS", "INT")]
    [ScalarFunction("JSON_SCHEMA_VALID", "INT")]
    [ScalarFunction("JSON_ARRAY_INTERSECT", "VARCHAR")]
    [ScalarFunction("JSON_OBJECT_FILTER_KEYS", "VARCHAR")]
    [ScalarFunction("JSON_OBJECT_TO_ARRAY", "VARCHAR")]
    [ScalarFunction("JSON_KEY_VALUE", "VARCHAR")]
    private static bool TryEvalMariaDbFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbFunctionHelper.TryEvalFunctions(context, fn, evalArg, out result);

    [ScalarFunction("CRC32C", "BIGINT", MinVersion = MariaDbDialect.Crc32cMinVersion)]
    private static bool TryEvalGeneratedCrc32cFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbFunctionHelper.TryEvalFunctions(context, fn, evalArg, out result);

    [ScalarFunction("COLUMN_CREATE", "VARBINARY")]
    [ScalarFunction("COLUMN_ADD", "VARBINARY")]
    [ScalarFunction("COLUMN_DELETE", "VARBINARY")]
    [ScalarFunction("VECTOR", "VARBINARY")]
    [ScalarFunction("VEC_FROMTEXT", "VARBINARY")]
    private static bool TryEvalMariaDbVarbinaryFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions(context, fn, evalArg, out result);

    [ScalarFunction("COLUMN_EXISTS", "INT")]
    [ScalarFunction("COLUMN_CHECK", "INT")]
    [ScalarFunction("WSREP_SYNC_WAIT_UPTO_GTID", "INT")]
    private static bool TryEvalMariaDbIntFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions(context, fn, evalArg, out result);

    [ScalarFunction("COLUMN_JSON", "VARCHAR")]
    [ScalarFunction("COLUMN_LIST", "VARCHAR")]
    [ScalarFunction("COLUMN_GET", "VARCHAR")]
    [ScalarFunction("VEC_TOTEXT", "VARCHAR")]
    [ScalarFunction("WSREP_LAST_SEEN_GTID", "VARCHAR")]
    [ScalarFunction("WSREP_LAST_WRITTEN_GTID", "VARCHAR")]
    private static bool TryEvalMariaDbVarcharFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions(context, fn, evalArg, out result);

    [ScalarFunction("VEC_DISTANCE", "DOUBLE")]
    [ScalarFunction("VEC_DISTANCE_EUCLIDEAN", "DOUBLE")]
    [ScalarFunction("VEC_DISTANCE_COSINE", "DOUBLE")]
    private static bool TryEvalMariaDbDoubleFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => QueryMariaDbSpecialFunctionHelper.TryEvalSpecialFunctions(context, fn, evalArg, out result);

    [ScalarFunction("NEXT_VALUE_FOR", "BIGINT")]
    [ScalarFunction("PREVIOUS_VALUE_FOR", "BIGINT")]
    private static bool TryEvalSequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = evalArg;
        return SqlSequenceEvaluator.TryEvaluateCall(
            context.Connection,
            fn.Name,
            fn.Args,
            expr => ResolveSequenceArgValue(fn.Args, expr, evalArg),
            out result);
    }

    private static object? ResolveSequenceArgValue(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        return expr switch
        {
            LiteralExpr lit => lit.Value,
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => string.IsNullOrWhiteSpace(col.Qualifier) ? col.Name : $"{col.Qualifier}.{col.Name}",
            _ => ResolveSequenceArgValueByReference(args, expr, evalArg)
        };
    }

    private static object? ResolveSequenceArgValueByReference(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        for (var i = 0; i < args.Count; i++)
        {
            if (ReferenceEquals(args[i], expr))
                return evalArg(i);
        }

        return null;
    }

    static partial void RegisterGeneratedScalarFunctions(ISqlDialect dialect);
}
