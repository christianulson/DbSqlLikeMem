using DbSqlLikeMem;
using DbSqlLikeMem.Models;
using DbSqlLikeMem.Query.Functions.Common;

namespace DbSqlLikeMem.Firebird;

internal static class FirebirdScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        _ = version;

        SqlSharedScalarFunctionRegistry.Register(dialect);
        RegisterTemporalFunctions(dialect);
        RegisterControlFlowFunctions(dialect);
        RegisterContextFunctions(dialect);
        RegisterSequenceFunctions(dialect);
        RegisterBinaryFunctions(dialect);
        RegisterFirebirdSpecificFunctions(dialect);
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect)
    {
        RegisterTemporalIdentifier(dialect, "CURRENT_DATE", "DATE", SqlTemporalFunctionKind.Date);
        RegisterTemporalIdentifier(dialect, "CURRENT DATE", "DATE", SqlTemporalFunctionKind.Date);
        RegisterTemporalIdentifier(dialect, "CURRENT_TIME", "TIME", SqlTemporalFunctionKind.Time);
        RegisterTemporalIdentifier(dialect, "CURRENT TIME", "TIME", SqlTemporalFunctionKind.Time);
        RegisterTemporalIdentifier(dialect, "CURRENT_TIMESTAMP", "TIMESTAMP", SqlTemporalFunctionKind.DateTime);
        RegisterTemporalIdentifier(dialect, "CURRENT TIMESTAMP", "TIMESTAMP", SqlTemporalFunctionKind.DateTime);
        RegisterTemporalIdentifier(dialect, "LOCALTIME", "TIME", SqlTemporalFunctionKind.Time);
        RegisterTemporalIdentifier(dialect, "LOCALTIMESTAMP", "TIMESTAMP", SqlTemporalFunctionKind.DateTime);
        RegisterTemporalIdentifier(dialect, "NOW", "TIMESTAMP", SqlTemporalFunctionKind.DateTime);
        RegisterTemporalIdentifier(dialect, "TODAY", "DATE", SqlTemporalFunctionKind.Date);
        RegisterTemporalIdentifier(dialect, "TOMORROW", "DATE", SqlTemporalFunctionKind.Date);
        RegisterTemporalIdentifier(dialect, "YESTERDAY", "DATE", SqlTemporalFunctionKind.Date);

        RegisterScalar(dialect, "DATEADD", "TIMESTAMP", DbFunctionCategory.DateTime);
    }

    private static void RegisterControlFlowFunctions(ISqlDialect dialect)
    {
        RegisterScalar(
            dialect,
            "IIF",
            "VARCHAR",
            DbFunctionCategory.ControlFlow,
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
        RegisterScalar(
            dialect,
            "DECODE",
            "VARCHAR",
            DbFunctionCategory.ControlFlow,
            QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions);
    }

    private static void RegisterContextFunctions(ISqlDialect dialect)
    {
        RegisterScalar(
            dialect,
            "RDB$GET_CONTEXT",
            "VARCHAR",
            DbFunctionCategory.System,
            AstQueryFirebirdContextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "RDB$SET_CONTEXT",
            "INT",
            DbFunctionCategory.System,
            AstQueryFirebirdContextFunctionEvaluator.TryEvaluate);
    }

    private static void RegisterSequenceFunctions(ISqlDialect dialect)
    {
        RegisterScalar(dialect, "GEN_ID", "BIGINT", DbFunctionCategory.System);
    }

    private static void RegisterBinaryFunctions(ISqlDialect dialect)
    {
        RegisterScalar(
            dialect,
            "BASE64_ENCODE",
            "VARCHAR",
            DbFunctionCategory.String,
            AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BASE64_DECODE",
            "VARBINARY",
            DbFunctionCategory.Conversion,
            AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "HEX_ENCODE",
            "VARCHAR",
            DbFunctionCategory.String,
            AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "HEX_DECODE",
            "VARBINARY",
            DbFunctionCategory.Conversion,
            AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "CRYPT_HASH",
            "VARBINARY",
            DbFunctionCategory.Conversion,
            AstQuerySharedBinaryTextFunctionEvaluator.TryEvaluate);
    }

    private static void RegisterFirebirdSpecificFunctions(ISqlDialect dialect)
    {
        RegisterScalar(
            dialect,
            "ASCII_CHAR",
            "VARCHAR",
            DbFunctionCategory.String,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "ASCII_VAL",
            "INT",
            DbFunctionCategory.String,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "UNICODE_CHAR",
            "VARCHAR",
            DbFunctionCategory.String,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "UNICODE_VAL",
            "INT",
            DbFunctionCategory.String,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "CHAR_TO_UUID",
            "VARBINARY",
            DbFunctionCategory.Conversion,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "UUID_TO_CHAR",
            "VARCHAR",
            DbFunctionCategory.Conversion,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "GEN_UUID",
            "VARBINARY",
            DbFunctionCategory.System,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "HASH",
            "BIGINT",
            DbFunctionCategory.System,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "TRUNC",
            "DECIMAL",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_AND",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_OR",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_XOR",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_NOT",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_SHL",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "BIN_SHR",
            "BIGINT",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "MAXVALUE",
            "DOUBLE",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "MINVALUE",
            "DOUBLE",
            DbFunctionCategory.Numeric,
            AstQueryFirebirdScalarFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "CHAR_LENGTH",
            "INT",
            DbFunctionCategory.String,
            AstQuerySharedTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "CHARACTER_LENGTH",
            "INT",
            DbFunctionCategory.String,
            AstQuerySharedTextFunctionEvaluator.TryEvaluate);
        RegisterScalar(
            dialect,
            "OVERLAY",
            "VARCHAR",
            DbFunctionCategory.String,
            AstQuerySharedTextFunctionEvaluator.TryEvaluate);
    }

    private static void RegisterTemporalIdentifier(
        ISqlDialect dialect,
        string name,
        string returnTypeSql,
        SqlTemporalFunctionKind kind)
        => dialect.AddScalarFunction(
            DbFunctionDef.CreateTemporal(name, returnTypeSql, kind, DbInvocationStyle.Identifier));

    private static void RegisterScalar(
        ISqlDialect dialect,
        string name,
        string returnTypeSql,
        DbFunctionCategory category = DbFunctionCategory.General,
        AstQueryGeneralScalarFunctionHandler? executor = null)
    {
        var definition = executor is null
            ? DbFunctionDef.CreateScalar(name, returnTypeSql, category)
            : DbFunctionDef.CreateScalar(name, returnTypeSql, executor, category);

        dialect.AddScalarFunction(definition);
    }
}
