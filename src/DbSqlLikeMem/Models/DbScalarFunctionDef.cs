namespace DbSqlLikeMem.Models;

internal sealed record DbScalarFunctionParameterDef(
    string Name,
    string TypeSql,
    bool Required)
{
    internal string NormalizedName => ProcedureDef.NormalizeParamName(Name);
}

internal enum SqlScalarFunctionUsageKind
{
    Call,
    Identifier,
    CallOrIdentifier
}

internal sealed record DbScalarFunctionDef(
    string Name,
    string ReturnTypeSql,
    IReadOnlyList<DbScalarFunctionParameterDef> Parameters,
    Func<SqlExpr, object> fnBody,
    SqlScalarFunctionUsageKind UsageKind = SqlScalarFunctionUsageKind.Call,
    SqlTemporalFunctionKind? TemporalKind = null,
    bool IsStringAggregate = false)
    : ProcessDef(Name)
{
    internal AstQueryGeneralScalarFunctionHandler? AstExecutor { get; init; }

    internal bool AllowsCall
        => UsageKind is SqlScalarFunctionUsageKind.Call
        or SqlScalarFunctionUsageKind.CallOrIdentifier;

    internal bool AllowsIdentifier
        => UsageKind is SqlScalarFunctionUsageKind.Identifier
        or SqlScalarFunctionUsageKind.CallOrIdentifier;
}
