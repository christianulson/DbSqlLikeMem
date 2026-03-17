namespace DbSqlLikeMem;

internal sealed record ScalarFunctionParameterDef(
    string Name,
    string TypeSql)
{
    internal string NormalizedName => ProcedureDef.NormalizeParamName(Name);
}

internal sealed record ScalarFunctionDef(
    string Name,
    string ReturnTypeSql,
    IReadOnlyList<ScalarFunctionParameterDef> Parameters,
    SqlExpr Body);