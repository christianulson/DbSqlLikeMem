namespace DbSqlLikeMem;

internal static class SqlFunctionBodyFactory
{
    internal static Func<SqlExpr, object> Identity()
        => static body => body;

    internal static Func<SqlExpr, object> CanonicalName(string canonicalName)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(canonicalName, nameof(canonicalName));

        return body => body is FunctionCallExpr call
            ? call with { Name = canonicalName }
            : body;
    }
}
