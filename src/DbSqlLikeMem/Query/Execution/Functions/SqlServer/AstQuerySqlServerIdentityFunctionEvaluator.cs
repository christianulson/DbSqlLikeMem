namespace DbSqlLikeMem;

internal sealed class AstQuerySqlServerIdentityFunctionEvaluator(
    Func<ISqlDialect?> getDialect,
    Func<object?> getLastInsertId,
    Func<string?, int?> resolveSystemTypeId,
    Func<object?, string?> resolveSystemTypeName)
{
    private readonly Func<ISqlDialect?> _getDialect = getDialect ?? throw new ArgumentNullException(nameof(getDialect));
    private readonly Func<object?> _getLastInsertId = getLastInsertId ?? throw new ArgumentNullException(nameof(getLastInsertId));
    private readonly Func<string?, int?> _resolveSystemTypeId = resolveSystemTypeId ?? throw new ArgumentNullException(nameof(resolveSystemTypeId));
    private readonly Func<object?, string?> _resolveSystemTypeName = resolveSystemTypeName ?? throw new ArgumentNullException(nameof(resolveSystemTypeName));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var dialect = _getDialect();
        if (dialect is null || !dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
            return false;

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("SCHEMA_ID" or "SCHEMA_NAME" or "SCOPE_IDENTITY" or "SUSER_ID" or "SUSER_SID" or "SUSER_NAME" or "SUSER_SNAME" or "TYPE_ID" or "TYPE_NAME" or "USER_ID" or "USER_NAME"))
            return false;

        result = name switch
        {
            "SCHEMA_ID" => 1,
            "SCHEMA_NAME" => "dbo",
            "SCOPE_IDENTITY" => _getLastInsertId(),
            "SUSER_ID" => 1,
            "SUSER_SID" => new byte[] { 0x01 },
            "SUSER_NAME" or "SUSER_SNAME" => "sa",
            "TYPE_ID" => _resolveSystemTypeId(evalArg(0)?.ToString()),
            "TYPE_NAME" => _resolveSystemTypeName(evalArg(0)),
            "USER_ID" => 1,
            "USER_NAME" => "dbo",
            _ => null
        };

        return true;
    }
}
