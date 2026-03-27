namespace DbSqlLikeMem;

internal sealed class AstQuerySqlServerDatabaseFunctionEvaluator(
    Func<string?, string?, object?> resolveDatabaseProperty,
    Func<string?, int?> resolveDatabasePrincipalId,
    Func<object?, string?, string?, object?> resolveColumnProperty,
    Func<string?, string?, int?> resolveColumnLength,
    Func<object?, object?, string?> resolveColumnName,
    Func<string?, int?> resolveObjectId,
    Func<object?, string?, object?> resolveObjectProperty,
    Func<object?, string?> resolveObjectName,
    Func<object?, string?> resolveObjectSchemaName,
    Func<string?, string?, object?> resolveTypeProperty,
    Func<string> getDatabaseName)
{
    private readonly Func<string?, string?, object?> _resolveDatabaseProperty = resolveDatabaseProperty ?? throw new ArgumentNullException(nameof(resolveDatabaseProperty));
    private readonly Func<string?, int?> _resolveDatabasePrincipalId = resolveDatabasePrincipalId ?? throw new ArgumentNullException(nameof(resolveDatabasePrincipalId));
    private readonly Func<object?, string?, string?, object?> _resolveColumnProperty = resolveColumnProperty ?? throw new ArgumentNullException(nameof(resolveColumnProperty));
    private readonly Func<string?, string?, int?> _resolveColumnLength = resolveColumnLength ?? throw new ArgumentNullException(nameof(resolveColumnLength));
    private readonly Func<object?, object?, string?> _resolveColumnName = resolveColumnName ?? throw new ArgumentNullException(nameof(resolveColumnName));
    private readonly Func<string?, int?> _resolveObjectId = resolveObjectId ?? throw new ArgumentNullException(nameof(resolveObjectId));
    private readonly Func<object?, string?, object?> _resolveObjectProperty = resolveObjectProperty ?? throw new ArgumentNullException(nameof(resolveObjectProperty));
    private readonly Func<object?, string?> _resolveObjectName = resolveObjectName ?? throw new ArgumentNullException(nameof(resolveObjectName));
    private readonly Func<object?, string?> _resolveObjectSchemaName = resolveObjectSchemaName ?? throw new ArgumentNullException(nameof(resolveObjectSchemaName));
    private readonly Func<string?, string?, object?> _resolveTypeProperty = resolveTypeProperty ?? throw new ArgumentNullException(nameof(resolveTypeProperty));
    private readonly Func<string> _getDatabaseName = getDatabaseName ?? throw new ArgumentNullException(nameof(getDatabaseName));

    internal bool TryEvaluate(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        var name = fn.Name.ToUpperInvariant();
        result = name switch
        {
            "DATABASEPROPERTYEX" => _resolveDatabaseProperty(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            "DATABASE_PRINCIPAL_ID" => _resolveDatabasePrincipalId(evalArg(0)?.ToString()),
            "COLUMNPROPERTY" => _resolveColumnProperty(evalArg(0), evalArg(1)?.ToString(), evalArg(2)?.ToString()),
            "COL_LENGTH" => _resolveColumnLength(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            "COL_NAME" => _resolveColumnName(evalArg(0), evalArg(1)),
            "DB_ID" => 1,
            "DB_NAME" => _getDatabaseName(),
            "OBJECT_ID" => _resolveObjectId(evalArg(0)?.ToString()),
            "OBJECTPROPERTY" => _resolveObjectProperty(evalArg(0), evalArg(1)?.ToString()),
            "OBJECTPROPERTYEX" => _resolveObjectProperty(evalArg(0), evalArg(1)?.ToString()),
            "OBJECT_NAME" => _resolveObjectName(evalArg(0)),
            "OBJECT_SCHEMA_NAME" => _resolveObjectSchemaName(evalArg(0)),
            "ORIGINAL_DB_NAME" => _getDatabaseName(),
            "TYPEPROPERTY" => _resolveTypeProperty(evalArg(0)?.ToString(), evalArg(1)?.ToString()),
            _ => null
        };

        return result is not null;
    }
}
