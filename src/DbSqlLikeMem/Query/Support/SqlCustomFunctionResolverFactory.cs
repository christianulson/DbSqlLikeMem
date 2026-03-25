namespace DbSqlLikeMem;

internal static class SqlCustomFunctionResolverFactory
{
    internal static Func<string, bool>? Create(QueryExecutionContext context)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(context, nameof(context));
        return Create(context.Connection.Db, context.Connection.Database);
    }

    internal static Func<string, bool>? Create(DbMock db, string schemaName)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        if (string.IsNullOrWhiteSpace(schemaName))
            return null;

        if (!db.TryGetValue(schemaName, out var schema) || schema is not SchemaMock schemaMock)
            return null;

        if (schemaMock.ScalarFunctions.Count == 0)
            return null;

        return functionName => schemaMock.TryGetFunction(functionName, out _);
    }
}
