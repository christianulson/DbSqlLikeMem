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
        return functionName =>
        {
            if (!string.IsNullOrWhiteSpace(schemaName)
                && db.TryGetValue(schemaName, out var schema)
                && schema is SchemaMock schemaMock
                && schemaMock.TryGetFunction(functionName, out _))
            {
                return true;
            }

            foreach (var candidateSchema in db.Values)
            {
                if (candidateSchema is null)
                    continue;

                if (candidateSchema.TryGetFunction(functionName, out _))
                    return true;
            }

            return false;
        };
    }
}
