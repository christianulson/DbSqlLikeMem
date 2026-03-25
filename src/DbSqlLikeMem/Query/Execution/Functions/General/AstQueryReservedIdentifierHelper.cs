namespace DbSqlLikeMem;

internal static class AstQueryReservedIdentifierHelper
{
    internal static bool IsReservedWindowValueIdentifier(
        ISqlDialect? dialect,
        string name)
    {
        if (dialect is not null
            && dialect.TryGetScalarFunctionDefinition(name, out var definition)
            && definition is not null
            && definition.AllowsIdentifier)
        {
            return true;
        }

        if (name.Equals("_ROWID", StringComparison.OrdinalIgnoreCase))
            return true;

        if (name.Equals("USER", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ORA_INVOKING_USER", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ORA_INVOKING_USERID", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CURRENT_SCHEMA", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CURRENT_DATABASE", StringComparison.OrdinalIgnoreCase)
            || name.Equals("CURRENT_CATALOG", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return dialect is not null && dialect.SupportsSqlServerMetadataIdentifier(name);
    }
}
