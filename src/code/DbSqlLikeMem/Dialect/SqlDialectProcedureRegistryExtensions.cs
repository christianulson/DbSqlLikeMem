namespace DbSqlLikeMem;

internal static class SqlDialectProcedureRegistryExtensions
{
    internal static bool TryGetProcedureDefinition(
        this ISqlDialect dialect,
        string procedureName,
        out ProcedureDef? definition)
    {
        if (string.IsNullOrWhiteSpace(procedureName))
        {
            definition = null;
            return false;
        }

        return dialect.Procedures.TryGetValue(procedureName.NormalizeName(), out definition);
    }

    internal static ProcedureDef? GetProcedureDefinitionOrNull(
        this ISqlDialect dialect,
        string procedureName)
        => dialect.TryGetProcedureDefinition(procedureName, out var definition)
            ? definition
            : null;

    internal static void AddProcedure(
        this ISqlDialect dialect,
        string procedureName,
        ProcedureDef procedure)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedureName, nameof(procedureName));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));

        dialect.Procedures[procedureName.NormalizeName()] = procedure;
    }

    internal static void AddProcedures(
        this ISqlDialect dialect,
        ProcedureDef procedure,
        params string[] procedureNames)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedure, nameof(procedure));
        ArgumentNullExceptionCompatible.ThrowIfNull(procedureNames, nameof(procedureNames));

        foreach (var procedureName in procedureNames)
            dialect.AddProcedure(procedureName, procedure);
    }

}
