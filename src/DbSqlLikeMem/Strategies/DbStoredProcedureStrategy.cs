namespace DbSqlLikeMem;

internal static class DbStoredProcedureStrategy
{
    public static int ExecuteStoredProcedure(
        this DbConnectionMockBase connection,
        string procedureName,
        DbParameterCollection parameters)
    {
        if (string.IsNullOrWhiteSpace(procedureName))
            throw new InvalidOperationException("Procedure name not provided.");

        procedureName = procedureName.Trim().Trim('`').NormalizeName();

        if (!connection.TryGetProcedure(procedureName, out var def/*, extrair schemaName*/ ) || def == null)
            throw connection.NewException($"PROCEDURE {procedureName} does not exist", 1305);

        connection.ValidateProcedureParameters(def, parameters);
        PopulateOutDefaults(def, parameters);
        return 0; // signature-only
    }

    public static int ExecuteCall(
        this DbConnectionMockBase connection,
        string callSql,
        DbParameterCollection parameters)
    {
        // Supports: CALL proc(@p1, @p2)
        var m = Regex.Match(callSql.Trim(),
            @"^CALL\s+(`?)(?<name>[A-Za-z0-9_]+)\1\s*(\((?<args>.*)\))?\s*;?\s*$",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (!m.Success)
            throw new InvalidOperationException("Invalid CALL statement.");

        var proc = m.Groups["name"].Value;
        // NOTE: we ignore positional arg parsing for now; validation is done against provided parameters.
        return connection.ExecuteStoredProcedure(proc, parameters);
    }

    private static void ValidateProcedureParameters(
        this DbConnectionMockBase connection,
        ProcedureDef def,
        DbParameterCollection parameters)
    {
        // Build lookup by normalized name
        var dict = new Dictionary<string, DbParameter>(StringComparer.OrdinalIgnoreCase);
        foreach (DbParameter p in parameters)
        {
            var n = ProcedureDef.NormalizeParamName(p.ParameterName);
            if (string.IsNullOrWhiteSpace(n)) continue;
            dict[n] = p;
        }

        // required IN
        foreach (var req in def.RequiredIn)
        {
            var n = ProcedureDef.NormalizeParamName(req.Name);
            if (!dict.TryGetValue(n, out var p))
                throw connection.NewException("Incorrect number of arguments for PROCEDURE", 1318);

            var val = p.Value;
            if (val is null || val == DBNull.Value)
                throw connection.NewException($"Parameter '{n}' cannot be null", 1048);
        }

        // OUT params must exist if defined
        foreach (var o in def.OutParams)
        {
            var n = ProcedureDef.NormalizeParamName(o.Name);
            if (!dict.TryGetValue(n, out var p))
                throw connection.NewException("Incorrect number of arguments for PROCEDURE", 1318);

            if (p.Direction != ParameterDirection.Output && p.Direction != ParameterDirection.InputOutput)
                throw connection.NewException($"OUT parameter '{n}' must be Output", 1414);
        }
    }

    private static void PopulateOutDefaults(
        ProcedureDef def,
        DbParameterCollection parameters)
    {
        var dict = new Dictionary<string, DbParameter>(StringComparer.OrdinalIgnoreCase);
        foreach (DbParameter p in parameters)
        {
            var n = ProcedureDef.NormalizeParamName(p.ParameterName);
            if (string.IsNullOrWhiteSpace(n)) continue;
            dict[n] = p;
        }

        foreach (var o in def.OutParams)
        {
            var n = ProcedureDef.NormalizeParamName(o.Name);
            if (!dict.TryGetValue(n, out var p)) continue;

            // only set if not already set
            if (p.Value is not null && p.Value != DBNull.Value) continue;

            p.Value = o.DbType switch
            {
                DbType.Int16
                or DbType.Int32
                or DbType.Int64
                or DbType.Byte
                or DbType.SByte => 0,
                DbType.Decimal
                or DbType.Double
                or DbType.Single => 0m,
                DbType.Boolean => false,
                DbType.Guid => Guid.Empty,
                DbType.Date
                or DbType.DateTime
                or DbType.DateTime2 => DateTime.MinValue,
                _ => string.Empty
            };
        }
    }
}
