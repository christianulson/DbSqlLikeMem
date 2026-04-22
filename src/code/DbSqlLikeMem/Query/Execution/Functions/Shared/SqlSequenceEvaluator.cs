namespace DbSqlLikeMem;

internal static class SqlSequenceEvaluator
{
    public static bool TryEvaluateCall(
        DbConnectionMockBase connection,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg,
        out object? value)
    {
        value = null;
        if (connection is null)
            return false;

        if (!TryResolveSequenceReference(functionName, args, evalArg, out var sequenceRef))
        {
            return TryEvaluateLastVal(connection, functionName, args, out value);
        }

        value = functionName.ToUpperInvariant() switch
        {
            "NEXT_VALUE_FOR" or SqlConst.NEXTVAL => connection.GetNextSequenceValue(sequenceRef!),
            SqlConst.CURRVAL or "PREVIOUS_VALUE_FOR" => connection.GetCurrentSequenceValue(sequenceRef!),
            SqlConst.SETVAL => connection.SetSequenceValue(sequenceRef!, args, evalArg),
            "GEN_ID" => connection.EvaluateGenId(sequenceRef!, args, evalArg),
            _ => null
        };

        return value is not null;
    }

    public static bool TryEvaluateCall(
        ITableMock table,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg,
        out object? value)
    {
        value = null;
        if (table is null)
            return false;

        if (!TryResolveSequenceReference(functionName, args, evalArg, out var sequenceRef))
            return false;

        if (!table.Schema.Db.TryGetSequence(sequenceRef!.SequenceName, out var sequence, sequenceRef.SchemaName ?? table.Schema.SchemaName) || sequence is null)
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        if (!functionName.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals(SqlConst.NEXTVAL, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"{functionName} requires a connection-scoped session context.");
        }

        value = sequence.NextValue();
        return true;
    }

    private static SequenceReference? TryReadSequenceReference(SqlExpr expr, Func<SqlExpr, object?> evalArg)
    {
        return expr switch
        {
            IdentifierExpr id => NormalizeSequenceReference(id.Name),
            ColumnExpr c => NormalizeSequenceReference(string.IsNullOrWhiteSpace(c.Qualifier) ? c.Name : $"{c.Qualifier}.{c.Name}"),
            LiteralExpr lit when lit.Value is not null => NormalizeSequenceReference(Convert.ToString(lit.Value, CultureInfo.InvariantCulture)),
            RawSqlExpr raw => NormalizeSequenceReference(raw.Sql),
            _ => NormalizeSequenceReference(Convert.ToString(evalArg(expr), CultureInfo.InvariantCulture))
        };
    }

    private static SequenceReference? NormalizeSequenceReference(string? sequenceName)
    {
        if (string.IsNullOrWhiteSpace(sequenceName))
            return null;

        var trimmed = StringCompatibility.Trim(sequenceName.AsSpan(), '\'', '"');
        var parts = new List<string>(4);
        var start = 0;
        while (start < trimmed.Length)
        {
            var remaining = trimmed[start..];
            var dot = remaining.IndexOf('.');
            var segment = dot >= 0
                ? remaining[..dot]
                : remaining;

            segment = StringCompatibility.Trim(segment, '\'', '"');
            if (segment.Length > 0)
                parts.Add(segment.NormalizeName());

            if (dot < 0)
                break;

            start += dot + 1;
        }

        if (parts.Count == 0)
            return null;

        return parts.Count == 1
            ? new SequenceReference(null, parts[0])
            : new SequenceReference(parts[^2], parts[^1]);
    }

    private static bool TryResolveSequenceReference(
        string functionName,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg,
        out SequenceReference? sequenceRef)
    {
        sequenceRef = null;
        if ((functionName.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals(SqlConst.NEXTVAL, StringComparison.OrdinalIgnoreCase)
                || functionName.Equals(SqlConst.CURRVAL, StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase))
            && args.Count == 1)
        {
            sequenceRef = TryReadSequenceReference(args[0], evalArg);
            return sequenceRef is not null;
        }

        if (functionName.Equals(SqlConst.SETVAL, StringComparison.OrdinalIgnoreCase)
            && (args.Count == 2 || args.Count == 3))
        {
            sequenceRef = TryReadSequenceReference(args[0], evalArg);
            return sequenceRef is not null;
        }

        if (functionName.Equals("GEN_ID", StringComparison.OrdinalIgnoreCase)
            && args.Count == 2)
        {
            sequenceRef = TryReadSequenceReference(args[0], evalArg);
            return sequenceRef is not null;
        }

        return false;
    }

    private static long GetNextSequenceValue(this DbConnectionMockBase connection, SequenceReference sequenceRef)
    {
        if (!TryGetSequence(connection, sequenceRef, out var sequence, out var resolvedSchemaName))
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        connection.CaptureSequenceStateForRollback(sequenceRef.SequenceName, resolvedSchemaName);
        var value = sequence!.NextValue();
        connection.SetSessionSequenceValue(sequenceRef.SequenceName, value, resolvedSchemaName);
        return value;
    }

    private static long GetCurrentSequenceValue(this DbConnectionMockBase connection, SequenceReference sequenceRef)
    {
        if (!TryGetSequence(connection, sequenceRef, out var _, out var resolvedSchemaName))
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        if (!connection.TryGetSessionSequenceValue(sequenceRef.SequenceName, out var value, resolvedSchemaName))
            throw new InvalidOperationException($"currval of sequence '{sequenceRef.DisplayName}' is not yet defined in this session.");

        return value;
    }

    private static long SetSequenceValue(
        this DbConnectionMockBase connection,
        SequenceReference sequenceRef,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg)
    {
        if (!TryGetSequence(connection, sequenceRef, out var sequence, out var resolvedSchemaName))
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        connection.CaptureSequenceStateForRollback(sequenceRef.SequenceName, resolvedSchemaName);
        var value = Convert.ToInt64(evalArg(args[1]), CultureInfo.InvariantCulture);
        var isCalled = args.Count < 3 || Convert.ToBoolean(evalArg(args[2]), CultureInfo.InvariantCulture);
        var result = sequence!.SetValue(value, isCalled);
        if (isCalled)
            connection.SetSessionSequenceValue(sequenceRef.SequenceName, result, resolvedSchemaName);

        return result;
    }

    private static long EvaluateGenId(
        this DbConnectionMockBase connection,
        SequenceReference sequenceRef,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg)
    {
        if (!TryGetSequence(connection, sequenceRef, out var sequence, out var resolvedSchemaName))
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        if (args.Count != 2)
            throw new InvalidOperationException("GEN_ID requires a sequence name and an increment value.");

        connection.CaptureSequenceStateForRollback(sequenceRef.SequenceName, resolvedSchemaName);

        var baseValue = sequence!.CurrentValue ?? 0L;
        var increment = Convert.ToInt64(evalArg(args[1]), CultureInfo.InvariantCulture);
        var result = sequence.SetValue(baseValue + increment, true);
        connection.SetSessionSequenceValue(sequenceRef.SequenceName, result, resolvedSchemaName);
        return result;
    }

    private static bool TryEvaluateLastVal(
        DbConnectionMockBase connection,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!functionName.Equals(SqlConst.LASTVAL, StringComparison.OrdinalIgnoreCase) || args.Count != 0)
            return false;

        if (!connection.TryGetLastSessionSequenceValue(out var currentValue))
            throw new InvalidOperationException("lastval is not yet defined in this session.");

        value = currentValue;
        return true;
    }

    private static bool TryGetSequence(
        DbConnectionMockBase connection,
        SequenceReference sequenceRef,
        out SequenceDef? sequence,
        out string? resolvedSchemaName)
    {
        if (connection.TryGetSequence(sequenceRef.SequenceName, out sequence, sequenceRef.SchemaName))
        {
            resolvedSchemaName = sequenceRef.SchemaName ?? connection.Database;
            return sequence is not null;
        }

        if (!string.IsNullOrWhiteSpace(sequenceRef.SchemaName))
        {
            resolvedSchemaName = sequenceRef.SchemaName;
            return sequence is not null;
        }

        if (connection.ProviderExecutionDialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            var candidates = new[]
            {
                connection.Database,
                "public",
                "DefaultSchema"
            };

            foreach (var candidate in candidates)
            {
                if (string.IsNullOrWhiteSpace(candidate))
                    continue;

                if (connection.Db.TryGetSequence(sequenceRef.SequenceName, out sequence, candidate) && sequence is not null)
                {
                    resolvedSchemaName = candidate;
                    return true;
                }
            }
        }

        sequence = null;
        resolvedSchemaName = sequenceRef.SchemaName ?? connection.Database;
        return false;
    }

    private sealed record SequenceReference(string? SchemaName, string SequenceName)
    {
        public string DisplayName => string.IsNullOrWhiteSpace(SchemaName)
            ? SequenceName
            : $"{SchemaName}.{SequenceName}";
    }
}
