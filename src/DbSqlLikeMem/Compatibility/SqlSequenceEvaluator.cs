using System.Globalization;

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
            if (TryEvaluateLastVal(connection, functionName, args, out value))
                return true;

            return false;
        }

        value = functionName.ToUpperInvariant() switch
        {
            "NEXT_VALUE_FOR" or "NEXTVAL" => connection.GetNextSequenceValue(sequenceRef!),
            "CURRVAL" or "PREVIOUS_VALUE_FOR" => connection.GetCurrentSequenceValue(sequenceRef!),
            "SETVAL" => connection.SetSequenceValue(sequenceRef!, args, evalArg),
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

        var targetSchema = sequenceRef!.SchemaName ?? table.Schema.SchemaName;
        if (!table.Schema.Db.TryGetSequence(sequenceRef.SequenceName, out var sequence, targetSchema) || sequence is null)
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        if (!functionName.Equals("NEXT_VALUE_FOR", StringComparison.OrdinalIgnoreCase)
            && !functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase))
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

        var trimmed = sequenceName!.Trim().Trim('\'', '"');
        var parts = trimmed
            .Split('.').Select(s => s.Trim()).Where(s => !string.IsNullOrWhiteSpace(s))
            .Select(part => part.Trim('\'', '"').NormalizeName())
            .Where(part => !string.IsNullOrWhiteSpace(part))
            .ToArray();

        if (parts.Length == 0)
            return null;

        return parts.Length == 1
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
                || functionName.Equals("NEXTVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("CURRVAL", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("PREVIOUS_VALUE_FOR", StringComparison.OrdinalIgnoreCase))
            && args.Count == 1)
        {
            sequenceRef = TryReadSequenceReference(args[0], evalArg);
            return sequenceRef is not null;
        }

        if (functionName.Equals("SETVAL", StringComparison.OrdinalIgnoreCase)
            && (args.Count == 2 || args.Count == 3))
        {
            sequenceRef = TryReadSequenceReference(args[0], evalArg);
            return sequenceRef is not null;
        }

        return false;
    }

    private static long GetNextSequenceValue(this DbConnectionMockBase connection, SequenceReference sequenceRef)
    {
        if (!connection.TryGetSequence(sequenceRef.SequenceName, out var sequence, sequenceRef.SchemaName) || sequence is null)
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        var value = sequence.NextValue();
        connection.SetSessionSequenceValue(sequenceRef.SequenceName, value, sequenceRef.SchemaName);
        return value;
    }

    private static long GetCurrentSequenceValue(this DbConnectionMockBase connection, SequenceReference sequenceRef)
    {
        if (!connection.TryGetSequence(sequenceRef.SequenceName, out var sequence, sequenceRef.SchemaName) || sequence is null)
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        if (!connection.TryGetSessionSequenceValue(sequenceRef.SequenceName, out var value, sequenceRef.SchemaName))
            throw new InvalidOperationException($"currval of sequence '{sequenceRef.DisplayName}' is not yet defined in this session.");

        return value;
    }

    private static long SetSequenceValue(
        this DbConnectionMockBase connection,
        SequenceReference sequenceRef,
        IReadOnlyList<SqlExpr> args,
        Func<SqlExpr, object?> evalArg)
    {
        if (!connection.TryGetSequence(sequenceRef.SequenceName, out var sequence, sequenceRef.SchemaName) || sequence is null)
            throw new InvalidOperationException($"Sequence not found: {sequenceRef.DisplayName}");

        var value = Convert.ToInt64(evalArg(args[1]), CultureInfo.InvariantCulture);
        var isCalled = args.Count < 3 || Convert.ToBoolean(evalArg(args[2]), CultureInfo.InvariantCulture);
        var result = sequence.SetValue(value, isCalled);
        if (isCalled)
            connection.SetSessionSequenceValue(sequenceRef.SequenceName, result, sequenceRef.SchemaName);

        return result;
    }

    private static bool TryEvaluateLastVal(
        DbConnectionMockBase connection,
        string functionName,
        IReadOnlyList<SqlExpr> args,
        out object? value)
    {
        value = null;
        if (!functionName.Equals("LASTVAL", StringComparison.OrdinalIgnoreCase) || args.Count != 0)
            return false;

        if (!connection.TryGetLastSessionSequenceValue(out var currentValue))
            throw new InvalidOperationException("lastval is not yet defined in this session.");

        value = currentValue;
        return true;
    }

    private sealed record SequenceReference(string? SchemaName, string SequenceName)
    {
        public string DisplayName => string.IsNullOrWhiteSpace(SchemaName)
            ? SequenceName
            : $"{SchemaName}.{SequenceName}";
    }
}
