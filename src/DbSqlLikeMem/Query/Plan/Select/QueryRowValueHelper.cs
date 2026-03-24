namespace DbSqlLikeMem;

internal static class QueryRowValueHelper
{
    internal static object? ResolveParam(IDataParameterCollection parameters, string name)
    {
        if (name == "?")
            return ResolvePositionalParam(parameters);

        return ResolveNamedParam(parameters, name);
    }

    internal static object? ResolveIdentifier(string name, AstQueryExecutorBase.EvalRow row)
    {
        if (name.Equals("_ROWID", StringComparison.OrdinalIgnoreCase)
            && TryResolveRowIdFromSources(row, out var rowId))
        {
            return rowId;
        }

        if (TrySplitQualifiedIdentifier(name, out var qualifier, out var columnName))
            return ResolveColumn(qualifier, columnName, row);

        return TryResolveIdentifierFromSources(name, row, out var value)
            ? value
            : TryResolveProjectedIdentifier(name, row, out value)
                ? value
                : null;
    }

    internal static object? ResolveColumn(
        string? qualifier,
        string col,
        AstQueryExecutorBase.EvalRow row)
    {
        col = col.NormalizeName();

        if (!string.IsNullOrWhiteSpace(qualifier))
            return ResolveQualifiedColumn(qualifier!, col, row);

        if (TryResolveUnqualifiedColumn(col, row, out var value))
            return value;

        return TryResolveColumnFromSources(col, row, out value)
            ? value
            : null;
    }

    internal static string NormalizeDistinctKey(object? value, ISqlDialect? dialect = null)
    {
        if (value is null or DBNull)
            return SqlConst.NULL;

        return value switch
        {
            decimal decimalValue => decimalValue.ToString(CultureInfo.InvariantCulture),
            double doubleValue => doubleValue.ToString("R", CultureInfo.InvariantCulture),
            float floatValue => floatValue.ToString("R", CultureInfo.InvariantCulture),
            DateTime dateTime => dateTime.ToUniversalTime().ToString("O", CultureInfo.InvariantCulture),
            bool boolValue => boolValue ? "1" : "0",
            string text => (dialect?.TextComparison ?? StringComparison.OrdinalIgnoreCase) == StringComparison.Ordinal
                ? text
                : text.ToUpperInvariant(),
            _ => value.ToString() ?? string.Empty
        };
    }

    internal static TableResultMock ApplyDistinct(TableResultMock result, ISqlDialect? dialect)
    {
        var estimatedCount = result.Count;
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outputRows = new List<Dictionary<int, object?>>(estimatedCount);

        foreach (var row in result)
        {
            if (seen.Add(BuildDistinctRowKey(row, result.Columns.Count, dialect)))
                outputRows.Add(row);
        }

        result.Clear();
        foreach (var row in outputRows)
            result.Add(row);

        return result;
    }

    private static object? ResolvePositionalParam(IDataParameterCollection parameters)
        => parameters.Count > 0 ? NormalizeParameterValue((IDataParameter)parameters[0]!) : null;

    private static object? ResolveNamedParam(IDataParameterCollection parameters, string name)
    {
        var normalizedName = name.TrimStart('@', ':');
        foreach (IDataParameter parameter in parameters)
        {
            var parameterName = parameter.ParameterName?.TrimStart('@', ':');
            if (string.Equals(parameterName, normalizedName, StringComparison.OrdinalIgnoreCase))
                return NormalizeParameterValue(parameter);
        }

        return null;
    }

    private static object? NormalizeParameterValue(IDataParameter parameter)
        => parameter.Value is DBNull ? null : parameter.Value;

    private static bool TrySplitQualifiedIdentifier(string name, out string qualifier, out string columnName)
    {
        qualifier = string.Empty;
        columnName = string.Empty;

        var dot = name.IndexOf('.');
        if (dot < 0)
            return false;

        qualifier = name[..dot];
        columnName = name[(dot + 1)..];
        return true;
    }

    private static bool TryResolveIdentifierFromSources(
        string name,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (row.TryGetSingleSource(out var singleSource))
            return TryResolveQualifiedNameValue(singleSource!, name, row, out value);

        foreach (var source in row.Sources.Values)
        {
            if (TryResolveQualifiedNameValue(source, name, row, out value))
                return true;
        }

        value = null;
        return false;
    }

    private static bool TryResolveProjectedIdentifier(
        string name,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
        => row.TryGetValue(name, out value);

    private static bool TryResolveUnqualifiedColumn(
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
        => row.TryGetValue(columnName, out value);

    private static bool TryResolveColumnFromSources(
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (row.TryGetSingleSource(out var singleSource))
            return TryResolveColumnFromSource(singleSource!, columnName, row, out value);

        foreach (var source in row.Sources.Values)
        {
            if (TryResolveColumnFromSource(source, columnName, row, out value))
                return true;
        }

        value = null;
        return false;
    }

    private static object? ResolveQualifiedColumn(
        string qualifier,
        string col,
        AstQueryExecutorBase.EvalRow row)
    {
        qualifier = qualifier.NormalizeName();

        var lastQualifier = GetLastQualifierSegment(qualifier);
        var source = FindQualifiedSource(qualifier, lastQualifier, row);
        if (source is null)
        {
            if (TryResolveDirectQualifiedField(qualifier, lastQualifier, col, row, out var directValue))
                return directValue;

            return null;
        }

        if (col == "*")
            return null;

        return TryResolveQualifiedNameValue(source, col, row, out var value)
            ? value
            : null;
    }

    private static string GetLastQualifierSegment(string qualifier)
    {
        var dot = qualifier.LastIndexOf('.');
        return dot >= 0 ? qualifier[(dot + 1)..] : qualifier;
    }

    private static AstQueryExecutorBase.Source? FindQualifiedSource(
        string qualifier,
        string lastQualifier,
        AstQueryExecutorBase.EvalRow row)
    {
        if (row.Sources.TryGetValue(qualifier, out var source)
            || row.Sources.TryGetValue(lastQualifier, out source))
        {
            return source;
        }

        if (row.TryGetSingleSource(out var singleSource)
            && (singleSource!.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
                || singleSource.Name.Equals(lastQualifier, StringComparison.OrdinalIgnoreCase)))
        {
            return singleSource;
        }

        foreach (var candidate in row.Sources.Values)
        {
            if (candidate.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
                || candidate.Name.Equals(lastQualifier, StringComparison.OrdinalIgnoreCase))
            {
                return candidate;
            }
        }

        return null;
    }

    private static bool TryResolveDirectQualifiedField(
        string qualifier,
        string lastQualifier,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (TryResolveQualifiedFieldFromSources(row, columnName, out value))
            return true;

        var directQualifiedName = $"{qualifier}.{columnName}";
        if (row.Fields.TryGetValue(directQualifiedName, out value))
            return true;

        if (!string.Equals(lastQualifier, qualifier, StringComparison.OrdinalIgnoreCase))
        {
            var lastQualifiedName = $"{lastQualifier}.{columnName}";
            if (row.Fields.TryGetValue(lastQualifiedName, out value))
                return true;
        }

        if (row.TryGetSingleSource(out var singleSource)
            && TryResolveQualifiedFieldFromSource(singleSource!, columnName, row, out value))
            return true;

        value = null;
        return false;
    }

    private static bool TryResolveColumnFromSource(
        AstQueryExecutorBase.Source source,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (TryResolveQualifiedNameValue(source, columnName, row, out value))
            return true;

        value = null;
        return false;
    }

    private static bool TryResolveRowIdFromSources(
        AstQueryExecutorBase.EvalRow row,
        out object? rowId)
    {
        if (row.TryGetSingleSource(out var singleSource))
            return TryResolveRowIdFromSource(singleSource!, row, out rowId);

        foreach (var source in row.Sources.Values)
        {
            if (TryResolveRowIdFromSource(source, row, out rowId))
                return true;
        }

        rowId = null;
        return false;
    }

    private static bool TryResolveRowIdFromSource(
        AstQueryExecutorBase.Source source,
        AstQueryExecutorBase.EvalRow row,
        out object? rowId)
    {
        rowId = null;
        string pkColumn = string.Empty;
        if (source.Physical is null || !source.TryGetSinglePrimaryKeyColumnName(out pkColumn) || pkColumn.Length == 0)
            return false;

        if (!TryResolveQualifiedNameValue(source, pkColumn, row, out var resolvedRowId)
            && !row.TryGetValue(pkColumn, out resolvedRowId))
        {
            return false;
        }

        if (resolvedRowId is DBNull)
            return false;

        rowId = resolvedRowId;
        return true;
    }

    private static bool TryResolveQualifiedNameValue(
        AstQueryExecutorBase.Source source,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        value = null;

        string? qualifiedName = null;
        if (!source.TryGetQualifiedColumnName(columnName, out qualifiedName)
            || string.IsNullOrEmpty(qualifiedName))
            return false;

        return row.TryGetValue(qualifiedName!, out value);
    }

    private static bool TryResolveQualifiedFieldFromSource(
        AstQueryExecutorBase.Source source,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        value = null;

        string? qualifiedName = null;
        if (!source.TryGetQualifiedColumnName(columnName, out qualifiedName))
            return false;

        if (string.IsNullOrWhiteSpace(qualifiedName))
            return false;

        return row.Fields.TryGetValue(qualifiedName!, out value);
    }

    private static bool TryResolveQualifiedFieldFromSources(
        AstQueryExecutorBase.EvalRow row,
        string columnName,
        out object? value)
    {
        if (row.TryGetSingleSource(out var singleSource)
            && TryResolveQualifiedFieldFromSource(singleSource!, columnName, row, out value))
        {
            return true;
        }

        foreach (var source in row.Sources.Values)
        {
            if (TryResolveQualifiedFieldFromSource(source, columnName, row, out value))
                return true;
        }

        value = null;
        return false;
    }

    private static string BuildDistinctRowKey(
        Dictionary<int, object?> row,
        int columnCount,
        ISqlDialect? dialect)
    {
        if (columnCount <= 0)
            return string.Empty;

        if (columnCount == 1)
            return NormalizeDistinctKey(row.TryGetValue(0, out var singleValue) ? singleValue : null, dialect);

        var builder = new StringBuilder(Math.Max(16, columnCount * 12));
        for (var i = 0; i < columnCount; i++)
        {
            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(NormalizeDistinctKey(row.TryGetValue(i, out var value) ? value : null, dialect));
        }

        return builder.ToString();
    }
}
