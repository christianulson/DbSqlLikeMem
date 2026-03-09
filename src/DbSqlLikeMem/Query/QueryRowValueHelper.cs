using System.Data;
using System.Globalization;
using System.Text;

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
            return "NULL";

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
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var outputRows = new List<Dictionary<int, object?>>();

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
        foreach (var source in row.Sources.Values)
        {
            if (row.Fields.TryGetValue($"{source.Alias}.{name}", out value))
                return true;
        }

        value = null;
        return false;
    }

    private static bool TryResolveProjectedIdentifier(
        string name,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
        => row.Fields.TryGetValue(name, out value);

    private static bool TryResolveUnqualifiedColumn(
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
        => row.Fields.TryGetValue(columnName, out value);

    private static bool TryResolveColumnFromSources(
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
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

        return TryResolveColumnFromSource(source, col, row, out var value)
            ? value
            : null;
    }

    private static string GetLastQualifierSegment(string qualifier)
        => qualifier.Contains('.')
            ? qualifier.Split('.').Last()
            : qualifier;

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

        return row.Sources.Values.FirstOrDefault(candidate =>
            candidate.Name.Equals(qualifier, StringComparison.OrdinalIgnoreCase)
            || candidate.Name.Equals(lastQualifier, StringComparison.OrdinalIgnoreCase));
    }

    private static bool TryResolveDirectQualifiedField(
        string qualifier,
        string lastQualifier,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (row.Fields.TryGetValue($"{lastQualifier}.{columnName}", out value))
            return true;

        return row.Fields.TryGetValue($"{qualifier}.{columnName}", out value);
    }

    private static bool TryResolveColumnFromSource(
        AstQueryExecutorBase.Source source,
        string columnName,
        AstQueryExecutorBase.EvalRow row,
        out object? value)
    {
        if (row.Fields.TryGetValue($"{source.Alias}.{columnName}", out value))
            return true;

        var matchedColumn = source.ColumnNames.FirstOrDefault(name => name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
        if (matchedColumn is not null)
            return row.Fields.TryGetValue($"{source.Alias}.{matchedColumn}", out value);

        value = null;
        return false;
    }

    private static string BuildDistinctRowKey(
        Dictionary<int, object?> row,
        int columnCount,
        ISqlDialect? dialect)
    {
        var builder = new StringBuilder();
        for (var i = 0; i < columnCount; i++)
        {
            if (builder.Length > 0)
                builder.Append('\u001F');

            builder.Append(NormalizeDistinctKey(row.TryGetValue(i, out var value) ? value : null, dialect));
        }

        return builder.ToString();
    }
}
