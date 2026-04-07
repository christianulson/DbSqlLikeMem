using System.Text.Json;

namespace DbSqlLikeMem.Firebird;

internal static class FirebirdValueHelper
{
    private static readonly Regex _list = new(@"^[\(](?<inner>.+)[\)]$", RegexOptions.Singleline);
    private static readonly AsyncLocal<string?> _currentColumn = new();

    /// <summary>
    /// EN: Gets or sets the column currently being resolved by Firebird value helpers.
    /// PT: Obtém ou define a coluna atualmente resolvida pelos auxiliares de valor do Firebird.
    /// </summary>
    internal static string? CurrentColumn
    {
        get => _currentColumn.Value;
        set => _currentColumn.Value = value;
    }

    /// <summary>
    /// EN: Resolves a token to a Firebird-compatible value.
    /// PT: Resolve um token para um valor compatível com Firebird.
    /// </summary>
    public static object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        if (token.StartsWith("@", StringComparison.Ordinal)
            || token.StartsWith(":", StringComparison.Ordinal))
        {
            var name = token[1..]
                .Replace("\r\n", string.Empty)
                .Replace(";", string.Empty);
            if (pars is null || !pars.Contains(name))
                throw new FirebirdMockException(SqlExceptionMessages.ParameterNotFound(name), 0);

            if (pars[name] is not IDataParameter parameter)
                throw new FirebirdMockException(SqlExceptionMessages.ParameterNotFound(name), 0);

            return parameter.Value;
        }

        if (token.Equals("null", StringComparison.OrdinalIgnoreCase))
            return isNullable
                ? null
                : throw (FirebirdExceptionFactory.ColumnCannotBeNull(CurrentColumn ?? string.Empty));

        var m = _list.Match(token);
        if (m.Success)
        {
            var inner = m.Groups["inner"].Value;
            return inner.Split(',')
                        .Select(s => Resolve(s.Trim(), dbType, isNullable, pars, colDict))
                        .ToList();
        }

        token = token.Trim('"', '\'');
        if (TryParseEnumOrSet(token, colDict, out var value))
            return ValidateColumnValue(value, colDict);

        if (dbType == DbType.Object && (token.StartsWith("{", StringComparison.Ordinal) || token.StartsWith("[", StringComparison.Ordinal)))
            return ParseJson(token);

        return ValidateColumnValue(dbType.Parse(token), colDict);
    }

    private static bool TryParseEnumOrSet(
        string token,
        IReadOnlyDictionary<string, ColumnDef>? colDict,
        out object? value)
    {
        value = null;
        if (colDict is null || string.IsNullOrWhiteSpace(CurrentColumn))
            return false;

        if (!colDict.TryGetValue(CurrentColumn!, out var cdef))
            return false;

        if (cdef.EnumValues is null || cdef.EnumValues.Count == 0)
            return false;

        var raw = token.Trim();
        if (raw.Contains(','))
        {
            var parts = raw.Split(',').Select(_ => _.Trim()).Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
            var hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var p in parts)
            {
                var match = cdef.EnumValues.FirstOrDefault(v => string.Equals(v, p, StringComparison.OrdinalIgnoreCase));
                if (match is null)
                    throw new FirebirdMockException($"Invalid set value '{p}' for column '{CurrentColumn}'", 0);
                hs.Add(match);
            }

            value = hs;
            return true;
        }

        {
            var match = cdef.EnumValues.FirstOrDefault(v => string.Equals(v, raw, StringComparison.OrdinalIgnoreCase));
            if (match is null)
                throw new FirebirdMockException($"Invalid enum value '{raw}' for column '{CurrentColumn}'", 0);
            value = match;
            return true;
        }
    }

    private static object? ValidateColumnValue(
        object? value,
        IReadOnlyDictionary<string, ColumnDef>? colDict)
    {
        if (value is null || value is DBNull)
            return value;

        if (colDict is null || string.IsNullOrWhiteSpace(CurrentColumn))
            return value;

        if (!colDict.TryGetValue(CurrentColumn!, out var cdef))
            return value;

        if (cdef.Size is int size && value is string s && s.Length > size)
            throw new FirebirdMockException(SqlExceptionMessages.DataTooLongForColumn(CurrentColumn!), 0);

        if (cdef.DecimalPlaces is int scale && value is decimal d && GetDecimalScale(d) > scale)
            throw new FirebirdMockException(SqlExceptionMessages.DataTruncatedForColumn(CurrentColumn!), 0);

        return value;
    }

    private static int GetDecimalScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }

    private static object ParseJson(string txt)
    {
#pragma warning disable CA1031
        try { return JsonDocument.Parse(txt); }
        catch { return txt; }
#pragma warning restore CA1031
    }
}
