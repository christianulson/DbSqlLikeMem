using System.Collections.Immutable;

namespace DbSqlLikeMem.MySql;

internal static class MySqlValueHelper
{
    private static readonly Regex _list = new(@"^[\(](?<inner>.+)[\)]$", RegexOptions.Singleline);

    // xUnit roda testes em paralelo por padrão. Se isso for global, um teste pisa no outro.
    // AsyncLocal resolve pra cenários async/await e também isola por fluxo de execução.
    private static readonly AsyncLocal<string?> _currentColumn = new();

    /// <summary>
    /// Nome da coluna que está sendo processada (setado pelo caller antes de chamar <c>ResolveRowsFrameRange</c>).
    /// Mantido em <see cref="AsyncLocal{T}"/> para evitar interferência entre testes.
    /// </summary>
    internal static string? CurrentColumn
    {
        get => _currentColumn.Value;
        set => _currentColumn.Value = value;
    }

    /// <summary>
    /// EN: Implements ResolveRowsFrameRange.
    /// PT-br: Implementa ResolveRowsFrameRange.
    /// </summary>
    public static object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        // ---------- parâmetro Dapper @p -------------------------------
        if (token.StartsWith("@"))
        {
            var name = token[1..]
                .Replace("\r\n", string.Empty)
                .Replace(";", string.Empty);
            if (pars == null || !pars.Contains(name))
                throw new MySqlMockException(SqlExceptionMessages.ParameterNotFound(name));
            return ((MySqlParameter)pars[name]).Value;
        }

        // ---------- literal NULL --------------------------------------
        if (token.Equals("null", StringComparison.OrdinalIgnoreCase))
            return isNullable
                ? null
                : throw new MySqlMockException(SqlExceptionMessages.ColumnDoesNotAcceptNull());

        // ---------- lista ( ..., ... )  para IN ------------------------
        var m = _list.Match(token);
        if (m.Success)
        {
            var inner = m.Groups["inner"].Value;
            return inner.Split(',')
                        .Select(s => Resolve(s.Trim(), dbType, isNullable, pars, colDict))
                        .ToList();
        }

        // ---------- remove aspas externas -----------------------------
        token = token.Trim('"', '\'');
        if (TryParseEnumOrSet(token, colDict, out var value))
            return ValidateColumnValue(value, colDict);

        // ---------- JSON (DbType.Object) --------------------------------
        if (dbType == DbType.Object && (token.StartsWith("{") || token.StartsWith("[")))
            return ParseJson(token);

        // ---------- MySQL JSON string normalization ---------------------
        // MySQL normalizes JSON on storage with spaces after colons/commas.
        // JSON columns map to DbType.String, so detect JSON by content.
        if (dbType == DbType.String && (token.StartsWith("{") || token.StartsWith("[")))
            return NormalizeJsonToMySqlFormat(token);

        // ---------- tipos padrões -------------------------------------
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

        // SET
        if (raw.Contains(','))
        {
            var parts = raw.Split(',').Select(_ => _.Trim()).ToArray();
            var hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var p in parts)
            {
                var match = cdef.EnumValues.FirstOrDefault(v =>
                    string.Equals(v, p, StringComparison.OrdinalIgnoreCase));

                if (match is null)
                    throw new MySqlMockException(
                        $"Invalid set value '{p}' for column '{CurrentColumn}'",
                        1265);

                hs.Add(match);
            }

            value = hs;
            return true;
        }

        // ENUM
        {
            var match = cdef.EnumValues.FirstOrDefault(v =>
                string.Equals(v, raw, StringComparison.OrdinalIgnoreCase));

            if (match is null)
                throw new MySqlMockException(
                    $"Invalid enum value '{raw}' for column '{CurrentColumn}'",
                    1265);

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
            throw new MySqlMockException(SqlExceptionMessages.DataTooLongForColumn(CurrentColumn!), 1406);

        if (cdef.DecimalPlaces is int scale && value is decimal d && GetDecimalScale(d) > scale)
            throw new MySqlMockException(SqlExceptionMessages.DataTruncatedForColumn(CurrentColumn!), 1265);

        return value;
    }

    private static int GetDecimalScale(decimal value)
    {
        var bits = decimal.GetBits(value);
        return (bits[3] >> 16) & 0x7F;
    }


    private static object ParseJson(string txt)
    {
#pragma warning disable CA1031 // Do not catch general exception types
        try { return JsonDocument.Parse(txt); }
        catch { return txt; }                 // se der erro, fica string crua
#pragma warning restore CA1031 // Do not catch general exception types
    }

    internal static string NormalizeJsonToMySqlFormat(string json)
    {
#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            using var doc = JsonDocument.Parse(json);
            return SerializeMySqlJson(doc.RootElement);
        }
        catch { return json; }
#pragma warning restore CA1031 // Do not catch general exception types
    }

    private static string SerializeMySqlJson(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var sb = new StringBuilder("{");
                var first = true;
                foreach (var prop in element.EnumerateObject())
                {
                    if (!first) sb.Append(", ");
                    first = false;
                    sb.Append('"');
                    sb.Append(prop.Name);
                    sb.Append("\": ");
                    sb.Append(SerializeMySqlJson(prop.Value));
                }
                sb.Append('}');
                return sb.ToString();
            }
            case JsonValueKind.Array:
            {
                var sb = new StringBuilder("[");
                var first = true;
                foreach (var item in element.EnumerateArray())
                {
                    if (!first) sb.Append(", ");
                    first = false;
                    sb.Append(SerializeMySqlJson(item));
                }
                sb.Append(']');
                return sb.ToString();
            }
            case JsonValueKind.String:
                return JsonSerializer.Serialize(element.GetString());

            case JsonValueKind.Number:
                return element.GetRawText();

            case JsonValueKind.True:
                return "true";

            case JsonValueKind.False:
                return "false";

            case JsonValueKind.Null:
                return "null";

            default:
                return element.GetRawText();
        }
    }

    // LIKE simples %xxx% → usa Contains
    /// <summary>
    /// EN: Implements Like.
    /// PT-br: Implementa Like.
    /// </summary>
    public static bool Like(string value, string pattern)
    {
        pattern = Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".");
        return Regex.IsMatch(value, "^" + pattern + "$", RegexOptions.Singleline | RegexOptions.IgnoreCase);
    }
}
