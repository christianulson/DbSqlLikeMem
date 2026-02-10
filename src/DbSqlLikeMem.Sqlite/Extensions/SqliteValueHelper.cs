using Microsoft.Data.Sqlite;
using System.Text.Json;

namespace DbSqlLikeMem.Sqlite;

internal static class SqliteValueHelper
{
    private static readonly Regex _list = new(@"^[\(](?<inner>.+)[\)]$", RegexOptions.Singleline);

    // xUnit roda testes em paralelo por padrão. Se isso for global, um teste pisa no outro.
    // AsyncLocal resolve pra cenários async/await e também isola por fluxo de execução.
    private static readonly AsyncLocal<string?> _currentColumn = new();

    /// <summary>
    /// Nome da coluna que está sendo processada (setado pelo caller antes de chamar <c>Resolve</c>).
    /// Mantido em <see cref="AsyncLocal{T}"/> para evitar interferência entre testes.
    /// </summary>
    internal static string? CurrentColumn
    {
        get => _currentColumn.Value;
        set => _currentColumn.Value = value;
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null)
    {
        // ---------- parâmetro Dapper @p -------------------------------
        if (token.StartsWith('@'))
        {
            var name = token[1..]
                .Replace("\r\n", string.Empty, StringComparison.Ordinal)
                .Replace(";", string.Empty, StringComparison.Ordinal);
            if (pars == null || !pars.Contains(name))
                throw new SqliteMockException($"Parâmetro {name} não encontrado.");
            return ((SqliteParameter)pars[name]).Value;
        }

        // ---------- literal NULL --------------------------------------
        if (token.Equals("null", StringComparison.OrdinalIgnoreCase))
            return isNullable 
                ? null
                : throw new SqliteMockException("Coluna não aceita NULL");

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

        // ---------- JSON ----------------------------------------------
        if (dbType == DbType.Object && (token.StartsWith('{') || token.StartsWith('[')))
            return ParseJson(token);

        // ---------- tipos padrões -------------------------------------
        return ValidateColumnValue(dbType.Parse(token), colDict);
    }

    private static bool TryParseEnumOrSet(
        string token,
        IColumnDictionary? colDict,
        out object? value)
    {
        value = null;

        if (colDict is null || string.IsNullOrWhiteSpace(CurrentColumn))
            return false;

        if (!colDict.TryGetValue(CurrentColumn, out var cdef))
            return false;

        if (cdef.EnumValues is null || cdef.EnumValues.Count == 0)
            return false;

        var raw = token.Trim();

        // SET
        if (raw.Contains(',', StringComparison.Ordinal))
        {
            var parts = raw.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
            var hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var p in parts)
            {
                var match = cdef.EnumValues.FirstOrDefault(v =>
                    string.Equals(v, p, StringComparison.OrdinalIgnoreCase));

                if (match is null)
                    throw new SqliteMockException(
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
                throw new SqliteMockException(
                    $"Invalid enum value '{raw}' for column '{CurrentColumn}'",
                    1265);

            value = match;
            return true;
        }
    }

    private static object? ValidateColumnValue(object? value, IColumnDictionary? colDict)
    {
        if (value is null || value is DBNull)
            return value;

        if (colDict is null || string.IsNullOrWhiteSpace(CurrentColumn))
            return value;

        if (!colDict.TryGetValue(CurrentColumn, out var cdef))
            return value;

        if (cdef.Size is int size && value is string s && s.Length > size)
            throw new SqliteMockException($"Data too long for column '{CurrentColumn}'", 1406);

        if (cdef.DecimalPlaces is int scale && value is decimal d && GetDecimalScale(d) > scale)
            throw new SqliteMockException($"Data truncated for column '{CurrentColumn}'", 1265);

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

    // LIKE simples %xxx% → usa Contains
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static bool Like(string value, string pattern)
    {
        pattern = Regex.Escape(pattern)
            .Replace("%", ".*", StringComparison.Ordinal)
            .Replace("_", ".", StringComparison.Ordinal);
        return Regex.IsMatch(value, "^" + pattern + "$", RegexOptions.Singleline | RegexOptions.IgnoreCase);
    }
}
