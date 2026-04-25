using System.Text.Json;
#if NET462
using DB2Parameter = IBM.Data.DB2.iSeries.iDB2Parameter;
#endif
namespace DbSqlLikeMem.Db2;

internal static class Db2ValueHelper
{
    private static readonly Regex _list = new(@"^[\(](?<inner>.+)[\)]$", RegexOptions.Singleline);

    // xUnit roda testes em paralelo por padrão. Se isso for global, um teste pisa no outro.
    // AsyncLocal resolve pra cenários async/await e também isola por fluxo de execução.
    private static readonly AsyncLocal<string?> _currentColumn = new();
    private static readonly AsyncLocal<int> _positionalParameterCursor = new();

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
    /// EN: Resets the positional parameter cursor used to resolve DB2 `?` placeholders in order.
    /// PT: Reinicia o cursor de parametros posicionais usado para resolver placeholders `?` do DB2 em ordem.
    /// </summary>
    internal static void ResetPositionalParameterCursor()
        => _positionalParameterCursor.Value = 0;

    /// <summary>
    /// EN: Implements ResolveRowsFrameRange.
    /// PT: Implementa ResolveRowsFrameRange.
    /// </summary>
    public static object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null)
    {
        token = token.Trim();

        // ---------- parâmetro posicional ? ---------------------------
        if (token == "?")
        {
            if (TryResolvePositionalParameter(pars, out var positionalValue))
                return positionalValue;

            throw new Db2MockException(SqlExceptionMessages.ParameterNotFound("?"));
        }

        // ---------- parâmetro Dapper @p -------------------------------
        if (token.StartsWith("@"))
        {
            var name = token[1..]
                .Replace("\r\n", string.Empty)
                .Replace(";", string.Empty);
            if (!TryResolveNamedParameter(pars, name, out var namedValue))
                throw new Db2MockException(SqlExceptionMessages.ParameterNotFound(name));
            return namedValue;
        }

        // ---------- literal NULL --------------------------------------
        if (token.Equals("null", StringComparison.OrdinalIgnoreCase))
            return isNullable
                ? null
                : throw new Db2MockException(SqlExceptionMessages.ColumnDoesNotAcceptNull());

        // ---------- lista ( ..., ... )  para IN ------------------------
        var m = _list.Match(token);
        if (m.Success)
        {
            var inner = m.Groups["inner"].Value;
            return inner.Split(',')
                        .Select(s => Resolve(s.Trim(), dbType, isNullable, pars, colDict))
                        .ToList();
        }

        // ---------- conversão textual Oracle/Db2 ----------------------
        if (TryUnwrapTextConversionFunction(token, out var textArgument))
            return Resolve(textArgument, dbType, isNullable, pars, colDict);

        // ---------- remove aspas externas -----------------------------
        token = token.Trim('"', '\'');
        if (TryParseEnumOrSet(token, colDict, out var value))
            return ValidateColumnValue(value, colDict);

        // ---------- JSON ----------------------------------------------
        if (dbType == DbType.Object && (token.StartsWith("{") || token.StartsWith("[")))
            return ParseJson(token);

        // ---------- tipos padrões -------------------------------------
        return ValidateColumnValue(dbType.Parse(token), colDict);
    }

    private static bool TryResolvePositionalParameter(
        IDataParameterCollection? pars,
        out object? value)
    {
        value = null;
        if (pars is null || pars.Count == 0)
            return false;

        var cursor = _positionalParameterCursor.Value;
        if ((uint)cursor < (uint)pars.Count && pars[cursor] is IDataParameter parameter)
        {
            _positionalParameterCursor.Value = cursor + 1;
            value = parameter.Value is DBNull ? null : parameter.Value;
            return true;
        }

        return false;
    }

    private static bool TryResolveNamedParameter(
        IDataParameterCollection? pars,
        string name,
        out object? value)
    {
        value = null;
        if (pars is null || string.IsNullOrWhiteSpace(name))
            return false;

        var normalized = name.Trim();
        var candidates = new[]
        {
            normalized,
            "@" + normalized,
            ":" + normalized,
            "?" + normalized
        };

        foreach (var candidate in candidates)
        {
            if (!pars.Contains(candidate))
                continue;

            if (pars[candidate] is not IDataParameter parameter)
                continue;

            value = parameter.Value is DBNull ? null : parameter.Value;
            return true;
        }

        return false;
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
            var parts = raw.Split(',').Select(_ => _.Trim()).Where(_ => !string.IsNullOrWhiteSpace(_)).ToArray();
            var hs = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var p in parts)
            {
                var match = cdef.EnumValues.FirstOrDefault(v =>
                    string.Equals(v, p, StringComparison.OrdinalIgnoreCase));

                if (match is null)
                    throw new Db2MockException(
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
                throw new Db2MockException(
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
            throw new Db2MockException(SqlExceptionMessages.DataTooLongForColumn(CurrentColumn!), 1406);

        if (cdef.DecimalPlaces is int scale && value is decimal d && GetDecimalScale(d) > scale)
            throw new Db2MockException(SqlExceptionMessages.DataTruncatedForColumn(CurrentColumn!), 1265);

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
    /// EN: Implements Like.
    /// PT: Implementa Like.
    /// </summary>
    public static bool Like(string value, string pattern)
    {
        pattern = Regex.Escape(pattern)
            .Replace("%", ".*")
            .Replace("_", ".");
        return Regex.IsMatch(value, "^" + pattern + "$", RegexOptions.Singleline | RegexOptions.IgnoreCase);
    }

    private static bool TryUnwrapTextConversionFunction(string token, out string innerToken)
    {
        foreach (var functionName in new[]
        {
            "TO_CLOB",
            "TO_NCLOB",
            "TO_LOB",
            "TO_MULTI_BYTE",
            "TO_NCHAR",
            "TO_SINGLE_BYTE"
        })
        {
            if (!token.StartsWith(functionName, StringComparison.OrdinalIgnoreCase))
                continue;

            var openParenIndex = functionName.Length;
            if (token.Length <= openParenIndex + 1
                || token[openParenIndex] != '('
                || token[^1] != ')')
            {
                continue;
            }

            innerToken = token[(openParenIndex + 1)..^1].Trim();
            if (innerToken.Length > 0)
                return true;
        }

        innerToken = string.Empty;
        return false;
    }
}
