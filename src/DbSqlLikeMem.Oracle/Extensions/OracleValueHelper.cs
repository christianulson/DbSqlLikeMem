using Oracle.ManagedDataAccess.Client;
using System.Collections.Immutable;
using System.Globalization;
using System.Text.Json;

namespace DbSqlLikeMem.Oracle;

internal static class OracleValueHelper
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

        // ---------- parâmetro Dapper (@p / :p) ------------------------
        if (token.StartsWith("@") || token.StartsWith(":"))
        {
            var name = token[1..]
                .Replace("\r\n", string.Empty)
                .Replace(";", string.Empty);

            if (!TryGetParameterValue(pars, name, out var parameterValue))
                throw new OracleMockException(SqlExceptionMessages.ParameterNotFound(name));

            return CoerceParameterValue(parameterValue, dbType);
        }

        // ---------- literal NULL --------------------------------------
        if (token.Equals("null", StringComparison.OrdinalIgnoreCase))
            return isNullable
                ? null
                : throw new OracleMockException(SqlExceptionMessages.ColumnDoesNotAcceptNull());

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

    private static bool TryGetParameterValue(
        IDataParameterCollection? pars,
        string name,
        out object? value)
    {
        value = null;

        if (pars is null)
            return false;

        if (pars.Contains(name))
        {
            value = ((OracleParameter)pars[name]).Value;
            return true;
        }

        var colonName = $":{name}";
        if (pars.Contains(colonName))
        {
            value = ((OracleParameter)pars[colonName]).Value;
            return true;
        }

        var atName = $"@{name}";
        if (pars.Contains(atName))
        {
            value = ((OracleParameter)pars[atName]).Value;
            return true;
        }

        return false;
    }

    private static object? CoerceParameterValue(object? parameterValue, DbType dbType)
    {
        if (parameterValue is null || parameterValue is DBNull)
            return parameterValue;

        if (parameterValue is string textValue)
        {
            try
            {
                return dbType.Parse(textValue);
            }
            catch
            {
                return parameterValue;
            }
        }

        try
        {
            return dbType switch
            {
                DbType.Byte => Convert.ToByte(parameterValue, CultureInfo.InvariantCulture),
                DbType.SByte => Convert.ToSByte(parameterValue, CultureInfo.InvariantCulture),
                DbType.Int16 => Convert.ToInt16(parameterValue, CultureInfo.InvariantCulture),
                DbType.Int32 => Convert.ToInt32(parameterValue, CultureInfo.InvariantCulture),
                DbType.Int64 => Convert.ToInt64(parameterValue, CultureInfo.InvariantCulture),
                DbType.UInt16 => Convert.ToUInt16(parameterValue, CultureInfo.InvariantCulture),
                DbType.UInt32 => Convert.ToUInt32(parameterValue, CultureInfo.InvariantCulture),
                DbType.UInt64 => Convert.ToUInt64(parameterValue, CultureInfo.InvariantCulture),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric => Convert.ToDecimal(parameterValue, CultureInfo.InvariantCulture),
                DbType.Double => Convert.ToDouble(parameterValue, CultureInfo.InvariantCulture),
                DbType.Single => Convert.ToSingle(parameterValue, CultureInfo.InvariantCulture),
                DbType.Boolean => Convert.ToBoolean(parameterValue, CultureInfo.InvariantCulture),
                DbType.Date => parameterValue is DateTime dateTimeValue
                    ? dateTimeValue.Date
                    : Convert.ToDateTime(parameterValue, CultureInfo.InvariantCulture).Date,
                DbType.DateTime => parameterValue is DateTime dateTime
                    ? dateTime
                    : Convert.ToDateTime(parameterValue, CultureInfo.InvariantCulture),
                DbType.DateTime2 => parameterValue is DateTime dateTime2
                    ? dateTime2
                    : Convert.ToDateTime(parameterValue, CultureInfo.InvariantCulture),
                DbType.DateTimeOffset => parameterValue is DateTimeOffset dateTimeOffset
                    ? dateTimeOffset
                    : DateTimeOffset.Parse(Convert.ToString(parameterValue, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture),
                DbType.Time => parameterValue is TimeSpan timeSpan
                    ? timeSpan
                    : TimeSpan.Parse(Convert.ToString(parameterValue, CultureInfo.InvariantCulture)!, CultureInfo.InvariantCulture),
                DbType.Guid => parameterValue is Guid guid
                    ? guid
                    : Guid.Parse(Convert.ToString(parameterValue, CultureInfo.InvariantCulture)!),
                DbType.Binary => parameterValue is byte[] bytes
                    ? bytes
                    : Convert.FromBase64String(Convert.ToString(parameterValue, CultureInfo.InvariantCulture)!),
                DbType.String
                or DbType.AnsiString
                or DbType.StringFixedLength
                or DbType.AnsiStringFixedLength => Convert.ToString(parameterValue, CultureInfo.InvariantCulture),
                DbType.Object => parameterValue,
                _ => parameterValue
            };
        }
        catch
        {
            var textValue1 = Convert.ToString(parameterValue, CultureInfo.InvariantCulture);
            if (string.IsNullOrWhiteSpace(textValue1))
                return parameterValue;

            try
            {
                return dbType.Parse(textValue1);
            }
            catch
            {
                return parameterValue;
            }
        }
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
                    throw new OracleMockException(
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
                throw new OracleMockException(
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
            throw new OracleMockException(
                $"Value too large for column '{CurrentColumn}'",
                12899);

        if (cdef.DecimalPlaces is int scale && value is decimal d && GetDecimalScale(d) > scale)
            throw new OracleMockException(
                $"Value larger than specified precision for column '{CurrentColumn}'",
                1438);

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
