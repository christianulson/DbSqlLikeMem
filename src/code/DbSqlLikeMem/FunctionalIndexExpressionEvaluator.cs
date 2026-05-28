using System.Globalization;
using System.Text.Json;

namespace DbSqlLikeMem;

internal static class FunctionalIndexExpressionEvaluator
{
    internal static Func<IReadOnlyDictionary<int, object?>, ITableMock, object?> CreateGetGenValue(
        string expressionText,
        DbMock db,
        ISqlDialect dialect,
        ITableMock table)
    {
        var expr = SqlExpressionParser.ParseScalar(expressionText, db, dialect);

        // Build a column-name-to-index map once for fast resolution
        var colNameToIndex = table.Columns.ToDictionary(
            static kv => kv.Key,
            static kv => kv.Value.Index,
            StringComparer.OrdinalIgnoreCase);

        return (row, tbl) => Eval(expr, row, tbl, colNameToIndex);
    }

    private static object? Eval(
        SqlExpr expr,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        switch (expr)
        {
            case LiteralExpr l:
                return l.Value;

            case IdentifierExpr id:
            {
                if (colNameToIndex.TryGetValue(id.Name, out var idx))
                    return row.TryGetValue(idx, out var v) ? v : null;
                return null;
            }

            case UnaryExpr u when u.Op == SqlUnaryOp.Not:
            {
                var v = Eval(u.Expr, row, table, colNameToIndex);
                return v is bool b ? !b : null;
            }

            case BinaryExpr b:
                return EvalBinary(b, row, table, colNameToIndex);

            case FunctionCallExpr f:
                return EvalFunctionCall(f, row, table, colNameToIndex);

            case CallExpr c:
                return EvalCall(c, row, table, colNameToIndex);

            case JsonAccessExpr j:
                return EvalJsonAccess(j, row, table, colNameToIndex);

            case BetweenExpr be:
                return EvalBetween(be, row, table, colNameToIndex);

            case InExpr ie:
                return EvalIn(ie, row, table, colNameToIndex);

            case IsNullExpr isn:
            {
                var v = Eval(isn.Expr, row, table, colNameToIndex);
                return isn.Negated ? v != null : v is null || v is DBNull;
            }

            case RawSqlExpr:
                return null;

            case RowExpr re:
                return re.Items.Select(i => Eval(i, row, table, colNameToIndex)).ToArray();

            case StarExpr:
                return "*";

            default:
                return null;
        }
    }

    private static object? EvalBinary(
        BinaryExpr b,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var left = Eval(b.Left, row, table, colNameToIndex);
        var right = Eval(b.Right, row, table, colNameToIndex);

        if (left is null || right is null)
            return b.Op switch
            {
                SqlBinaryOp.And => left is true ? right : left,
                SqlBinaryOp.Or => left is true ? left : (right is true ? right : left),
                _ => null
            };

        switch (b.Op)
        {
            case SqlBinaryOp.Add:
                return ToNumeric(left) + ToNumeric(right);
            case SqlBinaryOp.Subtract:
                return ToNumeric(left) - ToNumeric(right);
            case SqlBinaryOp.Multiply:
                return ToNumeric(left) * ToNumeric(right);
            case SqlBinaryOp.Divide:
                return ToNumeric(right) == 0 ? null : ToNumeric(left) / ToNumeric(right);
            case SqlBinaryOp.Modulo:
                return ToNumeric(right) == 0 ? null : ToNumeric(left) % ToNumeric(right);
            case SqlBinaryOp.Concat:
                return $"{left}{right}";
            case SqlBinaryOp.Eq:
                return Equals(left, right);
            case SqlBinaryOp.Neq:
                return !Equals(left, right);
            case SqlBinaryOp.Greater:
                return Compare(left, right) > 0;
            case SqlBinaryOp.GreaterOrEqual:
                return Compare(left, right) >= 0;
            case SqlBinaryOp.Less:
                return Compare(left, right) < 0;
            case SqlBinaryOp.LessOrEqual:
                return Compare(left, right) <= 0;
            case SqlBinaryOp.And:
                return left is true && right is true;
            case SqlBinaryOp.Or:
                return left is true || right is true;
            case SqlBinaryOp.Is:
                return ReferenceEquals(left, right) || Equals(left, right);
            case SqlBinaryOp.FullTextMatch:
                return AstQueryBinarySupportHelper.EvalMatchAgainst(left, right);
            default:
                return null;
        }
    }

    private static object? EvalFunctionCall(
        FunctionCallExpr f,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var args = f.Args.Select(a => Eval(a, row, table, colNameToIndex)).ToArray();
        return EvalNamedFunction(f.Name, args);
    }

    private static object? EvalCall(
        CallExpr c,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var args = c.Args.Select(a => Eval(a, row, table, colNameToIndex)).ToArray();
        var name = c.Name.ToUpperInvariant();
        if (name == "CAST") return args.Length > 0 ? args[0] : null;
        if (name == "CONVERT") return args.Length > 0 ? args[^1] : null;
        return EvalNamedFunction(c.Name, args);
    }

    private static object? EvalNamedFunction(string name, object?[] args)
    {
        return name.ToUpperInvariant() switch
        {
            "UPPER" => args.Length > 0 ? args[0]?.ToString()?.ToUpperInvariant() : null,
            "LOWER" => args.Length > 0 ? args[0]?.ToString()?.ToLowerInvariant() : null,
            "CONCAT" => string.Concat(args.Select(a => a?.ToString())),
            "LENGTH" => args.Length > 0 ? args[0]?.ToString()?.Length : null,
            "TRIM" => args.Length > 0 ? args[0]?.ToString()?.Trim() : null,
            "IFNULL" => args.Length >= 2 ? args[0] ?? args[1] : null,
            "COALESCE" => args.FirstOrDefault(a => a != null),
            "REPLACE" => args.Length >= 3 ? args[0]?.ToString()?.Replace(args[1]?.ToString() ?? "", args[2]?.ToString() ?? "") : null,
            "SUBSTRING" or "SUBSTR" => EvalSubstring(args),
            "ABS" => args.Length > 0 ? Math.Abs(Convert.ToDecimal(args[0], CultureInfo.InvariantCulture)) : null,
            "ROUND" => args.Length > 0 ? Math.Round(Convert.ToDecimal(args[0], CultureInfo.InvariantCulture), args.Length > 1 ? Convert.ToInt32(args[1], CultureInfo.InvariantCulture) : 0) : null,
            "CAST" => args.Length > 0 ? args[0] : null,
            "NULLIF" => args.Length >= 2 && Equals(args[0], args[1]) ? null : args[0],
            "JSON_EXTRACT" => args.Length >= 2 ? EvalJsonExtract(args[0], args[1]) : null,
            _ => null
        };
    }

    private static object? EvalJsonAccess(
        JsonAccessExpr j,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var target = Eval(j.Target, row, table, colNameToIndex);
        if (target is null)
            return null;

        var pathRaw = Eval(j.Path, row, table, colNameToIndex);
        var path = pathRaw?.ToString();
        if (string.IsNullOrWhiteSpace(path))
            return target;

        try
        {
            // Normalize JSON path: MySQL uses $[*].name, .NET uses $[*].name
            var jsonStr = target switch
            {
                string s => s,
                byte[] b => System.Text.Encoding.UTF8.GetString(b),
                _ => JsonSerializer.Serialize(target)
            };

            using var doc = JsonDocument.Parse(jsonStr);
            var result = NavigateJsonPath(doc.RootElement, path!);

            if (j.Unquote)
                return result?.GetRawText()?.Trim('"');

            return result?.GetRawText();
        }
        catch
        {
            return target?.ToString();
        }
    }

    private static JsonElement? NavigateJsonPath(JsonElement element, string path)
    {
        if (string.IsNullOrEmpty(path))
            return element;

        // Handle simple JSON path expressions: $.key, $[*], $[*].name, $[0].key
        if (!path.StartsWith("$", StringComparison.Ordinal))
            return element;

        var segments = path[1..].Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
        var current = element;

        foreach (var segment in segments)
        {
            if (segment == "[*]")
            {
                // Return array as-is or first element
                return current.ValueKind == JsonValueKind.Array && current.GetArrayLength() > 0
                    ? current[0]
                    : current;
            }

            if (segment.StartsWith("[", StringComparison.Ordinal) && segment.EndsWith("]", StringComparison.Ordinal))
            {
                if (int.TryParse(segment[1..^1], NumberStyles.Integer, CultureInfo.InvariantCulture, out var idx))
                {
                    current = current.ValueKind == JsonValueKind.Array && idx < current.GetArrayLength()
                        ? current[idx]
                        : default;
                    if (current.ValueKind == JsonValueKind.Undefined)
                        return null;
                    continue;
                }
                return null;
            }

            if (current.ValueKind == JsonValueKind.Object
                && current.TryGetProperty(segment, out var prop))
            {
                current = prop;
            }
            else if (current.ValueKind == JsonValueKind.Array)
            {
                // $[*].name: iterate array, collect all 'name' values
                var results = new List<JsonElement>();
                foreach (var item in current.EnumerateArray())
                {
                    if (item.ValueKind == JsonValueKind.Object
                        && item.TryGetProperty(segment, out var arrProp))
                    {
                        results.Add(arrProp);
                    }
                }

                if (results.Count == 0)
                    return null;
                if (results.Count == 1)
                    return results[0];

                // Return first for index key purposes
                return results[0];
            }
            else
            {
                return null;
            }
        }

        return current;
    }

    private static object? EvalBetween(
        BetweenExpr be,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var expr = Eval(be.Expr, row, table, colNameToIndex);
        var low = Eval(be.Low, row, table, colNameToIndex);
        var high = Eval(be.High, row, table, colNameToIndex);
        if (expr is null || low is null || high is null)
            return null;

        var result = Compare(expr, low) >= 0 && Compare(expr, high) <= 0;
        return be.Negated ? !result : result;
    }

    private static object? EvalIn(
        InExpr ie,
        IReadOnlyDictionary<int, object?> row,
        ITableMock table,
        Dictionary<string, int> colNameToIndex)
    {
        var left = Eval(ie.Left, row, table, colNameToIndex);
        if (left is null)
            return null;

        foreach (var item in ie.Items)
        {
            var v = Eval(item, row, table, colNameToIndex);
            if (Equals(left, v))
                return true;
        }

        return false;
    }

    private static object? EvalSubstring(object?[] args)
    {
        if (args.Length < 2 || args[0] is not string str)
            return null;

        var start = Convert.ToInt32(args[1], CultureInfo.InvariantCulture);
        if (start < 1) start = 1;

        if (args.Length >= 3 && args[2] is not null)
        {
            var len = Convert.ToInt32(args[2], CultureInfo.InvariantCulture);
            return str.Substring(start - 1, Math.Min(len, str.Length - start + 1));
        }

        return start <= str.Length ? str[(start - 1)..] : string.Empty;
    }

    private static decimal ToNumeric(object? value)
    {
        if (value is null) return 0;
        if (value is decimal d) return d;
        if (value is int i) return i;
        if (value is long l) return l;
        if (value is double db) return (decimal)db;
        if (value is float f) return (decimal)f;
        if (value is short s) return s;
        if (value is byte b) return b;
        if (value is string str && decimal.TryParse(str, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsed))
            return parsed;
        return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
    }

    private static int Compare(object? left, object? right)
    {
        if (left is null && right is null) return 0;
        if (left is null) return -1;
        if (right is null) return 1;

        if (left is IComparable lc && right is IComparable rc)
        {
            try { return lc.CompareTo(rc); }
            catch { return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase); }
        }

        return string.Compare(left.ToString(), right.ToString(), StringComparison.OrdinalIgnoreCase);
    }

    private static object? EvalJsonExtract(object? jsonArg, object? pathArg)
    {
        if (jsonArg is not string jsonStr || pathArg is not string path)
            return null;

        if (string.IsNullOrWhiteSpace(path))
            return null;

        try
        {
            using var doc = JsonDocument.Parse(jsonStr);
            // Ensure path starts with $
            if (!path.StartsWith("$", StringComparison.Ordinal))
                path = "$." + path;
            var result = NavigateJsonPath(doc.RootElement, path);
            return result?.GetRawText();
        }
        catch
        {
            return null;
        }
    }
}
