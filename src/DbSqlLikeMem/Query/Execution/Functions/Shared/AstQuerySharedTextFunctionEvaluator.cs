using System.Globalization;
using System.Text;

namespace DbSqlLikeMem;

internal static class AstQuerySharedTextFunctionEvaluator
{
    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        if (string.Equals(fn.Name, "ASCII", StringComparison.OrdinalIgnoreCase))
            return TryEvalAsciiFunction(evalArg, out result);

        if (string.Equals(fn.Name, "CHAR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "NCHAR", StringComparison.OrdinalIgnoreCase))
            return TryEvalCharFunction(evalArg, out result);

        if (string.Equals(fn.Name, "LIKE", StringComparison.OrdinalIgnoreCase))
            return TryEvalLikeFunction(context, fn, evalArg, out result);

        if (string.Equals(fn.Name, "LOWER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "LCASE", StringComparison.OrdinalIgnoreCase))
            return TryEvalLowerFunction(evalArg, out result);

        if (string.Equals(fn.Name, "UPPER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "UCASE", StringComparison.OrdinalIgnoreCase))
            return TryEvalUpperFunction(evalArg, out result);

        if (string.Equals(fn.Name, "TRIM", StringComparison.OrdinalIgnoreCase))
            return TryEvalTrimFunction(evalArg, out result);

        if (string.Equals(fn.Name, "RTRIM", StringComparison.OrdinalIgnoreCase))
            return TryEvalRTrimFunction(evalArg, out result);

        if (string.Equals(fn.Name, "LTRIM", StringComparison.OrdinalIgnoreCase))
            return TryEvalLTrimFunction(evalArg, out result);

        if (string.Equals(fn.Name, "LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CHAR_LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CHARACTER_LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "LEN", StringComparison.OrdinalIgnoreCase))
            return TryEvalLengthFunction(evalArg, out result);

        if (string.Equals(fn.Name, "SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "SUBSTR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "MID", StringComparison.OrdinalIgnoreCase))
            return TryEvalSubstringFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "LOCATE", StringComparison.OrdinalIgnoreCase))
            return TryEvalLocateFunction(context, fn, evalArg, out result);

        if (string.Equals(fn.Name, SqlConst.LEFT, StringComparison.OrdinalIgnoreCase))
            return TryEvalLeftFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "UNICODE", StringComparison.OrdinalIgnoreCase))
            return TryEvalUnicodeFunction(evalArg, out result);

        if (string.Equals(fn.Name, "SPACE", StringComparison.OrdinalIgnoreCase))
            return TryEvalSpaceFunction(evalArg, out result);

        if (string.Equals(fn.Name, SqlConst.RIGHT, StringComparison.OrdinalIgnoreCase))
            return TryEvalRightFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "INSTR", StringComparison.OrdinalIgnoreCase))
            return TryEvalInstrFunction(context, evalArg, out result);

        if (string.Equals(fn.Name, "LPAD", StringComparison.OrdinalIgnoreCase))
            return TryEvalLpadFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "REPLACE", StringComparison.OrdinalIgnoreCase))
            return TryEvalReplaceFunction(evalArg, out result);

        if (string.Equals(fn.Name, "REVERSE", StringComparison.OrdinalIgnoreCase))
            return TryEvalReverseFunction(evalArg, out result);

        if (string.Equals(fn.Name, "REPEAT", StringComparison.OrdinalIgnoreCase))
            return TryEvalRepeatFunction(evalArg, out result);

        if (string.Equals(fn.Name, "TRANSLATE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "TRANSLATE...USING", StringComparison.OrdinalIgnoreCase))
            return TryEvalTranslateFunction(fn, evalArg, out result);

        if (string.Equals(fn.Name, "BIT_LENGTH", StringComparison.OrdinalIgnoreCase))
            return TryEvalBitLengthFunction(evalArg, out result);

        if (string.Equals(fn.Name, "OCTET_LENGTH", StringComparison.OrdinalIgnoreCase))
            return TryEvalOctetLengthFunction(evalArg, out result);

        if (string.Equals(fn.Name, "POSITION", StringComparison.OrdinalIgnoreCase))
            return TryEvalPositionFunction(evalArg, out result);

        if (string.Equals(fn.Name, "RPAD", StringComparison.OrdinalIgnoreCase))
            return TryEvalPadRightFunction(fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalAsciiFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = text.Length == 0 ? 0 : (int)text[0];
        return true;
    }

    private static bool TryEvalCharFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var codePoint = Convert.ToInt32(value, CultureInfo.InvariantCulture);
            result = char.ConvertFromUtf32(codePoint);
            return true;
        }
        catch
        {
            // Fall back to textual conversion when the argument is not numeric.
        }

        result = value!.ToString() ?? string.Empty;
        return true;
    }

    private static bool TryEvalLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        var pattern = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(pattern))
        {
            result = null;
            return true;
        }

        var escape = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var escapeText = string.IsNullOrEmpty(escape) ? null : escape![0].ToString();
        var matches = context.Like(value?.ToString(), pattern?.ToString(), escapeText);
        result = matches ? 1 : 0;
        return true;
    }

    private static bool TryEvalUnicodeFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (text.Length == 0)
        {
            result = null;
            return true;
        }

        var codePoint = text.Length >= 2 && char.IsSurrogatePair(text, 0)
            ? char.ConvertToUtf32(text, 0)
            : text[0];
        result = codePoint;
        return true;
    }

    private static bool TryEvalLowerFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value!.ToString()!.ToLowerInvariant();
        return true;
    }

    private static bool TryEvalUpperFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value!.ToString()!.ToUpperInvariant();
        return true;
    }

    private static bool TryEvalTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value!.ToString()!.Trim();
        return true;
    }

    private static bool TryEvalRTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value!.ToString()!.TrimEnd();
        return true;
    }

    private static bool TryEvalLTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value!.ToString()!.TrimStart();
        return true;
    }

    private static bool TryEvalLengthFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : (long)(value!.ToString()!.Length);
        return true;
    }

    private static bool TryEvalLocateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;

        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var startPosition = fn.Args.Count > 2 ? evalArg(2) : null;
        var startIndex = 0;

        if (!AstQueryExecutorBase.IsNullish(startPosition))
        {
            startIndex = Convert.ToInt32(startPosition.ToDec()) - 1;
            if (startIndex < 0)
            {
                result = 0;
                return true;
            }
        }

        if (needle.Length == 0)
        {
            result = startIndex + 1;
            return true;
        }

        var index = haystack.IndexOf(needle, startIndex, context.Dialect.TextComparison);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalSubstringFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;

        var source = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(source))
        {
            result = null;
            return true;
        }

        var text = source!.ToString() ?? string.Empty;
        var position = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(position))
        {
            result = null;
            return true;
        }

        var start = Convert.ToInt32(position.ToDec()) - 1;
        if (start < 0)
            start = 0;

        if (start >= text.Length)
        {
            result = string.Empty;
            return true;
        }

        var lengthValue = fn.Args.Count > 2 ? evalArg(2) : null;
        if (AstQueryExecutorBase.IsNullish(lengthValue))
        {
            result = text[start..];
            return true;
        }

        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (start + length > text.Length)
            length = text.Length - start;

        result = text.Substring(start, length);
        return true;
    }

    private static bool TryEvalSpaceFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var count = Convert.ToInt32(value.ToDec());
        if (count <= 0)
        {
            result = string.Empty;
            return true;
        }

        result = new string(' ', count);
        return true;
    }

    private static bool TryEvalInstrFunction(
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        var haystack = evalArg(0);
        var needle = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(haystack) || AstQueryExecutorBase.IsNullish(needle))
        {
            result = null;
            return true;
        }

        var haystackText = haystack?.ToString() ?? string.Empty;
        var needleText = needle?.ToString() ?? string.Empty;
        if (needleText.Length == 0)
        {
            result = 1;
            return true;
        }

        var index = haystackText.IndexOf(needleText, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalLpadFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        var lenValue = evalArg(1);
        var padValue = fn.Args.Count > 2 ? evalArg(2) : " ";

        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(lenValue) || AstQueryExecutorBase.IsNullish(padValue))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var padText = padValue?.ToString() ?? string.Empty;
        var len = Convert.ToInt32(lenValue.ToDec());

        if (len < 0 || padText.Length == 0)
        {
            result = null;
            return true;
        }

        if (len == 0)
        {
            result = string.Empty;
            return true;
        }

        if (text.Length >= len)
        {
            result = text.Substring(0, len);
            return true;
        }

        var padNeeded = len - text.Length;
        var sb = new StringBuilder(len);
        while (sb.Length < padNeeded)
            sb.Append(padText);

        var prefix = sb.ToString().Substring(0, padNeeded);
        result = prefix + text;
        return true;
    }

    private static bool TryEvalReplaceFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var source = evalArg(0);
        var from = evalArg(1);
        var to = evalArg(2);
        if (AstQueryExecutorBase.IsNullish(source) || AstQueryExecutorBase.IsNullish(from) || AstQueryExecutorBase.IsNullish(to))
        {
            result = null;
            return true;
        }

        result = (source!.ToString() ?? string.Empty)
            .Replace(from!.ToString() ?? string.Empty, to!.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvalReverseFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var chars = text.ToCharArray();
        Array.Reverse(chars);
        result = new string(chars);
        return true;
    }

    private static bool TryEvalLeftFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, SqlConst.LEFT, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var lengthValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(textValue) || AstQueryExecutorBase.IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (length >= text.Length)
        {
            result = text;
            return true;
        }

        result = text[..length];
        return true;
    }

    private static bool TryEvalRightFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, SqlConst.RIGHT, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var lengthValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(textValue) || AstQueryExecutorBase.IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = string.Empty;
            return true;
        }

        if (length >= text.Length)
        {
            result = text;
            return true;
        }

        result = text[^length..];
        return true;
    }

    private static bool TryEvalRepeatFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var textValue = evalArg(0);
        var countValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(textValue) || AstQueryExecutorBase.IsNullish(countValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var count = Convert.ToInt32(countValue.ToDec());
        if (count <= 0)
        {
            result = string.Empty;
            return true;
        }

        var sb = new StringBuilder(text.Length * count);
        for (var i = 0; i < count; i++)
            sb.Append(text);
        result = sb.ToString();
        return true;
    }

    private static bool TryEvalTranslateFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;

        if (fn.Args.Count < 3)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var from = evalArg(1)?.ToString() ?? string.Empty;
        var to = evalArg(2)?.ToString() ?? string.Empty;

        var builder = new StringBuilder(source.Length);
        foreach (var ch in source)
        {
            var index = from.IndexOf(ch);
            if (index < 0)
            {
                builder.Append(ch);
                continue;
            }

            if (index < to.Length)
                builder.Append(to[index]);
        }

        result = builder.ToString();
        return true;
    }

    private static bool TryEvalBitLengthFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte[] bytes)
        {
            result = bytes.Length * 8;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = text.Length * 8;
        return true;
    }

    private static bool TryEvalOctetLengthFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte[] bytes)
        {
            result = bytes.Length;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = Encoding.UTF8.GetByteCount(text);
        return true;
    }

    private static bool TryEvalPositionFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        if (needle.Length == 0)
        {
            result = 1;
            return true;
        }

        var index = haystack.IndexOf(needle, StringComparison.Ordinal);
        result = index < 0 ? 0 : index + 1;
        return true;
    }

    private static bool TryEvalPadRightFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "RPAD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var lenValue = evalArg(1);
        var padValue = fn.Args.Count > 2 ? evalArg(2) : " ";

        if (AstQueryExecutorBase.IsNullish(value) || AstQueryExecutorBase.IsNullish(lenValue) || AstQueryExecutorBase.IsNullish(padValue))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var padText = padValue?.ToString() ?? string.Empty;
        var len = Convert.ToInt32(lenValue.ToDec());

        if (len < 0 || padText.Length == 0)
        {
            result = null;
            return true;
        }

        if (len == 0)
        {
            result = string.Empty;
            return true;
        }

        if (text.Length >= len)
        {
            result = text.Substring(0, len);
            return true;
        }

        var padNeeded = len - text.Length;
        var sb = new StringBuilder(len);
        sb.Append(text);
        while (sb.Length < len)
            sb.Append(padText);

        result = sb.ToString().Substring(0, len);
        return true;
    }
}
