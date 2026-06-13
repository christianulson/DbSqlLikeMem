namespace DbSqlLikeMem;

internal static class AstQuerySharedTextFunctionEvaluator
{
    private delegate bool TextFunctionHandler(QueryExecutionContext context, FunctionCallExpr fn, Func<int, object?> evalArg, out object? result);

    private static readonly Dictionary<string, TextFunctionHandler> _textFunctionDispatch = new(StringComparer.OrdinalIgnoreCase)
    {
        ["ASCII"] = static (_, _, arg, out result) => TryEvalAsciiFunction(arg, out result),
        ["CHAR"] = static (_, _, arg, out result) => TryEvalCharFunction(arg, out result),
        ["NCHAR"] = static (_, _, arg, out result) => TryEvalCharFunction(arg, out result),
        ["LIKE"] = static (ctx, fn, arg, out result) => TryEvalLikeFunction(ctx, fn, arg, out result),
        ["LOWER"] = static (_, _, arg, out result) => TryEvalLowerFunction(arg, out result),
        ["LCASE"] = static (_, _, arg, out result) => TryEvalLowerFunction(arg, out result),
        ["UPPER"] = static (_, _, arg, out result) => TryEvalUpperFunction(arg, out result),
        ["UCASE"] = static (_, _, arg, out result) => TryEvalUpperFunction(arg, out result),
        ["TRIM"] = static (_, _, arg, out result) => TryEvalTrimFunction(arg, out result),
        ["RTRIM"] = static (_, _, arg, out result) => TryEvalRTrimFunction(arg, out result),
        ["LTRIM"] = static (_, _, arg, out result) => TryEvalLTrimFunction(arg, out result),
        ["LENGTH"] = static (_, _, arg, out result) => TryEvalLengthFunction(arg, out result),
        ["CHAR_LENGTH"] = static (_, _, arg, out result) => TryEvalLengthFunction(arg, out result),
        ["CHARACTER_LENGTH"] = static (_, _, arg, out result) => TryEvalLengthFunction(arg, out result),
        ["LEN"] = static (_, _, arg, out result) => TryEvalLengthFunction(arg, out result),
        ["SUBSTRING"] = static (_, fn, arg, out result) => TryEvalSubstringFunction(fn, arg, out result),
        ["SUBSTR"] = static (_, fn, arg, out result) => TryEvalSubstringFunction(fn, arg, out result),
        ["MID"] = static (_, fn, arg, out result) => TryEvalSubstringFunction(fn, arg, out result),
        ["LOCATE"] = static (ctx, fn, arg, out result) => TryEvalLocateFunction(ctx, fn, arg, out result),
        [SqlConst.LEFT] = static (_, fn, arg, out result) => TryEvalLeftFunction(fn, arg, out result),
        ["UNICODE"] = static (_, _, arg, out result) => TryEvalUnicodeFunction(arg, out result),
        ["SPACE"] = static (_, _, arg, out result) => TryEvalSpaceFunction(arg, out result),
        [SqlConst.RIGHT] = static (_, fn, arg, out result) => TryEvalRightFunction(fn, arg, out result),
        ["INSTR"] = static (ctx, _, arg, out result) => TryEvalInstrFunction(ctx, arg, out result),
        ["LPAD"] = static (_, fn, arg, out result) => TryEvalLpadFunction(fn, arg, out result),
        ["REPLACE"] = static (_, _, arg, out result) => TryEvalReplaceFunction(arg, out result),
        ["OVERLAY"] = static (_, fn, arg, out result) => TryEvalOverlayFunction(fn, arg, out result),
        ["REVERSE"] = static (_, _, arg, out result) => TryEvalReverseFunction(arg, out result),
        ["REPEAT"] = static (_, _, arg, out result) => TryEvalRepeatFunction(arg, out result),
        ["TRANSLATE"] = static (_, fn, arg, out result) => TryEvalTranslateFunction(fn, arg, out result),
        ["TRANSLATE...USING"] = static (_, fn, arg, out result) => TryEvalTranslateFunction(fn, arg, out result),
        ["BIT_LENGTH"] = static (_, _, arg, out result) => TryEvalBitLengthFunction(arg, out result),
        ["OCTET_LENGTH"] = static (_, _, arg, out result) => TryEvalOctetLengthFunction(arg, out result),
        ["POSITION"] = static (_, _, arg, out result) => TryEvalPositionFunction(arg, out result),
        ["RPAD"] = static (_, fn, arg, out result) => TryEvalPadRightFunction(fn, arg, out result),
    };

    internal static bool TryEvaluate(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_textFunctionDispatch.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

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

        var text = ToInvariantText(value);
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

        result = ToInvariantText(value) ?? string.Empty;
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

        var escape = fn.Args.Count > 2 ? ToInvariantText(evalArg(2)) : null;
        var escapeText = string.IsNullOrEmpty(escape) ? null : escape![0].ToString(CultureInfo.InvariantCulture);
        var matches = context.Like(ToInvariantText(value), ToInvariantText(pattern), escapeText);
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

        var text = ToInvariantText(value);
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
        result = AstQueryExecutorBase.IsNullish(value) ? null : ToInvariantText(value).ToLowerInvariant();
        return true;
    }

    private static bool TryEvalUpperFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : ToInvariantText(value).ToUpperInvariant();
        return true;
    }

    private static bool TryEvalTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : ToInvariantText(value).Trim();
        return true;
    }

    private static bool TryEvalRTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : ToInvariantText(value).TrimEnd();
        return true;
    }

    private static bool TryEvalLTrimFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : ToInvariantText(value).TrimStart();
        return true;
    }

    private static bool TryEvalLengthFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : (long)ToInvariantText(value).Length;
        return true;
    }

    private static bool TryEvalLocateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;

        var needle = ToInvariantText(evalArg(0));
        var haystack = ToInvariantText(evalArg(1));
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

        var text = ToInvariantText(source);
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

        var haystackText = ToInvariantText(haystack);
        var needleText = ToInvariantText(needle);
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

        var text = ToInvariantText(value);
        var padText = ToInvariantText(padValue);
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
        var prefix = StringCreatePolyfill.CreateRepeated(padNeeded, padText);
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

        result = ToInvariantText(source)
            .Replace(ToInvariantText(from), ToInvariantText(to));
        return true;
    }

    private static bool TryEvalOverlayFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var source = evalArg(0);
        var replacement = evalArg(1);
        var positionValue = evalArg(2);
        var lengthValue = fn.Args.Count > 3 ? evalArg(3) : null;
        if (AstQueryExecutorBase.IsNullish(source)
            || AstQueryExecutorBase.IsNullish(replacement)
            || AstQueryExecutorBase.IsNullish(positionValue)
            || (fn.Args.Count > 3 && AstQueryExecutorBase.IsNullish(lengthValue)))
        {
            result = null;
            return true;
        }

        var text = ToInvariantText(source);
        var replacementText = ToInvariantText(replacement);

        var position = Convert.ToInt32(positionValue.ToDec(), CultureInfo.InvariantCulture);
        var length = fn.Args.Count > 3
            ? Convert.ToInt32(lengthValue!.ToDec(), CultureInfo.InvariantCulture)
            : replacementText.Length;
        if (position <= 0)
        {
            result = text;
            return true;
        }

        var startIndex = Math.Min(position - 1, text.Length);
        var overwriteLength = Math.Max(0, length);
        var endIndex = Math.Min(text.Length, startIndex + overwriteLength);
        result = text[..startIndex] + replacementText + text[endIndex..];
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

        var text = ToInvariantText(value);
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

        var text = ToInvariantText(textValue);
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

        var text = ToInvariantText(textValue);
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

        var text = ToInvariantText(textValue);
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

        var source = ToInvariantText(evalArg(0));
        var from = ToInvariantText(evalArg(1));
        var to = ToInvariantText(evalArg(2));

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

        var text = ToInvariantText(value);
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

        var text = ToInvariantText(value);
        result = Encoding.UTF8.GetByteCount(text);
        return true;
    }

    private static bool TryEvalPositionFunction(
        Func<int, object?> evalArg,
        out object? result)
    {
        var needle = ToInvariantText(evalArg(0));
        var haystack = ToInvariantText(evalArg(1));
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

        var text = ToInvariantText(value);
        var padText = ToInvariantText(padValue);
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

        var suffixLen = len - text.Length;
        var suffix = StringCreatePolyfill.CreateRepeated(suffixLen, padText);
        result = text + suffix;
        return true;
    }

    private static string ToInvariantText(object? value)
    {
        if (value is null || value is DBNull)
            return string.Empty;

        if (value is string text)
            return text;

        if (value is IFormattable formattable)
            return formattable.ToString(null, CultureInfo.InvariantCulture);

        return value.ToString() ?? string.Empty;
    }
}
