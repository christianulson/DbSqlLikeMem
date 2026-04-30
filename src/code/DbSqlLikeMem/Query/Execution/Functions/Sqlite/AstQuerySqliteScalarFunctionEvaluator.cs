using System.Collections.Concurrent;

namespace DbSqlLikeMem;

internal static class AstQuerySqliteScalarFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();
    private static readonly ConcurrentDictionary<string, Regex> _globRegexCache = new(StringComparer.Ordinal);

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        if (TryEvalSqliteSystemFunctions(context, fn, evalArg, out result))
            return true;

        if (TryEvalSqliteJsonFunctions(context, fn, evalArg, out result))
            return true;

        result = null;
        return false;
    }

    internal static bool TryEvalSqliteSystemFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();

        if (name is "SQLITE_VERSION" or "SQLITE3_LIBVERSION")
        {
            var version = context.Dialect?.Version ?? 3;
            result = $"{version}.0.0";
            return true;
        }

        if (name is "SQLITE_SOURCE_ID" or "SQLITE3_SOURCEID")
        {
            result = "DbSqlLikeMem.Sqlite";
            return true;
        }

        if (name is "SQLITE_COMPILEOPTION_GET" or "SQLITE3_COMPILEOPTION_GET")
        {
            result = null;
            return true;
        }

        if (name is "SQLITE_COMPILEOPTION_USED" or "SQLITE3_COMPILEOPTION_USED")
        {
            result = 0;
            return true;
        }

        if (name is "SQLITE_OFFSET")
        {
            result = 0L;
            return true;
        }

        if (name is "CHANGES"
            or "SQLITE3_CHANGES64"
            or "SQLITE3_TOTAL_CHANGES64"
            or "TOTAL_CHANGES")
        {
            result = context.Connection.GetLastChangesRows();
            return true;
        }

        if (name is "LOAD_EXTENSION" or "SQLITE3_LOAD_EXTENSION" or "SQLITE3_ENABLE_LOAD_EXTENSION")
        {
            result = 0;
            return true;
        }

        if (name is "READFILE")
        {
            var path = evalArg(0)?.ToString();
            if (string.IsNullOrWhiteSpace(path) || !File.Exists(path))
            {
                result = null;
                return true;
            }

            result = File.ReadAllBytes(path);
            return true;
        }

        if (name is "SQLITE3_LAST_INSERT_ROWID")
        {
            result = context.Connection.GetLastInsertId() ?? 0;
            return true;
        }

        if (name is "LAST_INSERT_ROWID")
        {
            result = context.Connection.GetLastInsertId() ?? 0;
            return true;
        }

        if (name is "SQLITE3_CREATE_FUNCTION"
            or "SQLITE3_CREATE_WINDOW_FUNCTION"
            or "SQLITE3_STEP")
        {
            result = 0;
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalSqliteJsonFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (string.Equals(fn.Name, "JSON_EACH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_TREE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSONB_EACH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSONB_TREE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.GetRawText();
            return true;
        }

        if (string.Equals(fn.Name, "JSONB_EXTRACT", StringComparison.OrdinalIgnoreCase))
        {
            var shim = new FunctionCallExpr("JSON_EXTRACT", fn.Args)
                .BindScalarFunctionDefinition(context.Dialect);
            result = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction(
                context,
                shim,
                evalArg,
                out var jsonExtractResult)
                ? jsonExtractResult
                : null;
            return true;
        }

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);
        Register(handlers, TryEvalGlobFunction, "GLOB");
        Register(handlers, TryEvalPrintfFunction, "PRINTF", "FORMAT", "SQLITE3_MPRINTF");
        Register(handlers, TryEvalRandomBlobFunction, "RANDOMBLOB");
        Register(handlers, TryEvalZeroBlobFunction, "ZEROBLOB", "SQLITE3_RESULT_ZEROBLOB");
        Register(handlers, TryEvalTypeofFunction, "TYPEOF");
        Register(handlers, TryEvalUnistrFunction, "UNISTR");
        Register(handlers, TryEvalUnistrQuoteFunction, "UNISTR_QUOTE");
        Register(handlers, TryEvalLikelyFunction, "LIKELY");
        Register(handlers, TryEvalUnlikelyFunction, "UNLIKELY");
        Register(handlers, TryEvalLikelihoodFunction, "LIKELIHOOD");
        Register(handlers, TryEvalJsonTableFunction, "JSON_EACH", "JSON_TREE", "JSONB_EACH", "JSONB_TREE");
        Register(handlers, TryEvalJsonBlobExtractFunction, "JSONB_EXTRACT");
        return handlers;
    }

    private static void Register(
        Dictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers.Add(name, handler);
    }

    private static bool TryEvalGlobFunction(
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

        var text = value?.ToString() ?? string.Empty;
        var patternText = pattern?.ToString() ?? string.Empty;
        var regex = GlobToRegex(patternText);
        result = regex.IsMatch(text) ? 1 : 0;
        return true;
    }

    private static Regex GlobToRegex(string pattern)
    {
        return _globRegexCache.GetOrAdd(pattern, BuildGlobRegex);

        static Regex BuildGlobRegex(string pattern)
        {
            var builder = new StringBuilder(pattern.Length + 2);
            for (var i = 0; i < pattern.Length; i++)
            {
                var ch = pattern[i];
                switch (ch)
                {
                    case '*':
                        builder.Append(".*");
                        break;
                    case '?':
                        builder.Append(".");
                        break;
                    case '[':
                        var end = pattern.IndexOf(']', i + 1);
                        if (end > i)
                        {
                            var content = pattern.Substring(i + 1, end - i - 1);
                            builder.Append('[').Append(Regex.Escape(content).Replace("\\-", "-")).Append(']');
                            i = end;
                        }
                        else
                        {
                            builder.Append("\\[");
                        }
                        break;
                    default:
                        builder.Append(Regex.Escape(ch.ToString()));
                        break;
                }
            }

            builder.Append('$');
            return new Regex(builder.ToString(), RegexOptions.CultureInvariant);
        }
    }

    private static bool TryEvalPrintfFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isPrintf = string.Equals(fn.Name, "PRINTF", StringComparison.OrdinalIgnoreCase);
        var isFormat = string.Equals(fn.Name, "FORMAT", StringComparison.OrdinalIgnoreCase);
        var isMPrintf = string.Equals(fn.Name, "SQLITE3_MPRINTF", StringComparison.OrdinalIgnoreCase);
        if (!(isPrintf || isFormat || isMPrintf))
        {
            result = null;
            return false;
        }

        var format = evalArg(0)?.ToString() ?? string.Empty;
        var args = new object?[Math.Max(0, fn.Args.Count - 1)];
        for (var i = 1; i < fn.Args.Count; i++)
            args[i - 1] = evalArg(i);

        result = AstQueryFormatFunctionHelper.FormatPrintf(format, args, FormatSqliteValueAsText);
        return true;
    }

    private static string FormatSqliteValueAsText(object? value)
    {
        if (AstQueryExecutorBase.IsNullish(value))
            return string.Empty;

        return value switch
        {
            string text => text,
            char ch => ch.ToString(),
            bool b => b ? "1" : "0",
            sbyte or byte or short or ushort or int or uint or long or ulong => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
            float f => FormatSqliteReal(f),
            double d => FormatSqliteReal(d),
            decimal dec => FormatSqliteReal(Convert.ToDouble(dec, CultureInfo.InvariantCulture)),
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty,
        };
    }

    private static string FormatSqliteReal(double value)
    {
        if (double.IsNaN(value))
            return "NaN";

        if (double.IsPositiveInfinity(value))
            return "Inf";

        if (double.IsNegativeInfinity(value))
            return "-Inf";

        var text = value.ToString("G17", CultureInfo.InvariantCulture);
        if (text.IndexOf('.') < 0 && text.IndexOf('e') < 0 && text.IndexOf('E') < 0)
            text += ".0";

        return text;
    }

    private static bool TryEvalRandomBlobFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRandomBlobLikeFunction(context, fn, evalArg, randomize: true, out result);

    private static bool TryEvalZeroBlobFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalRandomBlobLikeFunction(context, fn, evalArg, randomize: false, out result);

    private static bool TryEvalRandomBlobLikeFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        bool randomize,
        out object? result)
    {
        _ = context;
        _ = fn;

        var lengthValue = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(lengthValue))
        {
            result = null;
            return true;
        }

        var length = Convert.ToInt32(lengthValue.ToDec());
        if (length <= 0)
        {
            result = Array.Empty<byte>();
            return true;
        }

        var buffer = new byte[length];
        if (randomize)
        {
            using var rng = RandomNumberGenerator.Create();
            rng.GetBytes(buffer);
        }

        result = buffer;
        return true;
    }

    private static bool TryEvalTypeofFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = "null";
            return true;
        }

        result = value switch
        {
            sbyte or byte or short or ushort or int or uint or long or ulong or bool => "integer",
            float or double or decimal => "real",
            byte[] => "blob",
            _ => "text"
        };
        return true;
    }

    private static bool TryEvalUnistrFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        result = UnescapeUnicodeLiteral(value?.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvalUnistrQuoteFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = $"'{text.Replace("'", "''")}'";
        return true;
    }

    private static bool TryEvalLikelyFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(context, fn, evalArg, out result);

    private static bool TryEvalUnlikelyFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(context, fn, evalArg, out result);

    private static bool TryEvalLikelihoodFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(context, fn, evalArg, out result);

    private static bool TryEvalJsonTableFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        _ = fn;

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is null || !AstQueryJsonSharedFunctionEvaluator.TryParseJsonElement(value, out var element))
        {
            result = null;
            return true;
        }

        result = element.GetRawText();
        return true;
    }

    private static bool TryEvalJsonBlobExtractFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var shim = new FunctionCallExpr("JSON_EXTRACT", fn.Args)
            .BindScalarFunctionDefinition(context.Dialect);
        result = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction(
            context,
            shim,
            evalArg,
            out var jsonExtractResult)
        ? jsonExtractResult
        : null;
        return true;
    }

    private static bool TryEvalLikelihoodFunctions(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        result = evalArg(0);
        return true;
    }

    private static string UnescapeUnicodeLiteral(string input)
    {
        if (string.IsNullOrEmpty(input))
            return input;

        var builder = new StringBuilder();
        for (var i = 0; i < input.Length; i++)
        {
            var ch = input[i];
            if (ch == '\\' && i + 1 < input.Length)
            {
                if (input[i + 1] == '+' && i + 9 < input.Length)
                {
                    var hex = input.Substring(i + 2, 6);
                    if (int.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var codePoint))
                    {
                        builder.Append(char.ConvertFromUtf32(codePoint));
                        i += 7;
                        continue;
                    }
                }

                if (i + 5 <= input.Length)
                {
                    var hex = input.Substring(i + 1, 4);
                    if (int.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var codeUnit))
                    {
                        builder.Append((char)codeUnit);
                        i += 4;
                        continue;
                    }
                }
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }
}
