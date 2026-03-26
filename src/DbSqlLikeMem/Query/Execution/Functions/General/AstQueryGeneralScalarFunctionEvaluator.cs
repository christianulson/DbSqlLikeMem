using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal delegate bool AstQueryGeneralScalarFunctionHandler(
    FunctionCallExpr fn,
    QueryExecutionContext context,
    Func<int, object?> evalArg,
    out object? result);

internal class AstQueryGeneralScalarFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();
    private static readonly Random _sharedRandom = new();
    private static readonly object _randomLock = new();
    private readonly DbConnectionMockBase _cnn;

    internal AstQueryGeneralScalarFunctionEvaluator(DbConnectionMockBase cnn)
        => _cnn = cnn ?? throw new ArgumentNullException(nameof(cnn));

    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(fn, context, evalArg, out result);

        result = null;
        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalMinMaxFunctions, "GREATEST", "LEAST");
        Register(handlers, TryEvalAcosFunction, "ACOS");
        Register(handlers, TryEvalAsinFunction, "ASIN");
        Register(handlers, TryEvalAtanFunction, "ATAN");
        Register(handlers, TryEvalAtan2Function, "ATAN2");
        Register(handlers, TryEvalCeilingFunction, "CEIL", "CEILING");
        Register(handlers, TryEvalCosFunction, "COS");
        Register(handlers, TryEvalCotFunction, "COT");
        Register(handlers, TryEvalLocateFunction, "LOCATE");
        Register(handlers, TryEvalLogFunctions, "LN", "LOG", "LOG10", "LOG2");
        Register(handlers, TryEvalInstrFunction, "INSTR");
        Register(handlers, TryEvalGlobFunction, "GLOB");
        Register(handlers, TryEvalLikeFunction, "LIKE");
        Register(handlers, TryEvalPatIndexFunction, "PATINDEX");
        Register(handlers, TryEvalPrintfFunction, "PRINTF", "FORMAT", "SQLITE3_MPRINTF");
        Register(handlers, TryEvalRandomFunction, "RANDOM");
        Register(handlers, TryEvalRandomBlobFunction, "RANDOMBLOB");
        Register(handlers, TryEvalZeroBlobFunction, "ZEROBLOB", "SQLITE3_RESULT_ZEROBLOB");
        Register(handlers, TryEvalTypeofFunction, "TYPEOF");
        Register(handlers, TryEvalUnicodeFunction, "UNICODE");
        Register(handlers, TryEvalUnistrFunction, "UNISTR");
        Register(handlers, TryEvalUnistrQuoteFunction, "UNISTR_QUOTE");
        Register(handlers, TryEvalLikelyFunction, "LIKELY");
        Register(handlers, TryEvalUnlikelyFunction, "UNLIKELY");
        Register(handlers, TryEvalLikelihoodFunction, "LIKELIHOOD");
        Register(handlers, TryEvalAsciiFunction, "ASCII");
        Register(handlers, TryEvalBasicStringFunctions, "LOWER", "LCASE", "UPPER", "UCASE", "TRIM", "RTRIM", "LTRIM", "TO_CHAR", "LENGTH", "CHAR_LENGTH", "CHARACTER_LENGTH", "LEN");
        Register(handlers, TryEvalPadFunctions, "LPAD");
        Register(handlers, TryEvalMd5Function, "MD5");
        Register(handlers, TryEvalModFunction, "MOD");
        Register(handlers, TryEvalOctFunction, "OCT");
        Register(handlers, TryEvalHexFunction, "HEX");
        Register(handlers, TryEvalUnhexFunction, "UNHEX");
        Register(handlers, TryEvalOctetLengthFunction, "OCTET_LENGTH");
        Register(handlers, TryEvalNameConstFunction, "NAME_CONST");
        Register(handlers, TryEvalOrdFunction, "ORD");
        Register(handlers, TryEvalPositionFunction, "POSITION");
        Register(handlers, TryEvalPiFunction, "PI");
        Register(handlers, TryEvalPowerFunctions, "POWER", "POW");
        Register(handlers, TryEvalQuoteFunction, "QUOTE");
        Register(handlers, TryEvalDegreesFunction, "DEGREES");
        Register(handlers, TryEvalDifferenceFunction, "DIFFERENCE");
        Register(handlers, TryEvalExpFunction, "EXP");
        Register(handlers, TryEvalFloorFunction, "FLOOR");
        Register(handlers, TryEvalSignFunction, "SIGN");
        Register(handlers, TryEvalNumericFunction, "ABS", "ABSVAL", "BIN");
        Register(handlers, AstQueryGeneralDateFunctionEvaluator.TryEvaluate, "DATE", "TIMESTAMP", "DATETIME", "TIME", "STRFTIME", "MAKEDATE", "MAKETIME", "MICROSECOND", "MONTHNAME", "PERIOD_ADD", "PERIOD_DIFF", "QUARTER", "SEC_TO_TIME");
        Register(handlers, AstQuerySqlServerScalarFunctionEvaluator.TryEvaluate, "QUOTENAME", "REPLICATE", "SQUARE", "STUFF", "PARSENAME");
        Register(handlers, TryEvalRadiansFunction, "RADIANS");
        Register(handlers, TryEvalRandFunction, "RAND");
        Register(handlers, TryEvalRepeatFunction, "REPEAT");
        Register(handlers, TryEvalReverseFunction, "REVERSE");
        Register(handlers, TryEvalReplaceFunction, "REPLACE");
        Register(handlers, TryEvalTranslateFunctions, "TRANSLATE");
        Register(handlers, TryEvalCharFunction, "CHAR", "NCHAR");
        Register(handlers, TryEvalLeftFunction, SqlConst.LEFT);
        Register(handlers, TryEvalRightFunction, SqlConst.RIGHT);
        Register(handlers, TryEvalRoundFunction, "ROUND");
        Register(handlers, TryEvalPadRightFunction, "RPAD");
        Register(handlers, TryEvalBitCountFunction, "BIT_COUNT");
        Register(handlers, TryEvalBitLengthFunction, "BIT_LENGTH");
        Register(handlers, TryEvalShaFunctions, "SHA", "SHA1", "SHA2");
        Register(handlers, TryEvalSinFunction, "SIN");
        Register(handlers, TryEvalSoundexFunction, "SOUNDEX");
        Register(handlers, TryEvalSpaceFunction, "SPACE");
        Register(handlers, TryEvalSqrtFunction, "SQRT");
        Register(handlers, TryEvalSubstringIndexFunction, "SUBSTRING_INDEX");
        Register(handlers, TryEvalTanFunction, "TAN");

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

    private static bool TryEvalMinMaxFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isGreatest = string.Equals(fn.Name, "GREATEST", StringComparison.OrdinalIgnoreCase);
        var isLeast = string.Equals(fn.Name, "LEAST", StringComparison.OrdinalIgnoreCase);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        object? current = null;
        foreach (var index in Enumerable.Range(0, fn.Args.Count))
        {
            var value = evalArg(index);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (current is null)
            {
                current = value;
                continue;
            }

            var comparison = current.Compare(value!, context);
            if (isGreatest && comparison < 0)
                current = value;
            else if (isLeast && comparison > 0)
                current = value;
        }

        result = current;
        return true;
    }

    private static bool TryEvalAsciiFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = text.Length == 0 ? 0 : (int)text[0];
        return true;
    }

    private static bool TryEvalBasicStringFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalBasicStringFunction(fn, evalArg, out result);

    private static bool TryEvalLocateFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var needle = evalArg(0)?.ToString() ?? string.Empty;
        var haystack = evalArg(1)?.ToString() ?? string.Empty;
        var startPosition = fn.Args.Count > 2 ? evalArg(2) : null;
        var startIndex = 0;

        if (!IsNullish(startPosition))
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

    private static bool TryEvalLogFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isLn = string.Equals(fn.Name, "LN", StringComparison.OrdinalIgnoreCase);
        var isLog = string.Equals(fn.Name, "LOG", StringComparison.OrdinalIgnoreCase);
        var isLog10 = string.Equals(fn.Name, "LOG10", StringComparison.OrdinalIgnoreCase);
        var isLog2 = string.Equals(fn.Name, "LOG2", StringComparison.OrdinalIgnoreCase);

        var value = evalArg(isLog && fn.Args.Count > 1 ? 1 : 0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        double number;
        try
        {
            number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
        }
        catch
        {
            result = null;
            return true;
        }

        if (number <= 0)
        {
            result = null;
            return true;
        }

        if (isLog10)
        {
            result = Math.Log10(number);
            return true;
        }

        if (isLog2)
        {
            result = AstQueryRuntimeHelper.Log2(number);
            return true;
        }

        if (isLog && fn.Args.Count > 1)
        {
            var baseValue = evalArg(0);
            if (IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            double baseNumber;
            try
            {
                baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            }
            catch
            {
                result = null;
                return true;
            }

            if (baseNumber <= 0 || baseNumber == 1)
            {
                result = null;
                return true;
            }

            result = Math.Log(number, baseNumber);
            return true;
        }

        result = Math.Log(number);
        return true;
    }

    private static bool TryEvalInstrFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var haystack = evalArg(0);
        var needle = evalArg(1);
        if (IsNullish(haystack) || IsNullish(needle))
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

    private static bool TryEvalGlobFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        var pattern = evalArg(1);
        if (IsNullish(value) || IsNullish(pattern))
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

    private static bool TryEvalLikeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        var pattern = evalArg(1);
        if (IsNullish(value) || IsNullish(pattern))
        {
            result = null;
            return true;
        }

        var escape = fn.Args.Count > 2 ? evalArg(2)?.ToString() : null;
        var escapeText = string.IsNullOrEmpty(escape) ? null : escape![0].ToString();
        var matches = value!.ToString()!.Like(pattern!.ToString()!, context, escapeText);
        result = matches ? 1 : 0;
        return true;
    }

    private static bool TryEvalPatIndexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var pattern = evalArg(0);
        var value = evalArg(1);
        if (IsNullish(pattern) || IsNullish(value))
        {
            result = null;
            return true;
        }

        result = value!.ToString()!.PatIndex(pattern!.ToString()!, context);
        return true;
    }

    internal static bool TryEvalDegreesFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var radians = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = radians * (180d / Math.PI);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    internal static bool TryEvalDifferenceFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var first = evalArg(0)?.ToString() ?? string.Empty;
        var second = evalArg(1)?.ToString() ?? string.Empty;
        var soundex1 = ComputeSoundex(first);
        var soundex2 = ComputeSoundex(second);
        var score = 0;
        for (var i = 0; i < Math.Min(soundex1.Length, soundex2.Length); i++)
        {
            if (soundex1[i] == soundex2[i])
                score++;
        }

        result = score;
        return true;
    }

    internal static bool TryEvalExpFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = Math.Exp(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    internal static bool TryEvalFloorFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Floor(dec);
                return true;
            }

            result = Math.Floor(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCeilingFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isCeil = string.Equals(fn.Name, "CEIL", StringComparison.OrdinalIgnoreCase);
        var isCeiling = string.Equals(fn.Name, "CEILING", StringComparison.OrdinalIgnoreCase);
        if (!(isCeil || isCeiling))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            if (value is decimal dec)
            {
                result = Math.Ceiling(dec);
                return true;
            }

            result = Math.Ceiling(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAcosFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Acos(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAsinFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Asin(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAtanFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Atan(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalAtan2Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 2)
            throw new InvalidOperationException("ATAN2() espera 2 argumentos.");

        var yValue = evalArg(0);
        var xValue = evalArg(1);
        if (IsNullish(yValue) || IsNullish(xValue))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Atan2(
                Convert.ToDouble(yValue, CultureInfo.InvariantCulture),
                Convert.ToDouble(xValue, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCosFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            result = Math.Cos(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalCotFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var tangent = Math.Tan(Convert.ToDouble(value, CultureInfo.InvariantCulture));
            result = 1d / tangent;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSignFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number == 0d ? 0 : (number > 0d ? 1 : -1);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    internal static bool TryEvalReplaceFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var source = evalArg(0);
        var from = evalArg(1);
        var to = evalArg(2);
        if (IsNullish(source) || IsNullish(from) || IsNullish(to))
        {
            result = null;
            return true;
        }

        result = (source!.ToString() ?? string.Empty)
            .Replace(from!.ToString() ?? string.Empty, to!.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvalTranslateFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isTranslate = string.Equals(fn.Name, "TRANSLATE", StringComparison.OrdinalIgnoreCase);
        var isTranslateUsing = string.Equals(fn.Name, "TRANSLATE...USING", StringComparison.OrdinalIgnoreCase);
        if (!(isTranslate || isTranslateUsing))
        {
            result = null;
            return false;
        }

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

    private static Regex GlobToRegex(string pattern)
    {
        var builder = new StringBuilder("^");
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

        builder.Append("$");
        return new Regex(builder.ToString(), RegexOptions.CultureInvariant);
    }

    private static bool TryEvalPrintfFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
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

        result = FormatPrintf(format, args);
        return true;
    }

    internal static string FormatPrintf(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            var text = token switch
            {
                'd' or 'i' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                'f' => IsNullish(value) ? "0" : Convert.ToDouble(value, CultureInfo.InvariantCulture).ToString(CultureInfo.InvariantCulture),
                's' => value?.ToString() ?? string.Empty,
                'x' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("x", CultureInfo.InvariantCulture),
                'X' => IsNullish(value) ? "0" : Convert.ToInt64(value, CultureInfo.InvariantCulture).ToString("X", CultureInfo.InvariantCulture),
                _ => value?.ToString() ?? string.Empty
            };

            builder.Append(text);
        }

        return builder.ToString();
    }

    private static string FormatPostgreSql(string format, IReadOnlyList<object?> args)
    {
        var builder = new StringBuilder();
        var argIndex = 0;
        for (var i = 0; i < format.Length; i++)
        {
            var ch = format[i];
            if (ch != '%' || i + 1 >= format.Length)
            {
                builder.Append(ch);
                continue;
            }

            var token = format[++i];
            if (token == '%')
            {
                builder.Append('%');
                continue;
            }

            var value = argIndex < args.Count ? args[argIndex++] : null;
            builder.Append(token switch
            {
                's' => value?.ToString() ?? string.Empty,
                'I' => QuoteFormatIdentifier(value),
                'L' => QuoteFormatLiteral(value),
                _ => value?.ToString() ?? string.Empty
            });
        }

        return builder.ToString();
    }

    private static string QuoteFormatIdentifier(object? value)
    {
        var text = value?.ToString() ?? string.Empty;
        return $"\"{text.Replace("\"", "\"\"")}\"";
    }

    private static string QuoteFormatLiteral(object? value)
    {
        if (value is null || value is DBNull)
            return SqlConst.NULL;

        var text = value.ToString() ?? string.Empty;
        return $"'{text.Replace("'", "''")}'";
    }

    private static bool TryEvalRandomFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        _ = evalArg;
        result = AstQueryRuntimeHelper.NextRandomInt64();
        return true;
    }

    private static bool TryEvalRandomBlobFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalRandomBlobLikeFunction(fn, context, evalArg, randomize: true, out result);
    }

    private static bool TryEvalZeroBlobFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalRandomBlobLikeFunction(fn, context, evalArg, randomize: false, out result);
    }

    private static bool TryEvalRandomBlobLikeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        bool randomize,
        out object? result)
    {
        _ = context;

        var lengthValue = evalArg(0);
        if (IsNullish(lengthValue))
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
            lock (_randomLock)
                _sharedRandom.NextBytes(buffer);
        }

        result = buffer;
        return true;
    }

    private static bool TryEvalTypeofFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (IsNullish(value))
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

    private static bool TryEvalUnicodeFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalUnicodeFunctions(fn, context, evalArg, out result);

    private static bool TryEvalUnicodeFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (IsNullish(value))
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

    private static bool TryEvalUnistrFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        result = UnescapeUnicodeLiteral(value?.ToString() ?? string.Empty);
        return true;
    }

    private static bool TryEvalUnistrQuoteFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        _ = context;
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = $"'{text.Replace("'", "''")}'";
        return true;
    }

    private static bool TryEvalLikelyFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(fn, context, evalArg, out result);

    private static bool TryEvalUnlikelyFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(fn, context, evalArg, out result);

    private static bool TryEvalLikelihoodFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
        => TryEvalLikelihoodFunctions(fn, context, evalArg, out result);

    private static bool TryEvalLikelihoodFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
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

    internal static bool TryParsePostgresInetValue(
        object? value,
        out IPAddress address,
        out int prefixLength)
    {
        address = IPAddress.None;
        prefixLength = 0;

        var text = value?.ToString()?.Trim() ?? string.Empty;
        if (string.IsNullOrEmpty(text))
            return false;

        var slashIndex = text.IndexOf('/');
        var addressText = slashIndex >= 0 ? text[..slashIndex] : text;
        if (!IPAddress.TryParse(addressText, out var parsedAddress))
            return false;

        address = parsedAddress;

        var maxPrefix = address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork ? 32 : 128;
        if (slashIndex < 0)
        {
            prefixLength = maxPrefix;
            return true;
        }

        var prefixText = text[(slashIndex + 1)..];
        if (!int.TryParse(prefixText, NumberStyles.Integer, CultureInfo.InvariantCulture, out prefixLength))
            return false;

        return prefixLength >= 0 && prefixLength <= maxPrefix;
    }

    internal static byte[] BuildPrefixMaskBytes(int byteLength, int prefixLength)
    {
        var mask = new byte[byteLength];
        for (var i = 0; i < byteLength; i++)
        {
            var remainingBits = prefixLength - (i * 8);
            mask[i] = remainingBits switch
            {
                >= 8 => 0xFF,
                <= 0 => 0x00,
                _ => (byte)(0xFF << (8 - remainingBits))
            };
        }

        return mask;
    }

    internal static byte[] ApplyNetworkMask(byte[] addressBytes, byte[] maskBytes)
    {
        var networkBytes = new byte[addressBytes.Length];
        for (var i = 0; i < addressBytes.Length; i++)
            networkBytes[i] = (byte)(addressBytes[i] & maskBytes[i]);

        return networkBytes;
    }

    internal static long ComputeGreatestCommonDivisor(long left, long right)
    {
        while (right != 0)
        {
            var remainder = left % right;
            left = right;
            right = remainder;
        }

        return Math.Abs(left);
    }

    internal static int GetMinimumNumericScale(object value)
    {
        var text = value switch
        {
            decimal dec => dec.ToString(CultureInfo.InvariantCulture),
            double dbl => dbl.ToString("G17", CultureInfo.InvariantCulture),
            float flt => flt.ToString("G9", CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty
        };

        var exponentIndex = text.IndexOfAny(['e', 'E']);
        if (exponentIndex >= 0)
        {
            if (decimal.TryParse(text, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsedDecimal))
                text = parsedDecimal.ToString(CultureInfo.InvariantCulture);
            else
                text = text[..exponentIndex];
        }

        var dotIndex = text.IndexOf('.');
        if (dotIndex < 0)
            return 0;

        var fractional = text[(dotIndex + 1)..].TrimEnd('0');
        return fractional.Length;
    }

    internal static bool TryParsePostgresIdentifierParts(string text, out List<string> parts)
    {
        parts = [];
        if (string.IsNullOrWhiteSpace(text))
            return false;

        var current = new StringBuilder();
        var insideQuotes = false;
        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (insideQuotes)
            {
                if (ch == '"')
                {
                    if (i + 1 < text.Length && text[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                        continue;
                    }

                    insideQuotes = false;
                    continue;
                }

                current.Append(ch);
                continue;
            }

            if (ch == '"')
            {
                insideQuotes = true;
                continue;
            }

            if (ch == '.')
            {
                parts.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        if (insideQuotes)
            return false;

        parts.Add(current.ToString().Trim());
        return parts.Count > 0 && parts.All(static part => part.Length > 0);
    }

    internal static string ConvertToAscii(string text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        var decomposed = text.Normalize(NormalizationForm.FormD);
        var builder = new StringBuilder(decomposed.Length);
        foreach (var ch in decomposed)
        {
            var category = CharUnicodeInfo.GetUnicodeCategory(ch);
            if (category is UnicodeCategory.NonSpacingMark
                or UnicodeCategory.SpacingCombiningMark
                or UnicodeCategory.EnclosingMark)
            {
                continue;
            }

            if (ch <= 0x7F)
                builder.Append(ch);
        }

        return builder.ToString().Normalize(NormalizationForm.FormC);
    }

    internal bool TryEvalSqliteSystemFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
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
            result = 0;
            return true;
        }

        if (name is "SQLITE3_CHANGES64" or "SQLITE3_TOTAL_CHANGES64" or "TOTAL_CHANGES")
        {
            result = _cnn.GetLastFoundRows();
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
            result = _cnn.GetLastInsertId() ?? 0;
            return true;
        }

        if (name is "LAST_INSERT_ROWID")
        {
            result = _cnn.GetLastInsertId() ?? 0;
            return true;
        }

        if (name is "SQLITE3_CREATE_FUNCTION"
            or "SQLITE3_CREATE_WINDOW_FUNCTION"
            or "SQLITE3_STEP"
            or "SQLITE3_RESULT_ZEROBLOB")
        {
            result = 0;
            return true;
        }

        if (name is "SQLITE3_MPRINTF")
        {
            result = FormatPrintf(evalArg(0)?.ToString() ?? string.Empty, [.. Enumerable.Range(1, Math.Max(0, fn.Args.Count - 1)).Select(evalArg)]);
            return true;
        }

        if (name is "XFINAL" or "XINVERSE" or "XSTEP" or "XVALUE")
        {
            result = null;
            return true;
        }

        result = null;
        return false;
    }

    internal bool TryEvalSqliteJsonFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (string.Equals(fn.Name, "JSON", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.GetRawText();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_ARRAY_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind == JsonValueKind.Array
                ? element.GetArrayLength()
                : 0;
            return true;
        }

        if (string.Equals(fn.Name, "JSON_ERROR_POSITION", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString();
            if (string.IsNullOrWhiteSpace(text))
            {
                result = 1;
                return true;
            }

            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text!, out _);
                result = 0;
            }
            catch
            {
                result = 1;
            }

            return true;
        }

        if (string.Equals(fn.Name, "JSON_PATCH", StringComparison.OrdinalIgnoreCase))
        {
            var baseValue = evalArg(0);
            var patchValue = evalArg(1);
            if (IsNullish(baseValue) || IsNullish(patchValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(baseValue!, out var baseNode) || baseNode is null)
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(patchValue!, out var patchNode) || patchNode is null)
            {
                result = null;
                return true;
            }

            ApplyJsonMergePatch(ref baseNode, patchNode);
            result = baseNode.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_MERGE_PATCH() espera dois JSONs.");

            var baseValue = evalArg(0);
            if (IsNullish(baseValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(baseValue!, out var baseNode) || baseNode is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var patchValue = evalArg(i);
                if (IsNullish(patchValue))
                {
                    result = null;
                    return true;
                }

                if (!TryParseJsonNode(patchValue!, out var patchNode) || patchNode is null)
                {
                    result = null;
                    return true;
                }

                ApplyJsonMergePatch(ref baseNode, patchNode);
            }

            result = baseNode.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_MERGE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 2)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera dois JSONs.");

            var firstValue = evalArg(0);
            if (IsNullish(firstValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonNode(firstValue!, out var merged) || merged is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var nextValue = evalArg(i);
                if (IsNullish(nextValue))
                {
                    result = null;
                    return true;
                }

                if (!TryParseJsonNode(nextValue!, out var nextNode) || nextNode is null)
                {
                    result = null;
                    return true;
                }

                merged = MergeJsonPreserve(merged, nextNode);
            }

            result = merged.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_APPEND", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_ARRAY_APPEND", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                if (!TryAppendJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_ARRAY_INSERT", StringComparison.OrdinalIgnoreCase))
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException("JSON_ARRAY_INSERT() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                if (!TryInsertJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_EACH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_TREE", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSONB_EACH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSONB_TREE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
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
            result = TryEvalJsonExtractionFunction(shim, context, evalArg, out var jsonExtractResult)
                ? jsonExtractResult
                : null;
            return true;
        }

        result = null;
        return false;
    }

    private static void ApplyJsonMergePatch(ref System.Text.Json.Nodes.JsonNode baseNode, System.Text.Json.Nodes.JsonNode patchNode)
    {
        if (patchNode is System.Text.Json.Nodes.JsonObject patchObject
            && baseNode is System.Text.Json.Nodes.JsonObject baseObject)
        {
            foreach (var pair in patchObject)
            {
                if (pair.Value is null)
                {
                    baseObject.Remove(pair.Key);
                    continue;
                }

                if (pair.Value is System.Text.Json.Nodes.JsonObject patchChild
                    && baseObject[pair.Key] is System.Text.Json.Nodes.JsonObject baseChild)
                {
                    var child = (System.Text.Json.Nodes.JsonNode)baseChild;
                    ApplyJsonMergePatch(ref child, patchChild);
                    baseObject[pair.Key] = child;
                    continue;
                }

                baseObject[pair.Key] = CloneJsonNode(pair.Value!);
            }

            return;
        }

        baseNode = CloneJsonNode(patchNode);
    }

    internal static void StripJsonNullProperties(System.Text.Json.Nodes.JsonNode node)
    {
        if (node is System.Text.Json.Nodes.JsonObject obj)
        {
            var propertyNames = obj.Select(static pair => pair.Key).ToList();
            foreach (var propertyName in propertyNames)
            {
                var child = obj[propertyName];
                if (child is null)
                {
                    obj.Remove(propertyName);
                    continue;
                }

                StripJsonNullProperties(child);
            }

            return;
        }

        if (node is System.Text.Json.Nodes.JsonArray array)
        {
            foreach (var child in array)
            {
                if (child is not null)
                    StripJsonNullProperties(child);
            }
        }
    }

    internal static System.Text.Json.Nodes.JsonNode CloneJsonNode(System.Text.Json.Nodes.JsonNode node)
        => System.Text.Json.Nodes.JsonNode.Parse(node.ToJsonString())!;

    private static bool TryEvalPadFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "LPAD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        var lenValue = evalArg(1);
        var padValue = fn.Args.Count > 2 ? evalArg(2) : " ";

        if (IsNullish(value) || IsNullish(lenValue) || IsNullish(padValue))
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

    private static bool TryEvalMd5Function(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MD5", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var bytes = Encoding.UTF8.GetBytes(text);
        using var md5 = MD5.Create();
        var hash = ComputeHash(md5, bytes);
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2", CultureInfo.InvariantCulture));

        result = sb.ToString();
        return true;
    }

    internal static bool TryEvalToNumberFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TO_NUMBER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte or sbyte or short or ushort or int or uint or long or ulong)
        {
            result = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            return true;
        }

        if (value is decimal or double or float)
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }

        var text = value?.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            result = null;
            return true;
        }

        if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integerValue))
        {
            result = integerValue;
            return true;
        }

        if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var decimalValue))
        {
            result = decimalValue;
            return true;
        }

        result = null;
        return true;
    }

    internal static bool TryEvalNumericFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context.Dialect;
        var name = fn.Name;
        var value = evalArg(0);

        if (name.Equals("ABS", StringComparison.OrdinalIgnoreCase)
            || name.Equals("ABSVAL", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                if (value is decimal dec)
                {
                    result = Math.Abs(dec);
                    return true;
                }

                if (value is float or double)
                {
                    result = Math.Abs(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                    return true;
                }

                result = Math.Abs(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name.Equals("BIN", StringComparison.OrdinalIgnoreCase))
        {
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
                result = Convert.ToString(number, 2);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        result = null;
        return false;
    }

    internal static bool TryEvalJsonUtilityFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (string.Equals(fn.Name, "JSON_VALID", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString();
            if (string.IsNullOrWhiteSpace(text))
            {
                result = 0;
                return true;
            }

            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text!, out _);
                result = 1;
            }
            catch
            {
                result = 0;
            }

            return true;
        }

        if (string.Equals(fn.Name, "JSON_TYPE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Object => "OBJECT",
                JsonValueKind.Array => "ARRAY",
                JsonValueKind.String => "STRING",
                JsonValueKind.Number => element.TryGetInt64(out _)
                    ? "INTEGER"
                    : "DOUBLE",
                JsonValueKind.True => "BOOLEAN",
                JsonValueKind.False => "BOOLEAN",
                JsonValueKind.Null => SqlConst.NULL,
                _ => null
            };
            return true;
        }

        if (string.Equals(fn.Name, "JSON_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value, out var element))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 1)
            {
                var path = evalArg(1)?.ToString();
                if (!string.IsNullOrWhiteSpace(path)
                    && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
                {
                    element = pathElement;
                }
                else if (!string.IsNullOrWhiteSpace(path))
                {
                    result = null;
                    return true;
                }
            }

            result = element.ValueKind switch
            {
                JsonValueKind.Array => element.GetArrayLength(),
                JsonValueKind.Object => element.EnumerateObject().Count(),
                _ => 1
            };
            return true;
        }

        if (string.Equals(fn.Name, "JSON_STORAGE_SIZE", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count == 0)
                throw new InvalidOperationException("JSON_STORAGE_SIZE() espera um JSON.");

            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var raw = element.GetRawText();
            result = (long)Encoding.UTF8.GetByteCount(raw);
            return true;
        }

        if (string.Equals(fn.Name, "JSON_OVERLAPS", StringComparison.OrdinalIgnoreCase))
        {
            if (!dialect.TryGetScalarFunctionDefinition(fn.Name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_OVERLAPS() espera dois JSONs.");

            var leftValue = evalArg(0);
            var rightValue = evalArg(1);
            if (IsNullish(leftValue) || IsNullish(rightValue))
            {
                result = null;
                return true;
            }

            if (!TryParseJsonElement(leftValue!, out var leftElement)
                || !TryParseJsonElement(rightValue!, out var rightElement))
            {
                result = null;
                return true;
            }

            result = JsonOverlaps(leftElement, rightElement) ? 1 : 0;
            return true;
        }

        if (string.Equals(fn.Name, "JSON_OBJECT", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count % 2 != 0)
                throw new InvalidOperationException("JSON_OBJECT() espera um número par de argumentos.");

            var pairs = new List<(string Key, object? Value)>();
            for (var i = 0; i < fn.Args.Count; i += 2)
            {
                var key = evalArg(i)?.ToString() ?? string.Empty;
                var val = evalArg(i + 1);
                pairs.Add((key, val));
            }

            result = BuildJsonObject(pairs);
            return true;
        }

        if (string.Equals(fn.Name, "JSON_QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = JsonSerializer.Serialize(text);
            return true;
        }

        if (string.Equals(fn.Name, "JSON_PRETTY", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            var options = new JsonSerializerOptions
            {
                WriteIndented = true
            };
            result = JsonSerializer.Serialize(element, options)
                .Replace("\r\n", "\\n");
            return true;
        }

        if (string.Equals(fn.Name, "JSON_KEYS", StringComparison.OrdinalIgnoreCase))
        {
            var value = evalArg(0);
            if (IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is null || !TryParseJsonElement(value!, out var element))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 1)
            {
                var path = evalArg(1)?.ToString();
                if (!string.IsNullOrWhiteSpace(path)
                    && QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var pathElement))
                {
                    element = pathElement;
                }
                else if (!string.IsNullOrWhiteSpace(path))
                {
                    result = null;
                    return true;
                }
            }

            if (element.ValueKind != JsonValueKind.Object)
            {
                result = null;
                return true;
            }

            var keys = element.EnumerateObject().Select(static prop => (object?)prop.Name).ToArray();
            result = BuildJsonArray(keys);
            return true;
        }

        if (string.Equals(fn.Name, "JSON_SET", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException("JSON_SET() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                if (!TrySetJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_REMOVE", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_REMOVE() espera um JSON e ao menos um path.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i++)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                TryRemoveJsonPathValue(root, tokens);
            }

            result = root.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_INSERT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_REPLACE", StringComparison.OrdinalIgnoreCase))
        {
            var isInsert = string.Equals(fn.Name, "JSON_INSERT", StringComparison.OrdinalIgnoreCase);
            if (fn.Args.Count < 3 || fn.Args.Count % 2 == 0)
                throw new InvalidOperationException($"{fn.Name.ToUpperInvariant()}() espera um JSON seguido de pares path/valor.");

            var json = evalArg(0);
            if (IsNullish(json) || !TryParseJsonNode(json!, out var root) || root is null)
            {
                result = null;
                return true;
            }

            for (var i = 1; i < fn.Args.Count; i += 2)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path) || !TryParseJsonPathTokens(path!, out var tokens))
                {
                    result = null;
                    return true;
                }

                var value = evalArg(i + 1);
                var exists = TryGetJsonNodeAtPath(root, tokens, out _);
                if (isInsert && exists)
                    continue;

                if (!isInsert && !exists)
                    continue;

                if (!TrySetJsonPathValue(ref root, tokens, value))
                {
                    result = null;
                    return true;
                }
            }

            result = root.ToJsonString();
            return true;
        }

        if (string.Equals(fn.Name, "JSON_CONTAINS", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("JSON_CONTAINS() espera um JSON e um candidato.");

            var targetValue = evalArg(0);
            var candidateValue = evalArg(1);
            if (IsNullish(targetValue) || IsNullish(candidateValue))
            {
                result = null;
                return true;
            }

            if (targetValue is null || candidateValue is null
                || !TryParseJsonElement(targetValue, out var targetElement)
                || !TryParseJsonCandidate(candidateValue, out var candidateElement))
            {
                result = null;
                return true;
            }

            if (fn.Args.Count > 2)
            {
                var path = evalArg(2)?.ToString();
                if (string.IsNullOrWhiteSpace(path)
                    || !QueryJsonFunctionHelper.TryReadJsonPathElement(targetElement, path!, out var pathElement))
                {
                    result = 0;
                    return true;
                }

                result = JsonContains(pathElement, candidateElement) ? 1 : 0;
                return true;
            }

            result = JsonContains(targetElement, candidateElement) ? 1 : 0;
            return true;
        }

        if (string.Equals(fn.Name, "JSON_CONTAINS_PATH", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSON_CONTAINS_PATH() espera um JSON, modo e paths.");

            var json = evalArg(0);
            var mode = evalArg(1)?.ToString() ?? string.Empty;
            if (IsNullish(json))
            {
                result = null;
                return true;
            }

            if (json is null || !TryParseJsonElement(json, out var element))
            {
                result = null;
                return true;
            }

            var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
            var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
            if (!requireAll && !requireOne)
            {
                result = null;
                return true;
            }

            var anyFound = false;
            for (var i = 2; i < fn.Args.Count; i++)
            {
                var path = evalArg(i)?.ToString();
                if (string.IsNullOrWhiteSpace(path))
                {
                    if (requireAll)
                    {
                        result = 0;
                        return true;
                    }

                    continue;
                }

                var found = QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out _);
                if (found)
                    anyFound = true;

                if (requireAll && !found)
                {
                    result = 0;
                    return true;
                }

                if (requireOne && found)
                {
                    result = 1;
                    return true;
                }
            }

            result = requireAll ? 1 : (anyFound ? 1 : 0);
            return true;
        }

        if (string.Equals(fn.Name, "JSON_SEARCH", StringComparison.OrdinalIgnoreCase))
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("JSON_SEARCH() espera JSON, modo e termo.");

            var json = evalArg(0);
            var mode = evalArg(1)?.ToString() ?? string.Empty;
            var search = evalArg(2)?.ToString() ?? string.Empty;
            if (IsNullish(json) || string.IsNullOrWhiteSpace(search))
            {
                result = null;
                return true;
            }

            if (json is null || !TryParseJsonElement(json, out var element))
            {
                result = null;
                return true;
            }

            var requireAll = mode.Equals("all", StringComparison.OrdinalIgnoreCase);
            var requireOne = mode.Equals("one", StringComparison.OrdinalIgnoreCase);
            if (!requireAll && !requireOne)
            {
                result = null;
                return true;
            }

            var pathStart = 3;
            if (fn.Args.Count > 4)
            {
                var escapeCandidate = evalArg(3)?.ToString();
                if (!string.IsNullOrEmpty(escapeCandidate)
                    && escapeCandidate!.Length == 1)
                    pathStart = 4;
            }

            var results = new List<string>();
            if (fn.Args.Count > pathStart)
            {
                for (var i = pathStart; i < fn.Args.Count; i++)
                {
                    var path = evalArg(i)?.ToString();
                    if (string.IsNullOrWhiteSpace(path))
                        continue;

                    if (QueryJsonFunctionHelper.TryReadJsonPathElement(element, path!, out var scoped))
                        CollectJsonSearchMatches(scoped, path!, search, results);
                }
            }
            else
            {
                CollectJsonSearchMatches(element, "$", search, results);
            }

            if (results.Count == 0)
            {
                result = null;
                return true;
            }

            result = requireOne ? results[0] : BuildJsonArray(results.Cast<object?>());
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalJsonAccessShimFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "__JSON_ACCESS_JSON", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        if (!AstQueryExecutionRuntimeHelper.TryGetJsonAndPathArguments(evalArg, out var json, out var path))
        {
            result = null;
            return true;
        }

        var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
        result = string.Equals(fn.Name, "__JSON_ACCESS_TEXT", StringComparison.OrdinalIgnoreCase)
            ? value?.ToString()
            : value;
        return true;
    }

    internal static bool TryEvalJsonExtractionFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!(string.Equals(fn.Name, "JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        EnsureJsonExtractionSupported(fn.Name, dialect);
        var json = evalArg(0);
        if (IsNullish(json))
        {
            result = null;
            return true;
        }

        if (string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && fn.Args.Count == 1)
        {
            result = TryEvalJsonQueryWithoutPath(json!);
            return true;
        }

        var path = evalArg(1)?.ToString();
        if (string.IsNullOrWhiteSpace(path))
        {
            result = null;
            return true;
        }

        result = TryEvalJsonExtractionValue(fn, json!, path!);
        return true;
    }

    internal static void EnsureJsonExtractionSupported(string functionName, ISqlDialect dialect)
    {
        if (dialect.TryGetScalarFunctionDefinition(functionName, out var definition))
        {
            if (definition is null || definition.AllowsCall)
                return;

            throw SqlUnsupported.ForDialect(dialect, functionName.ToUpperInvariant());
        }

        if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_EXTRACT", out var jsonExtractDefinition)
                || jsonExtractDefinition is null
                || !jsonExtractDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_EXTRACT");

        if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_QUERY", out var jsonQueryDefinition)
                || jsonQueryDefinition is null
                || !jsonQueryDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_QUERY");

        if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
            && (!dialect.TryGetScalarFunctionDefinition("JSON_VALUE", out var jsonValueDefinition)
                || jsonValueDefinition is null
                || !jsonValueDefinition.AllowsCall))
            throw SqlUnsupported.ForDialect(dialect, "JSON_VALUE");
    }

    internal static object? TryEvalJsonExtractionValue(FunctionCallExpr fn, object json, string path)
    {
        try
        {
            if (string.Equals(fn.Name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                if (!QueryJsonFunctionHelper.TryReadJsonPathElement(json, path, out var element))
                    return null;

                return element.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? element.GetRawText()
                    : null;
            }

            var value = QueryJsonFunctionHelper.TryReadJsonPathValue(json, path);
            return string.Equals(fn.Name, "JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                ? QueryJsonFunctionHelper.ApplyJsonValueReturningClause(fn, value)
                : value;
        }
#pragma warning disable CA1031
        catch (Exception e)
        {
            AstQueryExecutionRuntimeHelper.LogFunctionEvaluationFailure(e);
            return null;
        }
#pragma warning restore CA1031
    }

    internal static object? TryEvalJsonQueryWithoutPath(object json)
    {
        if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
            return null;

        return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
            ? root.GetRawText()
            : null;
    }

    internal static bool TryEvalOpenJsonFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!string.Equals(fn.Name, SqlConst.OPENJSON, StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.TryGetTableFunctionDefinition(SqlConst.OPENJSON, out var openJsonDefinition)
            || openJsonDefinition is null)
            throw SqlUnsupported.ForDialect(dialect, SqlConst.OPENJSON);

        var json = evalArg(0);
        result = IsNullish(json) ? null : json?.ToString();
        return true;
    }

    internal static bool TryEvalJsonUnquoteFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "JSON_UNQUOTE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value!.ToString() ?? string.Empty;
        result = text.Length >= 2 && ((text[0] == '"' && text[^1] == '"') || (text[0] == '\'' && text[^1] == '\''))
            ? text[1..^1]
            : text;
        return true;
    }

    internal static bool TryEvalSqlServerJsonModifyFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var dialect = context.Dialect;
        if (!string.Equals(fn.Name, "JSON_MODIFY", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var definition = fn.ResolvedScalarFunction;
        if (definition is not null
            && !definition.AllowsCall)
        {
            throw SqlUnsupported.ForDialect(dialect, "JSON_MODIFY");
        }

        if (definition is null)
            throw SqlUnsupported.ForDialect(dialect, "JSON_MODIFY");

        if (fn.Args.Count < 3)
            throw new InvalidOperationException("JSON_MODIFY() espera JSON, path e novo valor.");

        var json = evalArg(0);
        var pathValue = evalArg(1)?.ToString();
        var newValue = evalArg(2);
        if (IsNullish(json) || string.IsNullOrWhiteSpace(pathValue) || !TryParseJsonNode(json!, out var root) || root is null)
        {
            result = null;
            return true;
        }

        if (!TryParseSqlServerJsonModifyPath(pathValue!, out var tokens, out var append, out var strict))
        {
            result = null;
            return true;
        }

        var exists = TryGetJsonNodeAtPath(root, tokens, out var existingNode);
        if (append)
        {
            if (!exists || existingNode is not System.Text.Json.Nodes.JsonArray array)
            {
                if (strict)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                result = root.ToJsonString();
                return true;
            }

            array.Add(CreateJsonNodeFromValue(newValue));
            result = root.ToJsonString();
            return true;
        }

        if (IsNullish(newValue))
        {
            if (strict)
            {
                if (!exists)
                    throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

                if (!TrySetJsonPathValue(ref root, tokens, null))
                {
                    result = null;
                    return true;
                }
            }
            else if (exists)
            {
                TryRemoveJsonPathValue(root, tokens);
            }

            result = root.ToJsonString();
            return true;
        }

        if (strict && !exists)
            throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

        if (!TrySetJsonPathValue(ref root, tokens, newValue))
        {
            if (strict)
                throw new InvalidOperationException($"JSON_MODIFY strict path '{pathValue}' was not found in the JSON payload.");

            result = root.ToJsonString();
            return true;
        }

        result = root.ToJsonString();
        return true;
    }

    internal static bool TryParseJsonElement(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
            return true;
        }
        catch
        {
            element = default;
            return false;
        }
    }

    internal static bool TryParseJsonCandidate(object value, out JsonElement element)
    {
        if (value is JsonElement jsonElement)
        {
            element = jsonElement;
            return true;
        }

        var text = value.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            element = default;
            return false;
        }

        if (text.TrimStart().StartsWith("{", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("[", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("\"", StringComparison.Ordinal)
            || text.TrimStart().StartsWith("true", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("false", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("null", StringComparison.OrdinalIgnoreCase)
            || text.TrimStart().StartsWith("-", StringComparison.Ordinal)
            || char.IsDigit(text.TrimStart()[0]))
        {
            try
            {
                QueryJsonFunctionHelper.TryGetJsonRootElement(text, out element);
                return true;
            }
            catch
            {
                // fallthrough to treat as string
            }
        }

        element = JsonSerializer.SerializeToElement(text);
        return true;
    }

    internal static bool JsonContains(JsonElement target, JsonElement candidate)
    {
        if (candidate.ValueKind == JsonValueKind.Object)
        {
            if (target.ValueKind != JsonValueKind.Object)
                return false;

            foreach (var prop in candidate.EnumerateObject())
            {
                if (!target.TryGetProperty(prop.Name, out var targetProp))
                    return false;

                if (!JsonContains(targetProp, prop.Value))
                    return false;
            }

            return true;
        }

        if (candidate.ValueKind == JsonValueKind.Array)
        {
            if (target.ValueKind != JsonValueKind.Array)
                return false;

            var targetItems = target.EnumerateArray().ToArray();
            foreach (var candidateItem in candidate.EnumerateArray())
            {
                if (!targetItems.Any(item => JsonContains(item, candidateItem)))
                    return false;
            }

            return true;
        }

        return JsonElementEquals(target, candidate);
    }

    private static bool JsonElementEquals(JsonElement left, JsonElement right)
    {
        if (left.ValueKind != right.ValueKind)
        {
            if (left.ValueKind == JsonValueKind.Number
                && right.ValueKind == JsonValueKind.Number)
            {
                if (left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec))
                    return ldec == rdec;
                return left.GetDouble().Equals(right.GetDouble());
            }

            return false;
        }

        return left.ValueKind switch
        {
            JsonValueKind.String => left.GetString() == right.GetString(),
            JsonValueKind.Number => left.TryGetDecimal(out var ldec) && right.TryGetDecimal(out var rdec)
                ? ldec == rdec
                : left.GetDouble().Equals(right.GetDouble()),
            JsonValueKind.True => right.ValueKind == JsonValueKind.True,
            JsonValueKind.False => right.ValueKind == JsonValueKind.False,
            JsonValueKind.Null => true,
            _ => left.GetRawText() == right.GetRawText()
        };
    }

    internal static bool JsonOverlaps(JsonElement left, JsonElement right)
    {
        if (left.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in left.EnumerateArray())
            {
                if (JsonOverlaps(item, right))
                    return true;
            }

            return false;
        }

        if (right.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in right.EnumerateArray())
            {
                if (JsonOverlaps(left, item))
                    return true;
            }

            return false;
        }

        if (left.ValueKind == JsonValueKind.Object
            && right.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in left.EnumerateObject())
            {
                if (right.TryGetProperty(prop.Name, out var rightProp)
                    && JsonOverlaps(prop.Value, rightProp))
                {
                    return true;
                }
            }

            return false;
        }

        if (left.ValueKind == JsonValueKind.Object
            || right.ValueKind == JsonValueKind.Object)
            return false;

        return JsonElementEquals(left, right);
    }

    private static System.Text.Json.Nodes.JsonNode MergeJsonPreserve(
        System.Text.Json.Nodes.JsonNode left,
        System.Text.Json.Nodes.JsonNode right)
    {
        if (left is System.Text.Json.Nodes.JsonObject leftObj
            && right is System.Text.Json.Nodes.JsonObject rightObj)
        {
            var merged = new System.Text.Json.Nodes.JsonObject();
            foreach (var pair in leftObj)
                merged[pair.Key] = pair.Value is null ? null : CloneJsonNode(pair.Value);

            foreach (var pair in rightObj)
            {
                if (merged.TryGetPropertyValue(pair.Key, out var existing) && existing is not null && pair.Value is not null)
                {
                    merged[pair.Key] = MergeJsonPreserve(existing, pair.Value);
                    continue;
                }

                merged[pair.Key] = pair.Value is null ? null : CloneJsonNode(pair.Value);
            }

            return merged;
        }

        if (left is System.Text.Json.Nodes.JsonArray leftArray
            && right is System.Text.Json.Nodes.JsonArray rightArray)
        {
            var merged = new System.Text.Json.Nodes.JsonArray();
            foreach (var item in leftArray)
                merged.Add(item is null ? null : CloneJsonNode(item));
            foreach (var item in rightArray)
                merged.Add(item is null ? null : CloneJsonNode(item));
            return merged;
        }

        return new System.Text.Json.Nodes.JsonArray
        {
            left is null ? null : CloneJsonNode(left),
            right is null ? null : CloneJsonNode(right)
        };
    }

    private static bool TryAppendJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
        {
            if (root is System.Text.Json.Nodes.JsonArray rootArray)
            {
                rootArray.Add(CreateJsonNodeFromValue(value));
                return true;
            }

            root = new System.Text.Json.Nodes.JsonArray
            {
                root is null ? null : CloneJsonNode(root),
                CreateJsonNodeFromValue(value)
            };
            return true;
        }

        if (!TryGetJsonNodeAtPath(root, tokens, out var node))
            return false;

        if (node is System.Text.Json.Nodes.JsonArray array)
        {
            array.Add(CreateJsonNodeFromValue(value));
            return true;
        }

        var newArray = new System.Text.Json.Nodes.JsonArray
        {
            node is null ? null : CloneJsonNode(node),
            CreateJsonNodeFromValue(value)
        };

        return TrySetJsonPathValue(ref root, tokens, newArray);
    }

    internal static bool TryInsertJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        var last = tokens[^1];
        if (last.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var parentTokens = tokens.Take(tokens.Count - 1).ToList();
            if (parentTokens.Count == 0)
            {
                if (root is not System.Text.Json.Nodes.JsonArray rootArray)
                    return false;

                var index = Math.Max(0, last.ArrayIndex ?? 0);
                var insertIndex = Math.Min(index, rootArray.Count);
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is not System.Text.Json.Nodes.JsonArray parentArray)
                return false;

            var parentIndex = Math.Max(0, last.ArrayIndex ?? 0);
            var targetIndex = Math.Min(parentIndex, parentArray.Count);
            parentArray.Insert(targetIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        if (!TryGetJsonNodeAtPath(root, tokens, out var node) || node is not System.Text.Json.Nodes.JsonArray array)
            return false;

        array.Add(CreateJsonNodeFromValue(value));
        return true;
    }

    internal static bool TryReadPostgresJsonPathElement(
        JsonElement element,
        string pathSegment,
        out JsonElement target)
    {
        target = default;
        if (string.IsNullOrEmpty(pathSegment))
            return false;

        if (element.ValueKind == JsonValueKind.Object)
        {
            if (element.TryGetProperty(pathSegment, out target))
                return true;

            return false;
        }

        if (element.ValueKind == JsonValueKind.Array
            && int.TryParse(pathSegment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
            && index >= 0)
        {
            var currentIndex = 0;
            foreach (var item in element.EnumerateArray())
            {
                if (currentIndex == index)
                {
                    target = item;
                    return true;
                }

                currentIndex++;
            }
        }

        return false;
    }

    internal static bool TryReadPostgresJsonPath(
        JsonElement element,
        string path,
        out JsonElement target)
    {
        target = default;
        if (!TryParseJsonPathTokens(path, out var tokens))
            return false;

        var current = element;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (!TryReadPostgresJsonPathElement(current, token.PropertyName ?? string.Empty, out current))
                    return false;

                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (!TryReadPostgresJsonPathElement(current, (token.ArrayIndex ?? 0).ToString(CultureInfo.InvariantCulture), out current))
                    return false;
            }
        }

        target = current;
        return true;
    }

    internal static void CollectJsonSearchMatches(
        JsonElement element,
        string currentPath,
        string search,
        List<string> results)
    {
        if (element.ValueKind == JsonValueKind.String)
        {
            var text = element.GetString() ?? string.Empty;
            if (text.IndexOf(search, StringComparison.Ordinal) >= 0)
                results.Add(currentPath);
            return;
        }

        if (element.ValueKind == JsonValueKind.Array)
        {
            var index = 0;
            foreach (var item in element.EnumerateArray())
            {
                CollectJsonSearchMatches(item, $"{currentPath}[{index}]", search, results);
                index++;
            }

            return;
        }

        if (element.ValueKind == JsonValueKind.Object)
        {
            foreach (var prop in element.EnumerateObject())
            {
                CollectJsonSearchMatches(prop.Value, $"{currentPath}.{prop.Name}", search, results);
            }
        }
    }

    internal static string BuildJsonArray(IEnumerable<object?> values)
    {
        var parts = values.Select(static value =>
        {
            if (value is null or DBNull)
                return "null";

            if (value is JsonElement element)
                return element.GetRawText();

            return JsonSerializer.Serialize(value);
        });

        return "[" + string.Join(",", parts) + "]";
    }

    internal static string BuildJsonObject(IEnumerable<(string Key, object? Value)> pairs)
    {
        var parts = pairs.Select(static pair =>
        {
            var key = JsonSerializer.Serialize(pair.Key ?? string.Empty);
            var value = pair.Value;
            if (value is null or DBNull)
                return $"{key}:null";

            if (value is JsonElement element)
                return $"{key}:{element.GetRawText()}";

            return $"{key}:{JsonSerializer.Serialize(value)}";
        });

        return "{" + string.Join(",", parts) + "}";
    }

    internal enum JsonPathTokenKind
    {
        Property,
        ArrayIndex
    }

    internal readonly record struct JsonPathToken(JsonPathTokenKind Kind, string? PropertyName, int? ArrayIndex);
    internal readonly record struct JsonPathTokenCacheEntry(bool Success, JsonPathToken[] Tokens);

    internal static bool TryParseJsonPathTokens(string path, out List<JsonPathToken> tokens)
    {
        if (_jsonPathTokenCache.TryGetValue(path, out var cached))
        {
            tokens = cached.Success ? [.. cached.Tokens] : [];
            return cached.Success;
        }

        var success = TryParseJsonPathTokensCore(path, out tokens);
        CacheJsonPathParseEntry(_jsonPathTokenCache, path, new JsonPathTokenCacheEntry(success, [.. tokens]));
        return success;
    }

    private static bool TryParseJsonPathTokensCore(string path, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (string.IsNullOrWhiteSpace(path))
            return false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("lax ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[4..].TrimStart();
        else if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            trimmed = trimmed[7..].TrimStart();

        if (trimmed.Length == 0 || trimmed[0] != '$')
            return false;

        var i = 1;
        while (i < trimmed.Length)
        {
            while (i < trimmed.Length && char.IsWhiteSpace(trimmed[i]))
                i++;

            if (i >= trimmed.Length)
                break;

            if (trimmed[i] == '.')
            {
                i++;
                var start = i;
                while (i < trimmed.Length && (char.IsLetterOrDigit(trimmed[i]) || trimmed[i] == '_'))
                    i++;

                if (i == start)
                    return false;

                var property = trimmed[start..i];
                tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                continue;
            }

            if (trimmed[i] == '[')
            {
                i++;
                if (i >= trimmed.Length)
                    return false;

                if (trimmed[i] is '"' or '\'')
                {
                    var quote = trimmed[i];
                    i++;
                    var start = i;
                    while (i < trimmed.Length && trimmed[i] != quote)
                        i++;

                    if (i >= trimmed.Length)
                        return false;

                    var property = trimmed[start..i];
                    i++; // closing quote
                    if (i >= trimmed.Length || trimmed[i] != ']')
                        return false;
                    i++; // closing bracket
                    tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, property, null));
                    continue;
                }

                var indexStart = i;
                while (i < trimmed.Length && char.IsDigit(trimmed[i]))
                    i++;

                if (i == indexStart || i >= trimmed.Length || trimmed[i] != ']')
                    return false;

                if (!int.TryParse(trimmed[indexStart..i], NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
                    return false;

                i++; // closing bracket
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            return false;
        }

        return true;
    }

    internal static bool TryParseSqlServerJsonModifyPath(
        string path,
        out List<JsonPathToken> tokens,
        out bool append,
        out bool strict)
    {
        tokens = [];
        append = false;
        strict = false;

        var trimmed = path.Trim();
        if (trimmed.StartsWith("append ", StringComparison.OrdinalIgnoreCase))
        {
            append = true;
            trimmed = trimmed[7..].TrimStart();
        }

        if (trimmed.StartsWith("strict ", StringComparison.OrdinalIgnoreCase))
            strict = true;

        return TryParseJsonPathTokens(trimmed, out tokens);
    }

    internal static bool TryParseJsonNode(object json, out System.Text.Json.Nodes.JsonNode? node)
    {
        if (json is System.Text.Json.Nodes.JsonNode jsonNode)
        {
            node = jsonNode;
            return true;
        }

        var text = json.ToString();
        if (string.IsNullOrWhiteSpace(text))
        {
            node = null;
            return false;
        }

        try
        {
            node = System.Text.Json.Nodes.JsonNode.Parse(text);
            return node is not null;
        }
        catch
        {
            node = null;
            return false;
        }
    }

    internal static bool TryReadPostgresTextArray(object? value, out List<string> items)
    {
        items = [];
        if (IsNullish(value))
            return false;

        if (value is IEnumerable enumerable && value is not string)
        {
            foreach (var item in enumerable)
                items.Add(item?.ToString() ?? string.Empty);

            return true;
        }

        return false;
    }

    internal static bool TryParsePostgresJsonPathTokens(object value, out List<JsonPathToken> tokens)
    {
        tokens = [];
        if (!TryReadPostgresTextArray(value, out var segments))
            return false;

        var cacheKey = BuildPostgresJsonPathCacheKey(segments);
        if (_postgresJsonPathTokenCache.TryGetValue(cacheKey, out var cached))
        {
            tokens = cached.Success ? [.. cached.Tokens] : [];
            return cached.Success;
        }

        foreach (var segment in segments)
        {
            if (int.TryParse(segment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index)
                && index >= 0)
            {
                tokens.Add(new JsonPathToken(JsonPathTokenKind.ArrayIndex, null, index));
                continue;
            }

            tokens.Add(new JsonPathToken(JsonPathTokenKind.Property, segment, null));
        }

        var success = tokens.Count > 0;
        CacheJsonPathParseEntry(_postgresJsonPathTokenCache, cacheKey, new JsonPathTokenCacheEntry(success, [.. tokens]));
        return success;
    }

    private static string BuildPostgresJsonPathCacheKey(IReadOnlyList<string> segments)
        => string.Join("\u001F", segments);

    private static void CacheJsonPathParseEntry(
        System.Collections.Concurrent.ConcurrentDictionary<string, JsonPathTokenCacheEntry> cache,
        string key,
        JsonPathTokenCacheEntry entry)
    {
        if (cache.Count >= JsonPathParseCacheSoftLimit)
            cache.Clear();

        cache[key] = entry;
    }

    internal static System.Text.Json.Nodes.JsonNode CreateJsonNodeFromValue(object? value)
    {
        if (value is null or DBNull)
        {
            return System.Text.Json.Nodes.JsonValue.Create((string?)null)
                ?? System.Text.Json.Nodes.JsonNode.Parse("null")!;
        }

        if (value is JsonElement element)
            return System.Text.Json.Nodes.JsonNode.Parse(element.GetRawText())!;

        if (value is System.Text.Json.Nodes.JsonNode node)
            return node;

        return System.Text.Json.Nodes.JsonValue.Create(value)
            ?? System.Text.Json.Nodes.JsonNode.Parse(JsonSerializer.Serialize(value))!;
    }

    private static System.Text.Json.Nodes.JsonNode CreateJsonContainer(JsonPathToken nextToken)
        => nextToken.Kind == JsonPathTokenKind.ArrayIndex
            ? new System.Text.Json.Nodes.JsonArray()
            : new System.Text.Json.Nodes.JsonObject();

    internal static bool TrySetJsonPathValue(
        ref System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value)
    {
        if (tokens.Count == 0)
            return false;

        System.Text.Json.Nodes.JsonNode? current = root;
        System.Text.Json.Nodes.JsonNode? parent = null;
        JsonPathToken? parentToken = null;

        for (var i = 0; i < tokens.Count; i++)
        {
            var token = tokens[i];
            var isLast = i == tokens.Count - 1;

            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not System.Text.Json.Nodes.JsonObject obj)
                {
                    if (current is null or System.Text.Json.Nodes.JsonValue)
                    {
                        obj = new System.Text.Json.Nodes.JsonObject();
                        AssignJsonChild(ref root, parent, parentToken, obj);
                    }
                    else
                    {
                        return false;
                    }
                }

                if (isLast)
                {
                    obj[token.PropertyName!] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = obj[token.PropertyName!];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    obj[token.PropertyName!] = child;
                }

                parent = obj;
                parentToken = token;
                current = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not System.Text.Json.Nodes.JsonArray array)
                {
                    if (current is null or System.Text.Json.Nodes.JsonValue)
                    {
                        array = new System.Text.Json.Nodes.JsonArray();
                        AssignJsonChild(ref root, parent, parentToken, array);
                    }
                    else
                    {
                        return false;
                    }
                }

                var index = token.ArrayIndex ?? 0;
                while (array.Count <= index)
                    array.Add(null);

                if (isLast)
                {
                    array[index] = CreateJsonNodeFromValue(value);
                    return true;
                }

                var child = array[index];
                if (child is null)
                {
                    child = CreateJsonContainer(tokens[i + 1]);
                    array[index] = child;
                }

                parent = array;
                parentToken = token;
                current = child;
            }
        }

        return false;
    }

    internal static bool TryGetJsonNodeAtPath(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        out System.Text.Json.Nodes.JsonNode? node)
    {
        node = root;
        foreach (var token in tokens)
        {
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (node is not System.Text.Json.Nodes.JsonObject obj)
                {
                    node = null;
                    return false;
                }

                if (!obj.TryGetPropertyValue(token.PropertyName!, out var child))
                {
                    node = null;
                    return false;
                }

                node = child;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (node is not System.Text.Json.Nodes.JsonArray array)
                {
                    node = null;
                    return false;
                }

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                {
                    node = null;
                    return false;
                }

                node = array[index];
            }
        }

        return node is not null;
    }

    private static void AssignJsonChild(
        ref System.Text.Json.Nodes.JsonNode root,
        System.Text.Json.Nodes.JsonNode? parent,
        JsonPathToken? parentToken,
        System.Text.Json.Nodes.JsonNode child)
    {
        if (parent is null)
        {
            root = child;
            return;
        }

        if (parent is System.Text.Json.Nodes.JsonObject obj && parentToken?.Kind == JsonPathTokenKind.Property)
        {
            obj[parentToken.Value.PropertyName!] = child;
            return;
        }

        if (parent is System.Text.Json.Nodes.JsonArray array && parentToken?.Kind == JsonPathTokenKind.ArrayIndex)
        {
            var index = parentToken.Value.ArrayIndex ?? 0;
            while (array.Count <= index)
                array.Add(null);
            array[index] = child;
        }
    }

    internal static bool TryRemoveJsonPathValue(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens)
    {
        if (tokens.Count == 0)
            return false;

        System.Text.Json.Nodes.JsonNode? current = root;
        for (var i = 0; i < tokens.Count - 1; i++)
        {
            var token = tokens[i];
            if (token.Kind == JsonPathTokenKind.Property)
            {
                if (current is not System.Text.Json.Nodes.JsonObject obj)
                    return true;

                current = obj[token.PropertyName!];
                if (current is null)
                    return true;
                continue;
            }

            if (token.Kind == JsonPathTokenKind.ArrayIndex)
            {
                if (current is not System.Text.Json.Nodes.JsonArray array)
                    return true;

                var index = token.ArrayIndex ?? 0;
                if (index < 0 || index >= array.Count)
                    return true;
                current = array[index];
                if (current is null)
                    return true;
            }
        }

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (current is not System.Text.Json.Nodes.JsonObject obj)
                return true;

            obj.Remove(lastToken.PropertyName!);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (current is not System.Text.Json.Nodes.JsonArray array)
                return true;

            var index = lastToken.ArrayIndex ?? 0;
            if (index < 0 || index >= array.Count)
                return true;

            array.RemoveAt(index);
            return true;
        }

        return true;
    }

    internal static bool TryInsertJsonPathValue(
        System.Text.Json.Nodes.JsonNode root,
        IReadOnlyList<JsonPathToken> tokens,
        object? value,
        bool insertAfter)
    {
        if (tokens.Count == 0)
            return false;

        if (tokens.Count == 1)
        {
            var targetToken = tokens[0];
            if (targetToken.Kind == JsonPathTokenKind.Property && root is System.Text.Json.Nodes.JsonObject rootObject)
            {
                if (rootObject[targetToken.PropertyName!] is not null)
                    return true;

                rootObject[targetToken.PropertyName!] = CreateJsonNodeFromValue(value);
                return true;
            }

            if (targetToken.Kind == JsonPathTokenKind.ArrayIndex && root is System.Text.Json.Nodes.JsonArray rootArray)
            {
                var insertIndex = targetToken.ArrayIndex ?? 0;
                if (insertAfter)
                    insertIndex++;

                insertIndex = Math.Max(0, Math.Min(insertIndex, rootArray.Count));
                rootArray.Insert(insertIndex, CreateJsonNodeFromValue(value));
                return true;
            }

            return false;
        }

        var parentTokens = tokens.Take(tokens.Count - 1).ToList();
        if (!TryGetJsonNodeAtPath(root, parentTokens, out var parent) || parent is null)
            return false;

        var lastToken = tokens[^1];
        if (lastToken.Kind == JsonPathTokenKind.Property)
        {
            if (parent is not System.Text.Json.Nodes.JsonObject obj)
                return false;

            if (obj[lastToken.PropertyName!] is not null)
                return true;

            obj[lastToken.PropertyName!] = CreateJsonNodeFromValue(value);
            return true;
        }

        if (lastToken.Kind == JsonPathTokenKind.ArrayIndex)
        {
            if (parent is not System.Text.Json.Nodes.JsonArray array)
                return false;

            var insertIndex = lastToken.ArrayIndex ?? 0;
            if (insertAfter)
                insertIndex++;

            insertIndex = Math.Max(0, Math.Min(insertIndex, array.Count));
            array.Insert(insertIndex, CreateJsonNodeFromValue(value));
            return true;
        }

        return false;
    }

    internal static bool TryEvalFieldFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = fn;
        var target = evalArg(0);
        if (IsNullish(target))
        {
            result = 0;
            return true;
        }

        for (int argIndex = 1; argIndex < fn.Args.Count; argIndex++)
        {
            var candidate = evalArg(argIndex);
            if (!IsNullish(candidate) && target.EqualsSql(candidate, context))
            {
                result = argIndex;
                return true;
            }
        }

        result = 0;
        return true;
    }

    internal static bool TryEvalCharFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (fn.Name.Equals("CHAR", StringComparison.OrdinalIgnoreCase)
            || fn.Name.Equals("NCHAR", StringComparison.OrdinalIgnoreCase))
        {
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
        }

        result = value!.ToString() ?? string.Empty;
        return true;
    }

    internal static bool TryEvalBasicStringFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var value = evalArg(0);

        if (string.Equals(fn.Name, "LOWER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "LCASE", StringComparison.OrdinalIgnoreCase))
        {
#pragma warning disable CA1308 // Normalize strings to uppercase
            result = IsNullish(value) ? null : value!.ToString()!.ToLowerInvariant();
#pragma warning restore CA1308 // Normalize strings to uppercase
            return true;
        }

        if (string.Equals(fn.Name, "UPPER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "UCASE", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.ToUpperInvariant();
            return true;
        }

        if (string.Equals(fn.Name, "TRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.Trim();
            return true;
        }

        if (string.Equals(fn.Name, "RTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.TrimEnd();
            return true;
        }

        if (string.Equals(fn.Name, "LTRIM", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString()!.TrimStart();
            return true;
        }

        if (string.Equals(fn.Name, "TO_CHAR", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : value!.ToString() ?? string.Empty;
            return true;
        }

        if (string.Equals(fn.Name, "LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CHAR_LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "CHARACTER_LENGTH", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "LEN", StringComparison.OrdinalIgnoreCase))
        {
            result = IsNullish(value) ? null : (long)(value!.ToString()!.Length);
            return true;
        }

        result = null;
        return false;
    }

    internal static bool TryEvalSubstringFunction(
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!(string.Equals(fn.Name, "SUBSTRING", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "SUBSTR", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "MID", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var source = evalArg(0);
        if (IsNullish(source))
        {
            result = null;
            return true;
        }

        var text = source!.ToString() ?? string.Empty;
        var position = evalArg(1);
        if (IsNullish(position))
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

        var lengthValue = evalArg(2);
        if (IsNullish(lengthValue))
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

    private static bool TryEvalModFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "MOD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("MOD() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (IsNullish(left) || IsNullish(right))
        {
            result = null;
            return true;
        }

        try
        {
            var l = Convert.ToDecimal(left, CultureInfo.InvariantCulture);
            var r = Convert.ToDecimal(right, CultureInfo.InvariantCulture);
            if (r == 0)
            {
                result = null;
                return true;
            }

            result = l % r;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOctFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "OCT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = Convert.ToString(number, 8);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitCountFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "BIT_COUNT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            var bits = unchecked((ulong)number);
            var count = 0;
            while (bits != 0)
            {
                count += (int)(bits & 1UL);
                bits >>= 1;
            }

            result = count;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalBitLengthFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "BIT_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
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

    private static bool TryEvalHexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "HEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (value is byte[] bytes)
        {
            result = BytesToHex(bytes);
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        result = BytesToHex(Encoding.UTF8.GetBytes(text));
        return true;
    }

    private static bool TryEvalUnhexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "UNHEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        try
        {
            if (text.Length % 2 != 0)
                text = "0" + text;

            var bytes = new byte[text.Length / 2];
            for (var i = 0; i < bytes.Length; i++)
            {
                bytes[i] = Convert.ToByte(text.Substring(i * 2, 2), 16);
            }

            result = bytes;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOctetLengthFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "OCTET_LENGTH", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
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

    private static bool TryEvalNameConstFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "NAME_CONST", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var nameValue = evalArg(0);
        var value = evalArg(1);
        if (IsNullish(nameValue))
        {
            result = null;
            return true;
        }

        result = value;
        return true;
    }

    private static bool TryEvalOrdFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "ORD", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (text.Length == 0)
        {
            result = 0;
            return true;
        }

        var bytes = Encoding.UTF8.GetBytes(text);
        result = bytes[0];
        return true;
    }

    private static bool TryEvalPositionFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "POSITION", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

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

    private static bool TryEvalPiFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "PI", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        result = Math.PI;
        return true;
    }

    private static bool TryEvalPowerFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var isPower = string.Equals(fn.Name, "POWER", StringComparison.OrdinalIgnoreCase)
            || string.Equals(fn.Name, "POW", StringComparison.OrdinalIgnoreCase);
        if (!isPower)
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("POWER() espera base e expoente.");

        var baseValue = evalArg(0);
        var expValue = evalArg(1);
        if (IsNullish(baseValue) || IsNullish(expValue))
        {
            result = null;
            return true;
        }

        try
        {
            var baseNumber = Convert.ToDouble(baseValue, CultureInfo.InvariantCulture);
            var expNumber = Convert.ToDouble(expValue, CultureInfo.InvariantCulture);
            result = Math.Pow(baseNumber, expNumber);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalQuoteFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "QUOTE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = SqlConst.NULL;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var escaped = text.Replace("\\", "\\\\").Replace("'", "\\'");
        result = $"'{escaped}'";
        return true;
    }

    private static bool TryEvalRadiansFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "RADIANS", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var degrees = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = degrees * (Math.PI / 180d);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalRandFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "RAND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var seedValue = fn.Args.Count > 0 ? evalArg(0) : null;
        double next;
        if (IsNullish(seedValue))
        {
            lock (_randomLock)
                next = _sharedRandom.NextDouble();
        }
        else
        {
            var seeded = new Random(Convert.ToInt32(seedValue.ToDec()));
            next = seeded.NextDouble();
        }

        result = next;
        return true;
    }

    private static bool TryEvalRepeatFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "REPEAT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var countValue = evalArg(1);
        if (IsNullish(textValue) || IsNullish(countValue))
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

    private static bool TryEvalReverseFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "REVERSE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
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
        QueryExecutionContext context,
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
        if (IsNullish(textValue) || IsNullish(lengthValue))
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
        QueryExecutionContext context,
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
        if (IsNullish(textValue) || IsNullish(lengthValue))
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

    private static bool TryEvalRoundFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "ROUND", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var decimals = fn.Args.Count > 1 ? evalArg(1) : null;
        try
        {
            var number = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            if (IsNullish(decimals))
            {
                result = Math.Round(number, 0, MidpointRounding.AwayFromZero);
                return true;
            }

            var digits = Convert.ToInt32(decimals.ToDec());
            result = Math.Round(number, digits, MidpointRounding.AwayFromZero);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalPadRightFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
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

        if (IsNullish(value) || IsNullish(lenValue) || IsNullish(padValue))
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

    private static bool TryEvalShaFunctions(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name;
        if (!(name.Equals("SHA", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SHA1", StringComparison.OrdinalIgnoreCase)
            || name.Equals("SHA2", StringComparison.OrdinalIgnoreCase)))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        var bytes = Encoding.UTF8.GetBytes(text);

        if (name.Equals("SHA2", StringComparison.OrdinalIgnoreCase))
        {
            var lengthArg = fn.Args.Count > 1 ? evalArg(1) : null;
            var length = IsNullish(lengthArg) ? 256 : Convert.ToInt32(lengthArg.ToDec());
            byte[] hash = length switch
            {
                224 => ComputeHash(SHA256.Create(), bytes),
                256 => ComputeHash(SHA256.Create(), bytes),
                384 => ComputeHash(SHA384.Create(), bytes),
                512 => ComputeHash(SHA512.Create(), bytes),
                _ => ComputeHash(SHA256.Create(), bytes)
            };

            result = BytesToHex(hash);
            return true;
        }

        using var sha1 = SHA1.Create();
        var sha = ComputeHash(sha1, bytes);
        result = BytesToHex(sha);
        return true;
    }

    private static bool TryEvalSinFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SIN", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var radians = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = Math.Sin(radians);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalSoundexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SOUNDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        var text = value?.ToString() ?? string.Empty;
        if (text.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var firstLetter = char.ToUpperInvariant(text[0]);
        var codes = new StringBuilder();

        char? lastCode = null;
        foreach (var ch in text.Skip(1))
        {
            var code = GetSoundexCode(ch);
            if (code is null)
            {
                lastCode = null;
                continue;
            }

            if (lastCode.HasValue && lastCode.Value == code.Value)
                continue;

            codes.Append(code.Value);
            lastCode = code.Value;
        }

        var soundex = new StringBuilder(4);
        soundex.Append(firstLetter);
        soundex.Append(codes);
        while (soundex.Length < 4)
            soundex.Append('0');

        if (soundex.Length > 4)
            soundex.Length = 4;

        result = soundex.ToString();
        return true;
    }

    internal static char? GetSoundexCode(char ch)
    {
        ch = char.ToUpperInvariant(ch);
        if (ch is 'A' or 'E' or 'I' or 'O' or 'U' or 'Y' or 'H' or 'W')
            return null;

        return ch switch
        {
            'B' or 'F' or 'P' or 'V' => '1',
            'C' or 'G' or 'J' or 'K' or 'Q' or 'S' or 'X' or 'Z' => '2',
            'D' or 'T' => '3',
            'L' => '4',
            'M' or 'N' => '5',
            'R' => '6',
            _ => null
        };
    }

    private static string ComputeSoundex(string value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        var firstLetter = char.ToUpperInvariant(value[0]);
        var codes = new StringBuilder();

        char? lastCode = null;
        foreach (var ch in value.Skip(1))
        {
            var code = GetSoundexCode(ch);
            if (code is null)
            {
                lastCode = null;
                continue;
            }

            if (lastCode.HasValue && lastCode.Value == code.Value)
                continue;

            codes.Append(code.Value);
            lastCode = code.Value;
        }

        var soundex = new StringBuilder(4);
        soundex.Append(firstLetter);
        soundex.Append(codes);
        while (soundex.Length < 4)
            soundex.Append('0');

        if (soundex.Length > 4)
            soundex.Length = 4;

        return soundex.ToString();
    }

    private static bool TryEvalSpaceFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SPACE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
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

    private static bool TryEvalSqrtFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SQRT", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            if (number < 0)
            {
                result = null;
                return true;
            }

            result = Math.Sqrt(number);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    internal static bool TryEvalSubDateFunction(
        FunctionCallExpr fn,
        EvalRow row,
        EvalGroup? group,
        IDictionary<string, Source> ctes,
        Func<CallExpr, EvalRow, EvalGroup?, IDictionary<string, Source>, IntervalValue?> parseIntervalValue,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SUBDATE", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("SUBDATE() espera data e intervalo.");

        var baseValue = evalArg(0);
        if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
        {
            result = null;
            return true;
        }

        var intervalExpr = fn.Args[1];
        if (intervalExpr is CallExpr intervalCall && intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase))
        {
            var intervalValue = parseIntervalValue(intervalCall, row, group, ctes);
            if (intervalValue is not { } parsedInterval)
            {
                result = null;
                return true;
            }

            result = dateTime.Subtract(parsedInterval.Span);
            return true;
        }

        var value = evalArg(1);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        if (TryConvertNumericToDouble(value, out var dayOffset))
        {
            result = dateTime.AddDays(-dayOffset);
            return true;
        }

        result = null;
        return true;
    }

    private static bool TryEvalSubstringIndexFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "SUBSTRING_INDEX", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var textValue = evalArg(0);
        var delimValue = evalArg(1);
        var countValue = evalArg(2);
        if (IsNullish(textValue) || IsNullish(delimValue) || IsNullish(countValue))
        {
            result = null;
            return true;
        }

        var text = textValue?.ToString() ?? string.Empty;
        var delim = delimValue?.ToString() ?? string.Empty;
        var count = Convert.ToInt32(countValue.ToDec());
        if (count == 0 || delim.Length == 0)
        {
            result = string.Empty;
            return true;
        }

        var parts = text.Split([delim], StringSplitOptions.None);
        if (Math.Abs(count) >= parts.Length)
        {
            result = text;
            return true;
        }

        if (count > 0)
        {
            result = string.Join(delim, parts.Take(count));
            return true;
        }

        var take = Math.Abs(count);
        result = string.Join(delim, parts.Skip(parts.Length - take));
        return true;
    }

    private static bool TryEvalTanFunction(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "TAN", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0);
        if (IsNullish(value))
        {
            result = null;
            return true;
        }

        try
        {
            var radians = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = Math.Tan(radians);
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }


}
