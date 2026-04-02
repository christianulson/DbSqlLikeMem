namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2BinaryTextFunctionEvaluator
{
    private static readonly HashSet<string> _hashFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "STANDARD_HASH",
        "ORA_HASH"
    };

    private static readonly HashSet<string> _rawFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "RAWTOHEX",
        "RAWTONHEX",
        "REF",
        "REFTOHEX"
    };

    private static readonly HashSet<string> _regexFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "REGEXP_COUNT",
        "REGEXP_INSTR",
        "REGEXP_REPLACE",
        "REGEXP_SUBSTR"
    };

    private static readonly HashSet<string> _rowIdFunctionNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "ROWIDTOCHAR",
        "ROWTONCHAR"
    };

    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalOracleBfilenameFunction, "BFILENAME");
        Register(handlers, TryEvalOracleHashFunction, "STANDARD_HASH", "ORA_HASH");
        Register(handlers, TryEvalOracleRawFunctions, "RAWTOHEX", "RAWTONHEX", "REF", "REFTOHEX");
        Register(handlers, TryEvalOracleRegexFunctions, "REGEXP_COUNT", "REGEXP_INSTR", "REGEXP_REPLACE", "REGEXP_SUBSTR");
        Register(handlers, TryEvalOracleRemainderFunction, "REMAINDER");
        Register(handlers, TryEvalOracleRowIdFunctions, "ROWIDTOCHAR", "ROWTONCHAR");
        Register(handlers, TryEvalOracleHexToRawFunction, "HEXTORAW");

        return handlers;
    }

    private static void Register(
        IDictionary<string, AstQueryGeneralScalarFunctionHandler> handlers,
        AstQueryGeneralScalarFunctionHandler handler,
        params string[] names)
    {
        foreach (var name in names)
            handlers[name] = handler;
    }

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        result = null;
        return false;
    }

    private static bool TryEvalOracleBfilenameFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "BFILENAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(fn);

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("BFILENAME() espera diretorio e nome do arquivo.");

        var dir = evalArg(0);
        var name = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(dir) || AstQueryExecutorBase.IsNullish(name))
        {
            result = null;
            return true;
        }

        result = $"{dir}/{name}";
        return true;
    }

    private static bool TryEvalOracleHashFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_hashFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        var algorithm = name.Equals("ORA_HASH", StringComparison.OrdinalIgnoreCase)
            ? "MD5"
            : (fn.Args.Count > 1 ? evalArg(1)?.ToString() : "SHA1");

        var text = value?.ToString() ?? string.Empty;
        var bytes = Encoding.UTF8.GetBytes(text);

        var normalized = algorithm?.ToUpperInvariant() ?? "SHA1";
        byte[] hashBytes;
        using (var hasher = CreateHashAlgorithm(normalized))
        {
            if (hasher is null)
            {
                result = null;
                return true;
            }

            hashBytes = hasher.ComputeHash(bytes);
        }

        var hex = ToHexString(hashBytes);
        if (name.Equals("ORA_HASH", StringComparison.OrdinalIgnoreCase))
        {
            var hash = 0;
            foreach (var b in hashBytes)
                hash = unchecked((hash * 31) + b);
            result = Math.Abs(hash);
            return true;
        }

        result = hex;
        return true;
    }

    private static bool TryEvalOracleRawFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_rawFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        if (AstQueryExecutorBase.IsNullish(value))
        {
            result = null;
            return true;
        }

        if (name == "REF")
        {
            result = value;
            return true;
        }

        var bytes = value switch
        {
            byte[] buffer => buffer,
            string text => Encoding.UTF8.GetBytes(text),
            _ => Encoding.UTF8.GetBytes(value!.ToString() ?? string.Empty)
        };

        result = ToHexString(bytes);
        return true;
    }

    private static bool TryEvalOracleRegexFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_regexFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
        {
            result = null;
            return true;
        }

        var source = evalArg(0)?.ToString() ?? string.Empty;
        var pattern = evalArg(1)?.ToString() ?? string.Empty;
        if (string.IsNullOrEmpty(pattern))
        {
            result = null;
            return true;
        }

        var start = 1;
        if (fn.Args.Count >= 3 && !AstQueryExecutorBase.IsNullish(evalArg(2)))
            start = Math.Max(1, Convert.ToInt32(evalArg(2)!.ToDec()));

        var startIndex = Math.Min(source.Length, Math.Max(0, start - 1));
        var options = RegexOptions.CultureInvariant;

        try
        {
            if (name == "REGEXP_COUNT")
            {
                var matches = Regex.Matches(source[startIndex..], pattern, options);
                result = matches.Count;
                return true;
            }

            if (name == "REGEXP_REPLACE")
            {
                var replacement = fn.Args.Count >= 3 ? evalArg(2)?.ToString() ?? string.Empty : string.Empty;
                result = Regex.Replace(source, pattern, replacement, options);
                return true;
            }

            var matchesForInstr = Regex.Matches(source[startIndex..], pattern, options);
            if (matchesForInstr.Count == 0)
            {
                result = 0;
                return true;
            }

            var occurrence = 1;
            if (fn.Args.Count >= 4 && !AstQueryExecutorBase.IsNullish(evalArg(3)))
                occurrence = Math.Max(1, Convert.ToInt32(evalArg(3)!.ToDec()));

            var idx = Math.Min(occurrence - 1, matchesForInstr.Count - 1);
            var match = matchesForInstr[idx];

            if (name == "REGEXP_INSTR")
            {
                result = startIndex + match.Index + 1;
                return true;
            }

            result = match.Value;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalOracleRemainderFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "REMAINDER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (fn.Args.Count < 2)
            throw new InvalidOperationException("REMAINDER() espera 2 argumentos.");

        var left = evalArg(0);
        var right = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(left) || AstQueryExecutorBase.IsNullish(right))
        {
            result = null;
            return true;
        }

        var leftValue = Convert.ToDouble(left, CultureInfo.InvariantCulture);
        var rightValue = Convert.ToDouble(right, CultureInfo.InvariantCulture);
        if (rightValue == 0)
        {
            result = null;
            return true;
        }

        result = Math.IEEERemainder(leftValue, rightValue);
        return true;
    }

    private static bool TryEvalOracleRowIdFunctions(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (!_rowIdFunctionNames.Contains(name))
        {
            result = null;
            return false;
        }

        context.EnsureOracleDb2FunctionSupported(name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value?.ToString();
        return true;
    }

    private static bool TryEvalOracleHexToRawFunction(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!string.Equals(fn.Name, "HEXTORAW", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var value = evalArg(0)?.ToString() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(value))
        {
            result = null;
            return true;
        }

        if (!AstQueryRuntimeHelper.TryNormalizeHexPayload(value, out var hex) || hex.Length % 2 != 0)
        {
            result = null;
            return true;
        }

        var buffer = new byte[hex.Length / 2];
        for (var i = 0; i < hex.Length; i += 2)
        {
            if (!byte.TryParse(hex.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
            {
                result = null;
                return true;
            }

            buffer[i / 2] = part;
        }

        result = buffer;
        return true;
    }

    private static HashAlgorithm? CreateHashAlgorithm(string algorithm)
    {
        try
        {
            return algorithm switch
            {
                "SHA256" => SHA256.Create(),
                "SHA384" => SHA384.Create(),
                "SHA512" => SHA512.Create(),
                "MD5" => MD5.Create(),
                _ => SHA1.Create()
            };
        }
        catch
        {
            return null;
        }
    }

    private static string ToHexString(byte[] bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        foreach (var b in bytes)
            sb.Append(b.ToString("X2", CultureInfo.InvariantCulture));
        return sb.ToString();
    }
}

