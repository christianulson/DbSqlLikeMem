namespace DbSqlLikeMem;

internal static class AstQueryOracleDb2BinaryTextFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        return TryEvalOracleBfilenameFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleHashFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleRawFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleRegexFunctions(fn, dialect, evalArg, out result)
            || TryEvalOracleRemainderFunction(fn, dialect, evalArg, out result)
            || TryEvalOracleRowIdFunctions(fn, dialect, evalArg, out result);
    }

    private static bool TryEvalOracleBfilenameFunction(
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("BFILENAME", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, fn);

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("STANDARD_HASH" or "ORA_HASH"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("RAWTOHEX" or "RAWTONHEX" or "REF" or "REFTOHEX"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("REGEXP_COUNT" or "REGEXP_INSTR" or "REGEXP_REPLACE" or "REGEXP_SUBSTR"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!fn.Name.Equals("REMAINDER", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
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
        FunctionCallExpr fn,
        ISqlDialect dialect,
        Func<int, object?> evalArg,
        out object? result)
    {
        var name = fn.Name.ToUpperInvariant();
        if (name is not ("ROWIDTOCHAR" or "ROWTONCHAR"))
        {
            result = null;
            return false;
        }

        if (!dialect.Name.Equals("oracle", StringComparison.OrdinalIgnoreCase)
            && !dialect.Name.Equals("db2", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return true;
        }

        QueryOracleDb2UtilityFunctionHelper.EnsureOracleDb2FunctionSupported(dialect, name);

        if (fn.Args.Count == 0)
        {
            result = null;
            return true;
        }

        var value = evalArg(0);
        result = AstQueryExecutorBase.IsNullish(value) ? null : value?.ToString();
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
