namespace DbSqlLikeMem;

using System;
using System.Globalization;
using System.Text;

internal static class AstQueryPostgresTextFunctionEvaluator
{
    internal static bool TryEvaluate(
        FunctionCallExpr fn,
        QueryExecutionContext context,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (!context.Dialect.Name.Equals("postgresql", StringComparison.OrdinalIgnoreCase))
        {
            result = null;
            return false;
        }

        var name = fn.Name.ToUpperInvariant();
        if (name is not ("BTRIM" or "INITCAP" or "CHR" or "SPLIT_PART" or "STRING_TO_ARRAY" or "QUOTE_LITERAL" or "QUOTE_IDENT" or "TO_HEX" or "TRANSLATE" or "STARTS_WITH"))
        {
            result = null;
            return false;
        }

        if (name is "BTRIM")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = text.Trim();
            return true;
        }

        if (name is "INITCAP")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = CultureInfo.InvariantCulture.TextInfo.ToTitleCase(text.ToLowerInvariant());
            return true;
        }

        if (name is "CHR")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            try
            {
                var code = Convert.ToInt32(value.ToDec(), CultureInfo.InvariantCulture);
                if (code < 0 || code > 0x10FFFF)
                {
                    result = null;
                    return true;
                }

                result = char.ConvertFromUtf32(code);
                return true;
            }
            catch
            {
                result = null;
                return true;
            }
        }

        if (name is "SPLIT_PART")
        {
            if (fn.Args.Count < 3)
                throw new InvalidOperationException("SPLIT_PART() espera texto, separador e indice.");

            var text = evalArg(0)?.ToString() ?? string.Empty;
            var delimiter = evalArg(1)?.ToString() ?? string.Empty;
            var index = Convert.ToInt32(evalArg(2).ToDec());
            if (index <= 0)
            {
                result = string.Empty;
                return true;
            }

            var parts = text.Split([delimiter], StringSplitOptions.None);
            result = index <= parts.Length ? parts[index - 1] : string.Empty;
            return true;
        }

        if (name is "STRING_TO_ARRAY")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("STRING_TO_ARRAY() espera texto e separador.");

            var text = evalArg(0)?.ToString() ?? string.Empty;
            var delimiter = evalArg(1)?.ToString() ?? string.Empty;
            result = text.Split([delimiter], StringSplitOptions.None);
            return true;
        }

        if (name is "QUOTE_LITERAL")
        {
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

        if (name is "QUOTE_IDENT")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var text = value?.ToString() ?? string.Empty;
            result = $"\"{text.Replace("\"", "\"\"")}\"";
            return true;
        }

        if (name is "TO_HEX")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var number = Convert.ToInt64(value, CultureInfo.InvariantCulture);
            result = number.ToString("x", CultureInfo.InvariantCulture);
            return true;
        }

        if (name is "TRANSLATE")
        {
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

        if (name is "STARTS_WITH")
        {
            if (!context.Dialect.TryGetScalarFunctionDefinition(name, out _))
            {
                result = null;
                return false;
            }

            if (fn.Args.Count < 2)
                throw new InvalidOperationException("STARTS_WITH() espera texto e prefixo.");

            var source = evalArg(0)?.ToString();
            var prefix = evalArg(1)?.ToString();
            if (source is null || prefix is null)
            {
                result = null;
                return true;
            }

            result = source.StartsWith(prefix, StringComparison.Ordinal);
            return true;
        }

        result = null;
        return false;
    }
}
