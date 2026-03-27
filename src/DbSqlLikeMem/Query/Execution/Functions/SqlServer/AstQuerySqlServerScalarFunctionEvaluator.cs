namespace DbSqlLikeMem;

internal static class AstQuerySqlServerScalarFunctionEvaluator
{
    private static readonly Dictionary<string, AstQueryGeneralScalarFunctionHandler> _handlers = CreateHandlers();

    internal static bool TryEvaluate(
        this QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        result = null;

        if (_handlers.TryGetValue(fn.Name, out var handler))
            return handler(context, fn, evalArg, out result);

        return false;
    }

    private static Dictionary<string, AstQueryGeneralScalarFunctionHandler> CreateHandlers()
    {
        var handlers = new Dictionary<string, AstQueryGeneralScalarFunctionHandler>(StringComparer.OrdinalIgnoreCase);

        Register(handlers, TryEvalQuotenameFunction, "QUOTENAME");
        Register(handlers, TryEvalReplicateFunction, "REPLICATE");
        Register(handlers, TryEvalSquareFunction, "SQUARE");
        Register(handlers, TryEvalStuffFunction, "STUFF");
        Register(handlers, TryEvalParsenameFunction, "PARSENAME");

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

    private static bool TryEvalQuotenameFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
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
        var delimiter = fn.Args.Count > 1 ? evalArg(1)?.ToString() : null;
        var quoteChar = string.IsNullOrEmpty(delimiter) ? "[" : delimiter![0].ToString();
        var closingChar = quoteChar switch
        {
            "[" => "]",
            "(" => ")",
            "<" => ">",
            "{" => "}",
            _ => quoteChar
        };
        var escaped = text.Replace(closingChar, closingChar + closingChar);
        result = quoteChar + escaped + closingChar;
        return true;
    }

    private static bool TryEvalReplicateFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
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

    private static bool TryEvalSquareFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
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
            var number = Convert.ToDouble(value, CultureInfo.InvariantCulture);
            result = number * number;
            return true;
        }
        catch
        {
            result = null;
            return true;
        }
    }

    private static bool TryEvalStuffFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        if (fn.Args.Count < 4)
            throw new InvalidOperationException("STUFF() espera 4 argumentos.");

        var sourceValue = evalArg(0);
        var startValue = evalArg(1);
        var lengthValue = evalArg(2);
        var replaceValue = evalArg(3);
        if (AstQueryExecutorBase.IsNullish(sourceValue)
            || AstQueryExecutorBase.IsNullish(startValue)
            || AstQueryExecutorBase.IsNullish(lengthValue)
            || AstQueryExecutorBase.IsNullish(replaceValue))
        {
            result = null;
            return true;
        }

        var source = sourceValue?.ToString() ?? string.Empty;
        var start = Convert.ToInt32(startValue.ToDec());
        var length = Convert.ToInt32(lengthValue.ToDec());
        var replacement = replaceValue?.ToString() ?? string.Empty;
        if (start <= 0 || length < 0 || start > source.Length + 1)
        {
            result = null;
            return true;
        }

        var zeroBasedStart = start - 1;
        var safeLength = Math.Min(length, source.Length - zeroBasedStart);
        result = source.Remove(zeroBasedStart, safeLength).Insert(zeroBasedStart, replacement);
        return true;
    }

    private static bool TryEvalParsenameFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        var objectNameValue = evalArg(0);
        var pieceValue = evalArg(1);
        if (AstQueryExecutorBase.IsNullish(objectNameValue) || AstQueryExecutorBase.IsNullish(pieceValue))
        {
            result = null;
            return true;
        }

        var objectName = objectNameValue?.ToString() ?? string.Empty;
        var piece = Convert.ToInt32(pieceValue.ToDec());
        if (piece is < 1 or > 4)
        {
            result = null;
            return true;
        }

        var parts = objectName.Split('.');
        var indexFromEnd = piece - 1;
        result = indexFromEnd < parts.Length
            ? parts[^(indexFromEnd + 1)]
            : null;
        return true;
    }
}
