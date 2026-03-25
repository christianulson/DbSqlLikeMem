namespace DbSqlLikeMem;

using System;
using System.Collections;
using System.Globalization;
using System.Linq;
using System.Text.Json;

internal static class AstQueryPostgresArrayFunctionEvaluator
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
        if (name is not ("ARRAY_TO_STRING" or "ARRAY_LENGTH" or "ARRAY_UPPER" or "ARRAY_LOWER" or "ARRAY_DIMS" or "ARRAY_NDIMS" or "ARRAY_POSITION" or "ARRAY_POSITIONS" or "ARRAY_TO_JSON" or "ARRAY_APPEND" or "ARRAY_PREPEND" or "ARRAY_CAT" or "ARRAY_REMOVE" or "ARRAY_REPLACE"))
        {
            result = null;
            return false;
        }

        if (name is "ARRAY_TO_STRING")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_TO_STRING() espera array e separador.");

            var value = evalArg(0);
            var separator = evalArg(1)?.ToString() ?? string.Empty;
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is IEnumerable enumerable)
            {
                var items = new List<string>();
                foreach (var item in enumerable)
                    items.Add(item?.ToString() ?? string.Empty);
                result = string.Join(separator, items);
                return true;
            }
        }

        if (name is "ARRAY_LENGTH" or "ARRAY_UPPER" or "ARRAY_LOWER")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            if (list.Count == 0)
            {
                result = null;
                return true;
            }

            result = name switch
            {
                "ARRAY_LENGTH" => list.Count,
                "ARRAY_UPPER" => list.Count,
                _ => 1
            };
            return true;
        }

        if (name is "ARRAY_DIMS" or "ARRAY_NDIMS")
        {
            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            if (list.Count == 0)
            {
                result = null;
                return true;
            }

            result = name == "ARRAY_DIMS"
                ? $"[1:{list.Count}]"
                : 1;
            return true;
        }

        if (name is "ARRAY_POSITION")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_POSITION() espera array e valor.");

            var value = evalArg(0);
            var target = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];
            var index = list.FindIndex(item => Equals(item, target));
            result = index >= 0 ? index + 1 : (object?)null;
            return true;
        }

        if (name is "ARRAY_POSITIONS")
        {
            if (fn.Args.Count < 2)
                throw new InvalidOperationException("ARRAY_POSITIONS() espera array e valor.");

            var value = evalArg(0);
            var target = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            var matches = new List<object?>(list.Count);
            for (var i = 0; i < list.Count; i++)
            {
                if (Equals(list[i], target))
                    matches.Add(i + 1);
            }

            result = matches.ToArray();
            return true;
        }

        if (name is "ARRAY_TO_JSON")
        {
            if (fn.Args.Count == 0)
                throw new InvalidOperationException("ARRAY_TO_JSON() espera array.");

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var list = value is IEnumerable enumerable
                ? enumerable.Cast<object?>().ToList()
                : [];

            var writeIndented = fn.Args.Count > 1 && Convert.ToBoolean(evalArg(1), CultureInfo.InvariantCulture);
            var options = writeIndented
                ? new JsonSerializerOptions { WriteIndented = true }
                : null;
            result = JsonSerializer.Serialize(list, options);
            return true;
        }

        if (name is "ARRAY_APPEND" or "ARRAY_PREPEND" or "ARRAY_CAT" or "ARRAY_REMOVE" or "ARRAY_REPLACE")
        {
            var left = name is "ARRAY_PREPEND" ? evalArg(1) : evalArg(0);
            var leftEnumerable = !AstQueryExecutorBase.IsNullish(left) && left is IEnumerable enumerable ? enumerable : null;
            var list = leftEnumerable is ICollection leftCollection
                ? new List<object?>(leftCollection.Count)
                : new List<object?>();
            if (leftEnumerable is not null)
                list.AddRange(leftEnumerable.Cast<object?>());

            if (name is "ARRAY_CAT")
            {
                var right = evalArg(1);
                if (!AstQueryExecutorBase.IsNullish(right) && right is IEnumerable rightEnum)
                {
                    if (rightEnum is ICollection rightCollection
                        && list.Capacity < list.Count + rightCollection.Count)
                    {
                        list.Capacity = list.Count + rightCollection.Count;
                    }

                    list.AddRange(rightEnum.Cast<object?>());
                }

                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_APPEND")
            {
                list.Add(evalArg(1));
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_PREPEND")
            {
                list.Insert(0, evalArg(0));
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_REMOVE")
            {
                var target = evalArg(1);
                list = [.. list.Where(item => !Equals(item, target))];
                result = list.ToArray();
                return true;
            }

            if (name is "ARRAY_REPLACE")
            {
                var target = evalArg(1);
                var replacement = evalArg(2);
                for (var i = 0; i < list.Count; i++)
                {
                    if (Equals(list[i], target))
                        list[i] = replacement;
                }

                result = list.ToArray();
                return true;
            }
        }

        result = null;
        return false;
    }
}
