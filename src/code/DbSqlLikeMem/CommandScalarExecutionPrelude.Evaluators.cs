using System.Text.Json.Nodes;

namespace DbSqlLikeMem;

internal static partial class CommandScalarExecutionPrelude
{
    /// <summary>
    /// EN: Temporal evaluation entry point for date construction and date arithmetic functions.
    /// PT: Ponto de entrada de avaliacao temporal para funcoes de construcao de data e aritmetica de datas.
    /// </summary>
    internal static class Temporal
    {
        internal static bool TryEvaluate(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            if (TryEvaluateConstantDateConstructionFunction(context, functionName, args, out value))
                return true;

            if (TryEvaluateConstantDateAddFunction(context, functionName, args, out value))
                return true;

            value = null;
            return false;
        }

        private static bool TryEvaluateConstantDateConstructionFunction(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            value = null;
            if (!(functionName.Equals("DATE", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TIMESTAMP", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATETIME", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)))
                return false;

            if (args.Count < 1)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
                return false;

            if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
                return true;

            for (var i = 1; i < args.Count; i++)
            {
                if (!TryEvaluateConstantScalarExpression(context, args[i], out var modifierValue))
                    return false;

                var modifier = modifierValue?.ToString();
                if (string.IsNullOrWhiteSpace(modifier)
                    || !TryParseDateModifier(modifier!, out var unit, out var amount))
                {
                    continue;
                }

                dateTime = ApplyDateDelta(dateTime, unit, amount);
            }

            value = functionName.Equals("TIME", StringComparison.OrdinalIgnoreCase)
                ? dateTime.TimeOfDay
                : functionName.Equals("DATE", StringComparison.OrdinalIgnoreCase)
                    ? dateTime.Date
                    : dateTime;
            return true;
        }

        private static bool TryEvaluateConstantDateAddFunction(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            value = null;

            if (string.Equals(context.Dialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase)
                || string.Equals(context.Connection.ProviderExecutionDialect.Name, "sqlserver", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!(functionName.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("ADDTIME", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("DATEADD", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("TIMESTAMPADD", StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }

            if (!context.Dialect.TryGetScalarFunctionDefinition(functionName, out var addDefinition)
                || addDefinition is null
                || !addDefinition.AllowsCall)
            {
                if (addDefinition is not null)
                    throw SqlUnsupported.NotSupported(context.Dialect, functionName.ToUpperInvariant());

                return false;
            }

            if (functionName.Equals("ADDDATE", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Count < 2)
                    return false;

                if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
                    return false;

                if (IsNullish(baseValue) || !TryCoerceDateTime(baseValue, out var dateTime))
                {
                    return true;
                }

                if (args[1] is CallExpr addDateIntervalCall
                    && TryParseIntervalCall(context, addDateIntervalCall, out var addDateIntervalUnit, out var addDateIntervalAmount))
                {
                    value = ApplyDateDelta(dateTime, addDateIntervalUnit, Convert.ToInt32(addDateIntervalAmount));
                    return true;
                }

                if (!TryEvaluateConstantScalarExpression(context, args[1], out var addValue))
                    return false;

                if (TryConvertDecimal(addValue, out var dayOffset))
                {
                    value = dateTime.AddDays((double)dayOffset);
                    return true;
                }

                value = null;
                return true;
            }

            if (functionName.Equals("ADDTIME", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Count < 2)
                    return false;

                if (!TryEvaluateConstantScalarExpression(context, args[0], out var baseValue))
                    return false;

                if (!TryEvaluateConstantScalarExpression(context, args[1], out var addValue))
                    return false;

                if (IsNullish(baseValue) || IsNullish(addValue))
                {
                    value = null;
                    return true;
                }

                if (TryCoerceDateTime(baseValue, out var dateTime) && TryCoerceTimeSpan(addValue, out var addSpan))
                {
                    value = dateTime.Add(addSpan);
                    return true;
                }

                if (TryCoerceTimeSpan(baseValue, out var baseSpan) && TryCoerceTimeSpan(addValue, out var addSpan2))
                {
                    value = baseSpan.Add(addSpan2);
                    return true;
                }

                value = null;
                return true;
            }

            if (args.Count < 3)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[2], out var baseDateValue))
            {
                if (!TryResolveTemporalBaseValue(context, args[2], out baseDateValue))
                    return false;
            }

            if (IsNullish(baseDateValue) || !TryCoerceDateTime(baseDateValue, out var dateTimeValue))
            {
                value = null;
                return true;
            }

            if (!TryGetTemporalUnitFromExpression(context, args[0], out var unit))
                unit = PreludeTemporalUnit.Unknown;

            if (args[1] is CallExpr addTimeIntervalCall
                && TryParseIntervalCall(context, addTimeIntervalCall, out var addTimeIntervalUnit, out var addTimeIntervalAmount))
            {
                value = ApplyDateDelta(dateTimeValue, addTimeIntervalUnit, Convert.ToInt32(addTimeIntervalAmount));
                return true;
            }

            if (!TryEvaluateConstantScalarExpression(context, args[1], out var amountValue))
                return false;

            value = ApplyDateDelta(dateTimeValue, unit, Convert.ToInt32((amountValue ?? 0m).ToDec()));
            return true;
        }

        private static bool TryResolveTemporalBaseValue(
            QueryExecutionContext context,
            SqlExpr expr,
            out object? value)
        {
            value = null;

            switch (expr)
            {
                case IdentifierExpr identifier:
                    return context.TryEvaluateZeroArgIdentifier(identifier.Name, out value);
                case FunctionCallExpr functionCall when functionCall.Args.Count == 0:
                    return context.TryEvaluateZeroArgCall(functionCall.Name, out value);
                case CallExpr call when call.Args.Count == 0:
                    return context.TryEvaluateZeroArgCall(call.Name, out value);
                default:
                    return false;
            }
        }

        private static bool TryParseIntervalCall(
            QueryExecutionContext context,
            CallExpr intervalCall,
            out PreludeTemporalUnit unit,
            out decimal amount)
        {
            unit = PreludeTemporalUnit.Unknown;
            amount = 0m;

            if (!intervalCall.Name.Equals("INTERVAL", StringComparison.OrdinalIgnoreCase)
                || intervalCall.Args.Count < 2)
            {
                return false;
            }

            if (!TryEvaluateConstantScalarExpression(context, intervalCall.Args[0], out var amountValue)
                || !TryConvertDecimal(amountValue, out amount))
            {
                return false;
            }

            return TryGetTemporalUnitFromExpression(context, intervalCall.Args[1], out unit);
        }

        private static bool TryGetTemporalUnitFromExpression(
            QueryExecutionContext context,
            SqlExpr expr,
            out PreludeTemporalUnit unit)
        {
            unit = PreludeTemporalUnit.Unknown;

            switch (expr)
            {
                case RawSqlExpr raw:
                    unit = ResolvePreludeTemporalUnit(raw.Sql);
                    return unit != PreludeTemporalUnit.Unknown;
                case IdentifierExpr identifier:
                    unit = ResolvePreludeTemporalUnit(identifier.Name);
                    return unit != PreludeTemporalUnit.Unknown;
            }

            if (!TryEvaluateConstantScalarExpression(context, expr, out var value))
                return false;

            unit = ResolvePreludeTemporalUnit(value?.ToString() ?? string.Empty);
            return unit != PreludeTemporalUnit.Unknown;
        }

        private enum PreludeTemporalUnit
        {
            Unknown,
            Year,
            Month,
            Day,
            Hour,
            Minute,
            Second
        }

        private static bool TryCoerceDateTime(object? baseVal, out DateTime dt)
        {
            dt = default;

            if (baseVal is null || baseVal is DBNull)
                return false;

            switch (baseVal)
            {
                case DateTime d:
                    dt = d;
                    return true;
                case DateTimeOffset dto:
                    dt = dto.DateTime;
                    return true;
            }

            var text = baseVal.ToString();
            return !string.IsNullOrWhiteSpace(text)
                && DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt);
        }

        private static bool TryCoerceTimeSpan(object? baseVal, out TimeSpan span)
        {
            span = default;

            if (baseVal is null || baseVal is DBNull)
                return false;

            if (baseVal is TimeSpan ts)
            {
                span = ts;
                return true;
            }

            if (baseVal is DateTime dt)
            {
                span = dt.TimeOfDay;
                return true;
            }

            var text = baseVal.ToString();
            return !string.IsNullOrWhiteSpace(text)
                && TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out span);
        }

        private static PreludeTemporalUnit ResolvePreludeTemporalUnit(string unit)
            => unit.Trim().ToUpperInvariant() switch
            {
                SqlConst.YEAR or "YEARS" or "YY" or "YYYY" => PreludeTemporalUnit.Year,
                "MONTH" or "MONTHS" or "MM" => PreludeTemporalUnit.Month,
                "DAY" or "DAYS" or "DD" or "D" => PreludeTemporalUnit.Day,
                "HOUR" or "HOURS" or "HH" => PreludeTemporalUnit.Hour,
                "MINUTE" or "MINUTES" or "MI" or "N" => PreludeTemporalUnit.Minute,
                "SECOND" or "SECONDS" or "SS" or "S" => PreludeTemporalUnit.Second,
                _ => PreludeTemporalUnit.Unknown
            };

        private static DateTime ApplyDateDelta(DateTime dt, PreludeTemporalUnit unit, int amount) => unit switch
        {
            PreludeTemporalUnit.Year => dt.AddYears(amount),
            PreludeTemporalUnit.Month => dt.AddMonths(amount),
            PreludeTemporalUnit.Day => dt.AddDays(amount),
            PreludeTemporalUnit.Hour => dt.AddHours(amount),
            PreludeTemporalUnit.Minute => dt.AddMinutes(amount),
            PreludeTemporalUnit.Second => dt.AddSeconds(amount),
            _ => dt
        };

        private static bool TryParseDateModifier(string modifier, out PreludeTemporalUnit unit, out int amount)
        {
            unit = PreludeTemporalUnit.Unknown;
            amount = 0;

            var trimmed = modifier.Trim();
            if (trimmed.Length == 0)
                return false;

            var sign = 1;
            if (trimmed[0] is '+' or '-')
            {
                sign = trimmed[0] == '-' ? -1 : 1;
                trimmed = trimmed[1..].TrimStart();
            }

            var firstSpace = trimmed.IndexOf(' ');
            if (firstSpace <= 0 || firstSpace >= trimmed.Length - 1)
                return false;

            if (!int.TryParse(trimmed[..firstSpace], NumberStyles.Integer, CultureInfo.InvariantCulture, out amount))
                return false;

            amount *= sign;
            unit = ResolvePreludeTemporalUnit(trimmed[(firstSpace + 1)..]);
            return unit != PreludeTemporalUnit.Unknown;
        }
    }

    /// <summary>
    /// EN: JSON evaluation entry point for JSON_EXTRACT, JSON_VALUE, JSON_QUERY, and JSON_MERGE functions.
    /// PT: Ponto de entrada de avaliacao JSON para funcoes JSON_EXTRACT, JSON_VALUE, JSON_QUERY e JSON_MERGE.
    /// </summary>
    internal static class Json
    {
        internal static bool TryEvaluate(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            if (TryEvaluateConstantJsonFunction(context, functionName, args, out value))
                return true;

            if (TryEvaluateConstantJsonMergeFunction(context, functionName, args, out value))
                return true;

            value = null;
            return false;
        }

        private static bool TryEvaluateConstantJsonFunction(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            value = null;
            if (!(functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }

            EnsureJsonExtractionSupported(functionName, context);
            if (args.Count == 0)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[0], out var json))
                return false;

            if (IsNullish(json))
            {
                return true;
            }

            if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
                && args.Count == 1)
            {
                value = TryEvalJsonQueryWithoutPath(json!);
                return true;
            }

            if (args.Count < 2)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[1], out var pathValue))
                return false;

            var path = pathValue?.ToString();
            if (string.IsNullOrWhiteSpace(path))
            {
                return true;
            }

            if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, path!);
                if (!lookup.Success)
                {
                    if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                        throw new InvalidOperationException($"JSON_QUERY path '{path}' is invalid in the mock.");

                    if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                        throw new InvalidOperationException($"JSON_QUERY strict path '{path}' was not found in the JSON payload.");

                    return true;
                }

                value = lookup.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                    ? lookup.Value.GetRawText()
                    : lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict
                        ? throw new InvalidOperationException($"JSON_QUERY strict path '{path}' was not found in the JSON payload.")
                        : null;
                return true;
            }

            if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase))
            {
                if (context.Dialect.Name.Equals("sqlserver", StringComparison.OrdinalIgnoreCase))
                {
                    var lookup = QueryJsonFunctionHelper.LookupJsonPath(json!, path!);
                    if (!lookup.Success)
                    {
                        if (lookup.Failure == QueryJsonFunctionHelper.JsonPathLookupFailure.InvalidPath)
                            throw new InvalidOperationException($"JSON_VALUE path '{path}' is invalid in the mock.");

                        if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                            throw new InvalidOperationException($"JSON_VALUE strict path '{path}' was not found in the JSON payload.");

                        return true;
                    }

                    value = QueryJsonFunctionHelper.ConvertJsonElementToSqlServerJsonValue(lookup.Value);
                    if (value is null
                        && lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict
                        && lookup.Value.ValueKind is JsonValueKind.Object or JsonValueKind.Array)
                        throw new InvalidOperationException($"JSON_VALUE strict path '{path}' was not found in the JSON payload.");

                    if (value is string text && text.Length > 4000)
                    {
                        if (lookup.Mode == QueryJsonFunctionHelper.JsonPathMode.Strict)
                            throw new InvalidOperationException($"JSON_VALUE strict path '{path}' exceeds the 4000 character limit.");

                        value = null;
                        return true;
                    }

                    return true;
                }

                var extracted = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
                value = QueryJsonFunctionHelper.ApplyJsonValueReturningClause(
                    new FunctionCallExpr(functionName, args).BindScalarFunctionDefinition(context.Dialect),
                    extracted);
                return true;
            }

            value = QueryJsonFunctionHelper.TryReadJsonPathValue(json!, path!);
            return true;
        }

        private static void EnsureJsonExtractionSupported(
            string functionName,
            QueryExecutionContext context)
        {
            var dialect = context.Dialect;
            if (dialect.TryGetScalarFunctionDefinition(functionName, out var definition))
            {
                if (definition is null || definition.AllowsCall)
                    return;

                throw SqlUnsupported.NotSupported(dialect, functionName.ToUpperInvariant());
            }

            if (functionName.Equals("JSON_EXTRACT", StringComparison.OrdinalIgnoreCase)
                && (!dialect.TryGetScalarFunctionDefinition("JSON_EXTRACT", out var jsonExtractDefinition)
                    || jsonExtractDefinition is null
                    || !jsonExtractDefinition.AllowsCall))
                throw SqlUnsupported.NotSupported(dialect, "JSON_EXTRACT");

            if (functionName.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase)
                && (!dialect.TryGetScalarFunctionDefinition("JSON_QUERY", out var jsonQueryDefinition)
                    || jsonQueryDefinition is null
                    || !jsonQueryDefinition.AllowsCall))
                throw SqlUnsupported.NotSupported(dialect, "JSON_QUERY");

            if (functionName.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase)
                && (!dialect.TryGetScalarFunctionDefinition("JSON_VALUE", out var jsonValueDefinition)
                    || jsonValueDefinition is null
                    || !jsonValueDefinition.AllowsCall))
                throw SqlUnsupported.NotSupported(dialect, "JSON_VALUE");
        }

        private static object? TryEvalJsonQueryWithoutPath(object json)
        {
            if (!QueryJsonFunctionHelper.TryGetJsonRootElement(json, out var root))
                return null;

            return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                ? root.GetRawText()
                : null;
        }

        private static bool TryEvaluateConstantJsonMergeFunction(
            QueryExecutionContext context,
            string functionName,
            IReadOnlyList<SqlExpr> args,
            out object? value)
        {
            value = null;
            if (!(functionName.Equals("JSON_MERGE", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("JSON_MERGE_PRESERVE", StringComparison.OrdinalIgnoreCase)
                || functionName.Equals("JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase)))
            {
                return false;
            }

            if (!context.Dialect.TryGetScalarFunctionDefinition(functionName, out var definition)
                || definition is null
                || !definition.AllowsCall)
            {
                return false;
            }

            if (args.Count < 2)
                return false;

            if (!TryEvaluateConstantScalarExpression(context, args[0], out var firstValue))
                return false;

            if (!TryParseConstantJsonNode(firstValue, out var mergedRoot))
                return false;

            for (var i = 1; i < args.Count; i++)
            {
                if (!TryEvaluateConstantScalarExpression(context, args[i], out var nextValue))
                    return false;

                if (!TryParseConstantJsonNode(nextValue, out var nextNode))
                    return false;

                mergedRoot = string.Equals(functionName, "JSON_MERGE_PATCH", StringComparison.OrdinalIgnoreCase)
                    ? MergeConstantJsonPatch(mergedRoot, nextNode)
                    : MergeConstantJsonPreserve(mergedRoot, nextNode);
            }

            value = mergedRoot!.ToJsonString();
            return true;
        }

        private static bool TryParseConstantJsonNode(object? value, out JsonNode? node)
        {
            if (value is null or DBNull)
            {
                node = null;
                return true;
            }

            if (value is JsonNode jsonNode)
            {
                node = jsonNode.DeepClone();
                return true;
            }

            return AstQueryJsonPathFunctionEvaluator.TryParseJsonNode(value!, out node);
        }

        private static JsonNode MergeConstantJsonPreserve(JsonNode? left, JsonNode? right)
        {
            if (left is null)
                return right?.DeepClone() ?? JsonValue.Create((string?)null)!;

            if (right is null)
                return left.DeepClone();

            if (left is JsonObject leftObject && right is JsonObject rightObject)
            {
                var merged = new JsonObject();
                foreach (var prop in leftObject)
                {
                    if (prop.Value is not null)
                        merged[prop.Key] = prop.Value.DeepClone();
                }

                foreach (var prop in rightObject)
                {
                    if (!merged.TryGetPropertyValue(prop.Key, out var existing) || existing is null)
                    {
                        merged[prop.Key] = prop.Value?.DeepClone();
                        continue;
                    }

                    merged[prop.Key] = MergeConstantJsonPreserve(existing, prop.Value);
                }

                return merged;
            }

            if (left is JsonArray leftArray && right is JsonArray rightArray)
            {
                var merged = new JsonArray();
                foreach (var item in leftArray)
                    merged.Add(item?.DeepClone());
                foreach (var item in rightArray)
                    merged.Add(item?.DeepClone());
                return merged;
            }

            if (left is JsonArray leftArrayOnly)
            {
                var merged = new JsonArray();
                foreach (var item in leftArrayOnly)
                    merged.Add(item?.DeepClone());
                merged.Add(right.DeepClone());
                return merged;
            }

            if (right is JsonArray rightArrayOnly)
            {
                var merged = new JsonArray();
                merged.Add(left.DeepClone());
                foreach (var item in rightArrayOnly)
                    merged.Add(item?.DeepClone());
                return merged;
            }

            return new JsonArray
            {
                left.DeepClone(),
                right.DeepClone()
            };
        }

        private static JsonNode MergeConstantJsonPatch(JsonNode? left, JsonNode? right)
        {
            if (right is null)
                return JsonValue.Create((string?)null)!;

            if (left is not JsonObject leftObject || right is not JsonObject rightObject)
                return right.DeepClone();

            var merged = new JsonObject();
            foreach (var prop in leftObject)
            {
                if (prop.Value is not null)
                    merged[prop.Key] = prop.Value.DeepClone();
            }

            foreach (var prop in rightObject)
            {
                if (prop.Value is null)
                {
                    merged.Remove(prop.Key);
                    continue;
                }

                if (prop.Value is JsonValue jsonValue
                    && string.Equals(jsonValue.ToJsonString(), "null", StringComparison.OrdinalIgnoreCase))
                {
                    merged.Remove(prop.Key);
                    continue;
                }

                merged[prop.Key] = prop.Value.DeepClone();
            }

            return merged;
        }
    }

    private static bool TryConvertDecimal(object? value, out decimal result)
    {
        result = 0m;
        if (IsNullish(value))
            return false;

        try
        {
            result = Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            return true;
        }
        catch
        {
            return false;
        }
    }

    private static bool TryCoerceBooleanValue(object? value, out bool result)
    {
        result = false;

        if (value is bool boolean)
        {
            result = boolean;
            return true;
        }

        if (IsNullish(value))
            return false;

        if (value is string text && bool.TryParse(text, out var parsedBool))
        {
            result = parsedBool;
            return true;
        }

        if (value is IConvertible)
        {
            try
            {
                result = Convert.ToBoolean(value, CultureInfo.InvariantCulture);
                return true;
            }
            catch
            {
                return false;
            }
        }

        return false;
    }

    private static bool TryConstantEqualityCore(object? left, object? right)
        => (IsNullish(left) && IsNullish(right))
            || (!IsNullish(left) && !IsNullish(right) && Equals(left, right));

    private static bool IsNullish(object? value)
        => value is null or DBNull;
}
