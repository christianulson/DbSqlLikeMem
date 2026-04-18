using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem;

internal static class AstQueryExecutorForJsonHelper
{
    private const string SqlServerForJsonColumnName = "JSON_F52E2B61-18A1-11d1-B105-00805F49916B";

    internal static TableResultMock ApplyForJsonIfNeeded(
        TableResultMock result,
        SqlSelectQuery query,
        QueryDebugTraceBuilder? debugTrace = null)
    {
        if (query.ForJson is null)
            return result;

        var serializeStart = debugTrace is not null ? Stopwatch.GetTimestamp() : 0L;
        var serialized = SerializeForJson(result, query);
        debugTrace?.AddStep(
            "ForJson",
            result.Count,
            serialized.Count,
            TimeSpan.FromTicks(StopwatchCompatible.GetElapsedTicks(serializeStart)),
            $"{query.ForJson.Mode.ToString().ToUpperInvariant()}{(query.ForJson.RootName is null ? string.Empty : $";root={query.ForJson.RootName}")}");
        return serialized;
    }

    private static TableResultMock SerializeForJson(TableResultMock result, SqlSelectQuery query)
    {
        var clause = query.ForJson ?? throw new InvalidOperationException("FOR JSON clause expected by serializer.");
        var payload = clause.Mode switch
        {
            SqlForJsonMode.Path => SerializeForJsonPath(result, clause),
            SqlForJsonMode.Auto => SerializeForJsonAuto(result, query, clause),
            _ => throw new NotSupportedException($"FOR JSON mode '{clause.Mode}' not supported in the mock.")
        };

        var table = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock(string.Empty, SqlServerForJsonColumnName, SqlServerForJsonColumnName, 0, DbType.String, false)
            ]
        };
        table.Add(new Dictionary<int, object?> { [0] = payload });
        return table;
    }

    private static string SerializeForJsonPath(TableResultMock result, SqlForJsonClause clause)
    {
        ValidateForJsonPathProjectionOrder(result);

        var rowJson = new List<string>(result.Count);
        foreach (var row in result)
            rowJson.Add(JsonSerializer.Serialize(BuildPathJsonObject(result, row, clause.IncludeNullValues)));

        return WrapForJsonPayload(rowJson, clause);
    }

    private static string SerializeForJsonAuto(TableResultMock result, SqlSelectQuery query, SqlForJsonClause clause)
    {
        var projections = BuildAutoJsonProjections(result, query);
        var rootAlias = query.Table?.Alias ?? query.Table?.Name;

        var grouped = new List<AutoJsonRootRow>(Math.Max(1, result.Count));
        var groupedIndex = new Dictionary<string, int>(StringComparer.Ordinal);

        foreach (var row in result)
        {
            var rootProperties = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            var nestedByAlias = new Dictionary<string, Dictionary<string, object?>>(StringComparer.OrdinalIgnoreCase);
            var nestedHasNonNullValueByAlias = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

            for (var i = 0; i < projections.Count; i++)
            {
                var projection = projections[i];
                var value = row.TryGetValue(projection.ColumnIndex, out var rawValue) ? rawValue : null;

                if (projection.Qualifier is null
                    || rootAlias is null
                    || projection.Qualifier.Equals(rootAlias, StringComparison.OrdinalIgnoreCase))
                {
                    AddJsonProperty(rootProperties, projection.PropertyName, value, clause.IncludeNullValues, projection.IsJsonFragment);
                    continue;
                }

                if (!nestedByAlias.TryGetValue(projection.Qualifier, out var nested))
                {
                    nested = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    nestedByAlias[projection.Qualifier] = nested;
                    nestedHasNonNullValueByAlias[projection.Qualifier] = false;
                }

                AddJsonProperty(nested, projection.PropertyName, value, clause.IncludeNullValues, projection.IsJsonFragment);
                if (NormalizeForJsonValue(value, projection.IsJsonFragment) is not null)
                    nestedHasNonNullValueByAlias[projection.Qualifier] = true;
            }

            var rootKey = JsonSerializer.Serialize(rootProperties);
            if (!groupedIndex.TryGetValue(rootKey, out var rootIndex))
            {
                rootIndex = grouped.Count;
                groupedIndex[rootKey] = rootIndex;
                grouped.Add(new AutoJsonRootRow(rootProperties));
            }

            var groupedRoot = grouped[rootIndex];
            foreach (var nested in nestedByAlias)
            {
                if (nested.Value.Count == 0
                    || !nestedHasNonNullValueByAlias.TryGetValue(nested.Key, out var hasNonNullValue)
                    || !hasNonNullValue)
                    continue;

                if (!groupedRoot.Nested.TryGetValue(nested.Key, out var items))
                {
                    items = [];
                    groupedRoot.Nested[nested.Key] = items;
                }

                items.Add(nested.Value);
            }
        }

        var serializedRows = grouped
            .ConvertAll(static groupedRow => JsonSerializer.Serialize(groupedRow.ToJsonObject()));

        return WrapForJsonPayload(serializedRows, clause);
    }

    private static string WrapForJsonPayload(IReadOnlyList<string> serializedRows, SqlForJsonClause clause)
    {
        var payload = clause.WithoutArrayWrapper
            ? serializedRows.Count switch
            {
                0 => "[]",
                1 => serializedRows[0],
                _ => string.Join(",", serializedRows)
            }
            : $"[{string.Join(",", serializedRows)}]";

        if (clause.RootName is null)
            return payload;

        return "{" + JsonSerializer.Serialize(clause.RootName) + ":" + payload + "}";
    }

    private static Dictionary<string, object?> BuildPathJsonObject(
        TableResultMock result,
        Dictionary<int, object?> row,
        bool includeNullValues)
    {
        var root = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        for (var index = 0; index < result.Columns.Count; index++)
        {
            var column = result.Columns[index];
            var value = row.TryGetValue(index, out var rawValue) ? rawValue : null;
            AddPathJsonProperty(root, column.ColumnAlias, value, includeNullValues, column.IsJsonFragment);
        }

        return root;
    }

    private static void ValidateForJsonPathProjectionOrder(TableResultMock result)
    {
        var terminalPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var objectPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var closedObjectPaths = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        string[]? previousSegments = null;

        foreach (var column in result.Columns)
        {
            var segments = column.ColumnAlias
                .Split('.')
                .Select(static segment => segment.Trim())
                .Where(static segment => !string.IsNullOrWhiteSpace(segment))
                .ToArray();
            if (segments.Length == 0)
                continue;

            if (previousSegments is not null)
            {
                var commonPrefixLength = 0;
                var maxShared = Math.Min(previousSegments.Length, segments.Length);
                while (commonPrefixLength < maxShared
                    && previousSegments[commonPrefixLength].Equals(segments[commonPrefixLength], StringComparison.OrdinalIgnoreCase))
                {
                    commonPrefixLength++;
                }

                for (var depth = commonPrefixLength + 1; depth < previousSegments.Length; depth++)
                    closedObjectPaths.Add(BuildJsonPath(previousSegments, depth));

                for (var depth = 1; depth < commonPrefixLength; depth++)
                {
                    var sharedPrefix = BuildJsonPath(segments, depth);
                    if (closedObjectPaths.Contains(sharedPrefix))
                        throw CreateForJsonPathConflictException(column.ColumnAlias);
                }
            }

            for (var depth = 1; depth < segments.Length; depth++)
            {
                var prefix = BuildJsonPath(segments, depth);
                if (closedObjectPaths.Contains(prefix))
                    throw CreateForJsonPathConflictException(column.ColumnAlias);

                if (terminalPaths.Contains(prefix))
                    throw CreateForJsonPathConflictException(column.ColumnAlias);
            }

            var fullPath = BuildJsonPath(segments, segments.Length);
            if (terminalPaths.Contains(fullPath) || objectPaths.Contains(fullPath))
                throw CreateForJsonPathConflictException(column.ColumnAlias);

            for (var depth = 1; depth < segments.Length; depth++)
                objectPaths.Add(BuildJsonPath(segments, depth));

            terminalPaths.Add(fullPath);
            previousSegments = segments;
        }
    }

    private static InvalidOperationException CreateForJsonPathConflictException(string propertyPath)
        => new($"Property '{propertyPath}' cannot be generated in JSON output due to a conflict with another column name or alias in the FOR JSON PATH projection order.");

    private static string BuildJsonPath(string[] segments, int length)
        => string.Join(".", segments.Take(length));

    private static void AddPathJsonProperty(
        Dictionary<string, object?> root,
        string propertyPath,
        object? value,
        bool includeNullValues,
        bool isJsonFragment = false)
    {
        if (value is null && !includeNullValues)
            return;

        var segments = propertyPath
            .Split('.')
            .Select(_ => _.Trim())
            .Where(_ => !string.IsNullOrWhiteSpace(_))
            .ToArray();
        if (segments.Length == 0)
            return;

        Dictionary<string, object?> current = root;
        for (var i = 0; i < segments.Length - 1; i++)
        {
            var segment = segments[i];
            if (!current.TryGetValue(segment, out var nestedValue) || nestedValue is not Dictionary<string, object?> nested)
            {
                nested = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                current[segment] = nested;
            }

            current = nested;
        }

        current[segments[^1]] = NormalizeForJsonValue(value, isJsonFragment);
    }

    private static void AddJsonProperty(
        Dictionary<string, object?> target,
        string propertyName,
        object? value,
        bool includeNullValues,
        bool isJsonFragment = false)
    {
        if (value is null && !includeNullValues)
            return;

        target[propertyName] = NormalizeForJsonValue(value, isJsonFragment);
    }

    private static List<AutoJsonProjection> BuildAutoJsonProjections(TableResultMock result, SqlSelectQuery query)
    {
        var projections = new List<AutoJsonProjection>(result.Columns.Count);
        for (var i = 0; i < result.Columns.Count; i++)
        {
            var qualifier = i < query.SelectItems.Count
                ? TryGetSimpleQualifiedColumnQualifier(query.SelectItems[i].Raw, query.SelectItems[i].Alias)
                : null;

            projections.Add(new AutoJsonProjection(i, qualifier, result.Columns[i].ColumnAlias, result.Columns[i].IsJsonFragment));
        }

        return projections;
    }

    private static object? NormalizeForJsonValue(object? value, bool isJsonFragment)
    {
        if (!isJsonFragment || value is null)
            return value;

        if (value is JsonElement jsonElement)
            return jsonElement.Clone();

        if (value is not string text)
            return value;

        var trimmed = text.Trim();
        if (trimmed.Length < 2 || (trimmed[0] != '{' && trimmed[0] != '['))
            return value;

        try
        {
            QueryJsonFunctionHelper.TryGetJsonRootElement(trimmed, out var root);
            return root.ValueKind is JsonValueKind.Object or JsonValueKind.Array
                ? root
                : value;
        }
        catch (JsonException)
        {
            return value;
        }
    }

    private static string? TryGetSimpleQualifiedColumnQualifier(string raw, string? alias)
    {
        var (expression, _) = SelectAliasParserHelper.SplitTrailingAsAlias(raw, alias);
        var match = Regex.Match(
            expression.Trim(),
            @"^(?<qual>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)\s*\.\s*(?<name>\[[^\]]+\]|""[^""]+""|`[^`]+`|[A-Za-z_][A-Za-z0-9_$#]*)$",
            RegexOptions.CultureInvariant);

        return match.Success
            ? match.Groups["qual"].Value.NormalizeName()
            : null;
    }


}
