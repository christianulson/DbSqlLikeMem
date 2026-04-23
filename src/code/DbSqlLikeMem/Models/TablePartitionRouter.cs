using System.Globalization;
using System.Text.RegularExpressions;

namespace DbSqlLikeMem;

internal static class TablePartitionRouter
{
    public static bool MatchesRequestedPartitions(
        TableMock table,
        IReadOnlyDictionary<int, object?> row,
        IReadOnlyCollection<string> requestedPartitionNames)
    {
        if (requestedPartitionNames.Count == 0 || string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return true;

        var routingInfo = GetPartitionRoutingInfo(table);
        if (routingInfo is null)
            return true;

        if (!routingInfo.TryGetPartitionName(row, table, out var partitionName))
            return false;

        foreach (var requestedPartitionName in requestedPartitionNames)
        {
            if (string.Equals(requestedPartitionName, partitionName, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    public static bool TryGetPartitionName(
        TableMock table,
        IReadOnlyDictionary<int, object?> row,
        out string partitionName)
    {
        partitionName = string.Empty;
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo(table);
        if (routingInfo is null)
            return false;

        return routingInfo.TryGetPartitionName(row, table, out partitionName);
    }

    public static bool TryInferRequestedPartitionNames(
        TableMock table,
        IReadOnlyDictionary<string, object?> equalsByColumn,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo(table);
        if (routingInfo is null)
            return false;

        if (!equalsByColumn.TryGetValue(routingInfo.PartitionedColumnName, out var rawValue))
            return false;

        return TryInferRequestedPartitionNames(table, [rawValue], out partitionNames);
    }

    public static bool TryInferRequestedPartitionNames(
        TableMock table,
        IEnumerable<object?> rawValues,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        if (GetPartitionRoutingInfo(table) is null)
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var hasValue = false;
        foreach (var rawValue in rawValues)
        {
            hasValue = true;
            if (!TryGetPartitionNameForValue(table, rawValue, out var partitionName))
                return false;

            distinctPartitionNames.Add(partitionName);
        }

        if (!hasValue || distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    public static bool TryInferRequestedPartitionNamesForRange(
        TableMock table,
        object? lowValue,
        object? highValue,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        if (!TryGetYearForPartitionValue(lowValue, out var lowYear)
            || !TryGetYearForPartitionValue(highValue, out var highYear))
        {
            return false;
        }

        if (lowYear > highYear)
            return false;

        var span = highYear - lowYear;
        if (span > 32)
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        for (var year = lowYear; year <= highYear; year++)
        {
            if (!TryGetPartitionNameForValue(table, year, out var partitionName))
                return false;

            distinctPartitionNames.Add(partitionName);
        }

        if (distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    public static bool TryInferRequestedPartitionNamesForRanges(
        TableMock table,
        IEnumerable<(object? Low, object? High)> ranges,
        out IReadOnlyList<string> partitionNames)
    {
        partitionNames = [];
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        var distinctPartitionNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var hasRange = false;
        foreach (var (low, high) in ranges)
        {
            hasRange = true;
            if (!TryInferRequestedPartitionNamesForRange(table, low, high, out var rangePartitionNames))
                return false;

            foreach (var partitionName in rangePartitionNames)
                distinctPartitionNames.Add(partitionName);
        }

        if (!hasRange || distinctPartitionNames.Count == 0)
            return false;

        partitionNames = [.. distinctPartitionNames];
        return true;
    }

    public static bool TryGetPartitionNameForValue(
        TableMock table,
        object? rawValue,
        out string partitionName)
    {
        partitionName = string.Empty;
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo(table);
        if (routingInfo is null)
            return false;

        if (rawValue is null || rawValue is DBNull)
            return false;

        if (!table.ColumnsRaw.TryGetValue(routingInfo.PartitionedColumnName, out var partitionedColumn))
            return false;

        var probeRow = new Dictionary<int, object?>(1)
        {
            [partitionedColumn.Index] = rawValue
        };

        return routingInfo.TryGetPartitionName(probeRow, table, out partitionName);
    }

    public static bool TryGetPartitionedColumnName(
        TableMock table,
        out string partitionedColumnName)
    {
        partitionedColumnName = string.Empty;
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return false;

        var routingInfo = GetPartitionRoutingInfo(table);
        if (routingInfo is null)
            return false;

        partitionedColumnName = routingInfo.PartitionedColumnName;
        return true;
    }

    private static PartitionRoutingInfo? GetPartitionRoutingInfo(TableMock table)
    {
        if (string.IsNullOrWhiteSpace(table.PartitionClauseSql))
            return null;

        return PartitionRoutingInfo.TryParse(table.PartitionClauseSql!);
    }

    private static bool TryGetYearForPartitionValue(object? rawValue, out int year)
    {
        switch (rawValue)
        {
            case DateTime dateTime:
                year = dateTime.Year;
                return true;
            case DateTimeOffset dateTimeOffset:
                year = dateTimeOffset.Year;
                return true;
            case int intValue:
                year = intValue;
                return true;
            case short shortValue:
                year = shortValue;
                return true;
            case sbyte sbyteValue:
                year = sbyteValue;
                return true;
            case byte byteValue:
                year = byteValue;
                return true;
            case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                year = (int)longValue;
                return true;
            case uint uintValue when uintValue <= int.MaxValue:
                year = (int)uintValue;
                return true;
            case ulong ulongValue when ulongValue <= int.MaxValue:
                year = (int)ulongValue;
                return true;
            case decimal decimalValue when decimalValue >= int.MinValue && decimalValue <= int.MaxValue:
                year = (int)decimalValue;
                return true;
            case double doubleValue when doubleValue >= int.MinValue && doubleValue <= int.MaxValue:
                year = (int)doubleValue;
                return true;
            case float floatValue when floatValue >= int.MinValue && floatValue <= int.MaxValue:
                year = (int)floatValue;
                return true;
            case string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate):
                year = parsedDate.Year;
                return true;
            default:
                year = default;
                return false;
        }
    }

    private sealed record PartitionRoutingInfo(
        string PartitionedColumnName,
        PartitionPartitionKind Kind,
        IReadOnlyList<PartitionPartitionItem> Partitions)
    {
        internal static PartitionRoutingInfo? TryParse(string partitionClauseSql)
        {
            var rangeMatch = Regex.Match(
                partitionClauseSql,
                @"PARTITION\s+BY\s+RANGE\s*\(\s*YEAR\s*\(\s*`?(?<column>[A-Za-z0-9_]+)`?\s*\)\s*\)\s*\((?<parts>[\s\S]+)\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (rangeMatch.Success)
            {
                var partitionColumn = rangeMatch.Groups["column"].Value.NormalizeName();
                if (string.IsNullOrWhiteSpace(partitionColumn))
                    return null;

                var partitions = new List<PartitionPartitionItem>();
                foreach (var part in SplitTopLevelPartitions(rangeMatch.Groups["parts"].Value))
                {
                    var item = ParseRangePartitionItem(part);
                    if (item is null)
                        return null;

                    partitions.Add(item);
                }

                if (partitions.Count == 0)
                    return null;

                return new PartitionRoutingInfo(partitionColumn, PartitionPartitionKind.Range, partitions);
            }

            var listMatch = Regex.Match(
                partitionClauseSql,
                @"PARTITION\s+BY\s+LIST\s*\(\s*YEAR\s*\(\s*`?(?<column>[A-Za-z0-9_]+)`?\s*\)\s*\)\s*\((?<parts>[\s\S]+)\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!listMatch.Success)
                return null;

            var listPartitionColumn = listMatch.Groups["column"].Value.NormalizeName();
            if (string.IsNullOrWhiteSpace(listPartitionColumn))
                return null;

            var listPartitions = new List<PartitionPartitionItem>();
            foreach (var part in SplitTopLevelPartitions(listMatch.Groups["parts"].Value))
            {
                var item = ParseListPartitionItem(part);
                if (item is null)
                    return null;

                listPartitions.Add(item);
            }

            if (listPartitions.Count == 0)
                return null;

            return new PartitionRoutingInfo(listPartitionColumn, PartitionPartitionKind.List, listPartitions);
        }

        internal bool TryGetPartitionName(
            IReadOnlyDictionary<int, object?> row,
            TableMock table,
            out string partitionName)
        {
            partitionName = string.Empty;
            if (!table.ColumnsRaw.TryGetValue(PartitionedColumnName, out var column))
                return false;

            if (!row.TryGetValue(column.Index, out var rawValue) || rawValue is null || rawValue is DBNull)
                return false;

            if (!TryGetYear(rawValue, out var year))
                return false;

            foreach (var partition in Partitions)
            {
                if (Kind == PartitionPartitionKind.Range)
                {
                    if (partition.MaxValue)
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    if (year < partition.Value)
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    continue;
                }

                if (partition.ListValues is not null)
                {
                    if (partition.ListValues.Contains(year))
                    {
                        partitionName = partition.Name;
                        return true;
                    }

                    continue;
                }

                if (partition.Value == year)
                {
                    partitionName = partition.Name;
                    return true;
                }
            }

            return false;
        }

        private static IEnumerable<string> SplitTopLevelPartitions(string partsSql)
        {
            var start = 0;
            var depth = 0;
            for (var i = 0; i < partsSql.Length; i++)
            {
                var ch = partsSql[i];
                if (ch == '(')
                {
                    depth++;
                    continue;
                }

                if (ch == ')')
                {
                    if (depth > 0)
                        depth--;
                    continue;
                }

                if (ch == ',' && depth == 0)
                {
                    var slice = partsSql[start..i].Trim();
                    if (!string.IsNullOrWhiteSpace(slice))
                        yield return slice;
                    start = i + 1;
                }
            }

            var last = partsSql[start..].Trim();
            if (!string.IsNullOrWhiteSpace(last))
                yield return last;
        }

        private static PartitionPartitionItem? ParseRangePartitionItem(string partSql)
        {
            var match = Regex.Match(
                partSql,
                @"^\s*PARTITION\s+`?(?<name>[A-Za-z0-9_]+)`?\s+VALUES\s+LESS\s+THAN\s*(?:\(\s*(?<bound>MAXVALUE|-?\d+)\s*\)|(?<bound>MAXVALUE|-?\d+))\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!match.Success)
                return null;

            var name = match.Groups["name"].Value.NormalizeName();
            var boundRaw = match.Groups["bound"].Value.Trim();
            if (string.Equals(boundRaw, "MAXVALUE", StringComparison.OrdinalIgnoreCase))
                return new PartitionPartitionItem(name, 0, MaxValue: true);

            if (!int.TryParse(boundRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var upperBound))
                return null;

            return new PartitionPartitionItem(name, upperBound, MaxValue: false);
        }

        private static PartitionPartitionItem? ParseListPartitionItem(string partSql)
        {
            var match = Regex.Match(
                partSql,
                @"^\s*PARTITION\s+`?(?<name>[A-Za-z0-9_]+)`?\s+VALUES\s+IN\s*\(\s*(?<values>[\s\S]+?)\s*\)\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);
            if (!match.Success)
                return null;

            var name = match.Groups["name"].Value.NormalizeName();
            if (string.IsNullOrWhiteSpace(name))
                return null;

            var valuesSql = match.Groups["values"].Value;
            var parsedValues = new List<int>();
            var hasValue = false;
            foreach (var rawValue in SplitTopLevelCsv(valuesSql))
            {
                hasValue = true;
                if (!int.TryParse(rawValue.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var value))
                    return null;

                parsedValues.Add(value);
            }

            if (!hasValue)
                return null;

            return new PartitionPartitionItem(name, 0, MaxValue: false, parsedValues);
        }

        private static bool TryGetYear(object rawValue, out int year)
        {
            switch (rawValue)
            {
                case DateTime dateTime:
                    year = dateTime.Year;
                    return true;
                case DateTimeOffset dateTimeOffset:
                    year = dateTimeOffset.Year;
                    return true;
                case int intValue:
                    year = intValue;
                    return true;
                case short shortValue:
                    year = shortValue;
                    return true;
                case sbyte sbyteValue:
                    year = sbyteValue;
                    return true;
                case byte byteValue:
                    year = byteValue;
                    return true;
                case long longValue when longValue >= int.MinValue && longValue <= int.MaxValue:
                    year = (int)longValue;
                    return true;
                case uint uintValue when uintValue <= int.MaxValue:
                    year = (int)uintValue;
                    return true;
                case ulong ulongValue when ulongValue <= int.MaxValue:
                    year = (int)ulongValue;
                    return true;
                case decimal decimalValue when decimalValue >= int.MinValue && decimalValue <= int.MaxValue:
                    year = (int)decimalValue;
                    return true;
                case double doubleValue when doubleValue >= int.MinValue && doubleValue <= int.MaxValue:
                    year = (int)doubleValue;
                    return true;
                case float floatValue when floatValue >= int.MinValue && floatValue <= int.MaxValue:
                    year = (int)floatValue;
                    return true;
                case string text when DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var parsedDate):
                    year = parsedDate.Year;
                    return true;
                default:
                    year = default;
                    return false;
            }
        }
    }

    private static IEnumerable<string> SplitTopLevelCsv(string partsSql)
    {
        var start = 0;
        var depth = 0;
        for (var i = 0; i < partsSql.Length; i++)
        {
            var ch = partsSql[i];
            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                if (depth > 0)
                    depth--;
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                var slice = partsSql[start..i].Trim();
                if (!string.IsNullOrWhiteSpace(slice))
                    yield return slice;
                start = i + 1;
            }
        }

        var last = partsSql[start..].Trim();
        if (!string.IsNullOrWhiteSpace(last))
            yield return last;
    }

    private enum PartitionPartitionKind
    {
        Range,
        List
    }

    private sealed record PartitionPartitionItem(string Name, int Value, bool MaxValue, IReadOnlyList<int>? ListValues = null);
}
