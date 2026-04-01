using System.Globalization;
using System.Text.Json;
using FluentAssertions;
using FluentAssertions.Execution;

namespace DbSqlLikeMem.TestTools.Query;

internal sealed record QueryResultSnapshot
{
    public required IReadOnlyList<string> ColumnNames { get; init; }

    public required IReadOnlyList<QueryResultRowSnapshot> Rows { get; init; }
}

internal sealed record QueryResultRowSnapshot
{
    public required IReadOnlyList<object?> Values { get; init; }
}

internal static class QueryResultSnapshotReader
{
    internal static QueryResultSnapshot Capture(DbDataReader reader)
    {
        var columnNames = new string[reader.FieldCount];
        for (var i = 0; i < columnNames.Length; i++)
            columnNames[i] = reader.GetName(i);

        var rows = new List<QueryResultRowSnapshot>();
        while (reader.Read())
            rows.Add(CaptureRow(reader));

        return new QueryResultSnapshot
        {
            ColumnNames = columnNames,
            Rows = rows,
        };
    }

    internal static QueryResultRowSnapshot CaptureRow(DbDataReader reader)
    {
        var values = new object?[reader.FieldCount];
        for (var i = 0; i < values.Length; i++)
            values[i] = NormalizeValue(reader.GetValue(i));

        return new QueryResultRowSnapshot
        {
            Values = values,
        };
    }

    private static object? NormalizeValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is string or char)
            return value.ToString();

        if (value is bool booleanValue)
            return booleanValue ? 1m : 0m;

        if (value is byte[] bytes)
            return Convert.ToBase64String(bytes);

        if (value is JsonElement jsonElement)
            return NormalizeJsonElement(jsonElement);

        if (value is JsonDocument jsonDocument)
            return NormalizeJsonElement(jsonDocument.RootElement);

        if (value is DateTime dateTime)
            return dateTime.ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.ToString("O", CultureInfo.InvariantCulture);

        if (value is TimeSpan timeSpan)
            return timeSpan.ToString("c", CultureInfo.InvariantCulture);

        if (value is Guid guid)
            return guid.ToString("D", CultureInfo.InvariantCulture);

        if (value is IConvertible)
        {
            try
            {
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
            }
            catch (InvalidCastException)
            {
            }
            catch (OverflowException)
            {
            }
        }

        return value;
    }

    private static object? NormalizeJsonElement(JsonElement jsonElement)
        => jsonElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined
            ? null
            : jsonElement.GetRawText();
}

internal static class QueryResultSnapshotAssertions
{
    internal static void ShouldMatch(
        this QueryResultSnapshot actual,
        QueryResultSnapshot expected)
    {
        using var scope = new AssertionScope();

        actual.ColumnNames.Should().Equal(expected.ColumnNames, "the projected columns must stay stable");
        actual.Rows.Count.Should().Be(expected.Rows.Count, "the result set must keep the same row count");

        var rowCount = Math.Min(actual.Rows.Count, expected.Rows.Count);
        for (var rowIndex = 0; rowIndex < rowCount; rowIndex++)
        {
            var rowNumber = rowIndex + 1;
            var actualRow = actual.Rows[rowIndex];
            var expectedRow = expected.Rows[rowIndex];

            actualRow.Values.Count.Should().Be(
                expectedRow.Values.Count,
                $"row {rowNumber} must keep the same column count");

            var columnCount = Math.Min(actualRow.Values.Count, expectedRow.Values.Count);
            for (var columnIndex = 0; columnIndex < columnCount; columnIndex++)
            {
                var columnName = columnIndex < actual.ColumnNames.Count
                    ? actual.ColumnNames[columnIndex]
                    : $"column {columnIndex + 1}";

                actualRow.Values[columnIndex].Should().Be(
                    expectedRow.Values[columnIndex],
                    $"row {rowNumber}, column '{columnName}'");
            }
        }
    }
}
