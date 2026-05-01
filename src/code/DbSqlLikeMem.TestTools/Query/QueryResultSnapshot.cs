using System.Text.Json;
using FluentAssertions.Execution;

namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Captures the normalized column names and rows returned by a query.
/// PT-br: Captura os nomes de colunas normalizados e as linhas retornadas por uma consulta.
/// </summary>
public sealed record QueryResultSnapshot
{
    /// <summary>
    /// EN: Gets the normalized column names captured from the reader.
    /// PT-br: Obtém os nomes de colunas normalizados capturados do reader.
    /// </summary>
    public required IReadOnlyList<string> ColumnNames { get; init; }

    /// <summary>
    /// EN: Gets the captured rows in result-set order.
    /// PT-br: Obtém as linhas capturadas na ordem do conjunto de resultados.
    /// </summary>
    public required IReadOnlyList<QueryResultRowSnapshot> Rows { get; init; }
}

/// <summary>
/// EN: Captures the normalized values of a single result-set row.
/// PT-br: Captura os valores normalizados de uma unica linha do conjunto de resultados.
/// </summary>
public sealed record QueryResultRowSnapshot
{
    /// <summary>
    /// EN: Gets the normalized values captured for the row.
    /// PT-br: Obtém os valores normalizados capturados para a linha.
    /// </summary>
    public required IReadOnlyList<object?> Values { get; init; }
}

internal static class QueryResultSnapshotReader
{
    internal static QueryResultSnapshot Capture(DbDataReader reader)
    {
        var columnNames = new string[reader.FieldCount];
        for (var i = 0; i < columnNames.Length; i++)
            columnNames[i] = NormalizeCapturedColumnName(reader.GetName(i), reader);

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
            values[i] = NormalizeValue(reader.GetValue(i), reader.GetName(i));

        return new QueryResultRowSnapshot
        {
            Values = values,
        };
    }

    private static object? NormalizeValue(object? value, string columnName)
    {
        if (value is null || value is DBNull)
            return null;

        if (IsDateOnlyColumn(columnName))
            return NormalizeDateOnlyValue(value);

        if (IsGuidColumn(columnName))
            return NormalizeGuidValue(value);

        if (IsTimeOnlyColumn(columnName))
            return NormalizeTimeOnlyValue(value);

        if (IsDateTimeOffsetColumn(columnName))
            return NormalizeDateTimeOffsetColumnValue(value);

        if (IsDateTimeLikeColumn(columnName))
            return NormalizeDateTimeOffsetColumnValue(value);

        if (IsAmountTextColumn(columnName))
            return NormalizeAmountTextValue(value);

        if (value is string or char)
        {
            var text = value.ToString();
            if (!string.IsNullOrEmpty(text))
            {
                if (IsDateTimeLikeColumn(columnName))
                {
                    if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                        return new DateTimeOffset(RoundToNearestSecond(dateTimeOffsetValue.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

                    if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                        return RoundToNearestSecond(DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Unspecified)).ToString("O", CultureInfo.InvariantCulture);
                }
            }

            return text;
        }

        if (value is bool booleanValue)
            return booleanValue ? 1m : 0m;

        if (value is byte[] bytes)
            return Convert.ToBase64String(bytes);

        if (value is JsonElement jsonElement)
            return NormalizeJsonElement(jsonElement);

        if (value is JsonDocument jsonDocument)
            return NormalizeJsonElement(jsonDocument.RootElement);

        if (value is DateTime dateTime)
            return RoundToNearestSecond(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified)).ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return new DateTimeOffset(RoundToNearestSecond(dateTimeOffset.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

        if (value is TimeSpan timeSpan)
            return timeSpan.ToString("c", CultureInfo.InvariantCulture);

        if (TryNormalizeTimeOnlyValue(value, out var timeSpanValue))
            return timeSpanValue.ToString("c", CultureInfo.InvariantCulture);

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

    private static object? NormalizeAmountTextValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is string or char)
        {
            var text = value.ToString();
            if (!string.IsNullOrWhiteSpace(text))
            {
                if (decimal.TryParse(text, NumberStyles.Number, CultureInfo.InvariantCulture, out var decimalValue))
                    return decimalValue.ToString("0.00", CultureInfo.InvariantCulture);
            }

            return text;
        }

        if (value is IConvertible)
        {
            try
            {
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString("0.00", CultureInfo.InvariantCulture);
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

    private static object? NormalizeDateOnlyValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is string or char)
        {
            var text = value.ToString();
            if (!string.IsNullOrEmpty(text))
            {
                if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                    return dateTimeOffsetValue.DateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

                if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                    return dateTimeValue.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
            }

            return text;
        }

        if (value is DateTime dateTime)
            return dateTime.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.DateTime.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (TryNormalizeDateOnlyValue(value, out var normalizedDateTime))
            return normalizedDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        return value;
    }

    private static string NormalizeCapturedColumnName(string columnName, DbDataReader reader)
    {
        var readerTypeName = reader.GetType().FullName ?? string.Empty;
        if (readerTypeName.Contains("DbSqlLikeMem.Npgsql", StringComparison.Ordinal)
            || readerTypeName.Contains("Npgsql.", StringComparison.Ordinal))
        {
            return columnName.ToLowerInvariant();
        }

        if (readerTypeName.Contains("DbSqlLikeMem.Firebird", StringComparison.Ordinal)
            || readerTypeName.Contains("FirebirdSql.Data.FirebirdClient", StringComparison.Ordinal))
            return columnName.ToUpperInvariant();

        if (readerTypeName.Contains("DbSqlLikeMem.Oracle", StringComparison.Ordinal)
            || readerTypeName.Contains("DbSqlLikeMem.Db2", StringComparison.Ordinal))
            return columnName.ToUpperInvariant();

        return columnName;
    }

    private static bool IsDateOnlyColumn(string columnName)
        => columnName.Contains("BirthDate", StringComparison.OrdinalIgnoreCase);

    private static bool IsGuidColumn(string columnName)
        => columnName.Contains("Guid", StringComparison.OrdinalIgnoreCase);

    private static bool IsTimeOnlyColumn(string columnName)
        => columnName.Contains("TimeValue", StringComparison.OrdinalIgnoreCase);

    private static bool IsDateTimeOffsetColumn(string columnName)
        => columnName.Contains("DateTimeOffsetValue", StringComparison.OrdinalIgnoreCase);

    private static bool IsDateTimeLikeColumn(string columnName)
        => columnName.EndsWith("At", StringComparison.OrdinalIgnoreCase)
            || columnName.Contains("DateTime", StringComparison.OrdinalIgnoreCase);

    private static bool IsAmountTextColumn(string columnName)
        => columnName.Contains("AmountText", StringComparison.OrdinalIgnoreCase);

    private static object? NormalizeTimeOnlyValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is TimeSpan timeSpan)
            return timeSpan.ToString("c", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

        if (value is DateTime dateTime)
            return dateTime.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

        if (value is string or char)
        {
            var text = value.ToString();
            if (!string.IsNullOrEmpty(text))
            {
                if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var timeSpanValue))
                    return timeSpanValue.ToString("c", CultureInfo.InvariantCulture);

                if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                    return dateTimeOffsetValue.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

                if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                    return dateTimeValue.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);
            }

            return text;
        }

        if (TryNormalizeTimeOnlyValue(value, out var normalizedTime))
            return normalizedTime.ToString("c", CultureInfo.InvariantCulture);

        return value;
    }

    private static object? NormalizeDateTimeOffsetColumnValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is DateTimeOffset dateTimeOffset)
            return new DateTimeOffset(RoundToNearestSecond(dateTimeOffset.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTime dateTime)
            return new DateTimeOffset(RoundToNearestSecond(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified)), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

        if (value is string or char)
        {
            var text = value.ToString();
            if (!string.IsNullOrEmpty(text))
            {
                if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                    return new DateTimeOffset(RoundToNearestSecond(dateTimeOffsetValue.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

                if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                    return new DateTimeOffset(RoundToNearestSecond(DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Unspecified)), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);
            }

            return text;
        }

        return value;
    }

    private static string? NormalizeGuidValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is Guid guid)
            return guid.ToString("D", CultureInfo.InvariantCulture).ToLowerInvariant();

        return Convert.ToString(value, CultureInfo.InvariantCulture)?.ToLowerInvariant();
    }

    private static bool TryNormalizeTimeOnlyValue(object? value, out TimeSpan timeSpan)
    {
        timeSpan = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.TimeOnly", StringComparison.Ordinal))
            return false;

        if (type.GetMethod("ToTimeSpan", Type.EmptyTypes)?.Invoke(value, null) is not TimeSpan normalized)
            return false;

        timeSpan = normalized;
        return true;
    }

    private static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }

    private static DateTime RoundToNearestSecond(DateTime value)
    {
        var ticks = value.Ticks;
        var roundedTicks = ((ticks + (TimeSpan.TicksPerSecond / 2)) / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond;
        return new DateTime(roundedTicks, DateTimeKind.Unspecified);
    }
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

                var actualValue = NormalizeComparisonValue(columnName, actualRow.Values[columnIndex]);
                var expectedValue = NormalizeComparisonValue(columnName, expectedRow.Values[columnIndex]);

                actualValue.Should().Be(
                    expectedValue,
                    $"row {rowNumber}, column '{columnName}'");
            }
        }
    }

    private static object? NormalizeComparisonValue(string columnName, object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (IsDateOnlyColumn(columnName))
            return NormalizeDateOnlyComparisonValue(value);

        if (IsGuidColumn(columnName))
            return NormalizeGuidComparisonValue(value);

        if (IsTimeOnlyColumn(columnName))
            return NormalizeTimeOnlyComparisonValue(value);

        if (columnName.Contains("DateTimeOffset", StringComparison.OrdinalIgnoreCase))
            return NormalizeDateTimeOffsetComparisonValue(value);

        if (IsDateTimeLikeColumn(columnName))
            return NormalizeDateTimeOffsetComparisonValue(value);

        if (value is string or char)
            return NormalizeDateTimeComparisonText(value.ToString());

        if (value is DateTime dateTime)
            return DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified).ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.DateTime.ToString("O", CultureInfo.InvariantCulture);

        if (TryNormalizeTimeOnlyValue(value, out var timeSpanValue))
            return timeSpanValue.ToString("c", CultureInfo.InvariantCulture);

        if (value is bool booleanValue)
            return booleanValue ? 1m : 0m;

        if (value is byte[] bytes)
            return Convert.ToBase64String(bytes);

        if (value is JsonElement jsonElement)
            return NormalizeJsonElement(jsonElement);

        if (value is JsonDocument jsonDocument)
            return NormalizeJsonElement(jsonDocument.RootElement);

        if (value is TimeSpan timeSpan)
            return timeSpan.ToString("c", CultureInfo.InvariantCulture);

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

    private static string? NormalizeGuidComparisonValue(object? value)
        => NormalizeGuidValue(value);

    private static object? NormalizeDateTimeComparisonText(string? text)
    {
        if (string.IsNullOrEmpty(text))
            return text;

        if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
            return RoundToNearestSecond(DateTime.SpecifyKind(dateTimeOffsetValue.DateTime, DateTimeKind.Unspecified)).ToString("O", CultureInfo.InvariantCulture);

        if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
            return RoundToNearestSecond(DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Unspecified)).ToString("O", CultureInfo.InvariantCulture);

        return text;
    }

    private static object? NormalizeDateTimeOffsetComparisonValue(object value)
    {
        if (value is DateTimeOffset dateTimeOffset)
            return new DateTimeOffset(RoundToNearestSecond(dateTimeOffset.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

        if (value is DateTime dateTime)
            return new DateTimeOffset(RoundToNearestSecond(DateTime.SpecifyKind(dateTime, DateTimeKind.Unspecified)), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

        if (value is string or char)
        {
            var text = value.ToString();
            if (string.IsNullOrEmpty(text))
                return text;

            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                return new DateTimeOffset(RoundToNearestSecond(dateTimeOffsetValue.DateTime), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                return new DateTimeOffset(RoundToNearestSecond(DateTime.SpecifyKind(dateTimeValue, DateTimeKind.Unspecified)), TimeSpan.Zero).ToString("O", CultureInfo.InvariantCulture);

            return text;
        }

        return NormalizeComparisonValue(string.Empty, value);
    }

    private static object? NormalizeTimeOnlyComparisonValue(object value)
    {
        if (value is TimeSpan timeSpan)
            return timeSpan.ToString("c", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

        if (value is DateTime dateTime)
            return dateTime.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

        if (value is string or char)
        {
            var text = value.ToString();
            if (string.IsNullOrEmpty(text))
                return text;

            if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var timeSpanValue))
                return timeSpanValue.ToString("c", CultureInfo.InvariantCulture);

            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                return dateTimeOffsetValue.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                return dateTimeValue.TimeOfDay.ToString("c", CultureInfo.InvariantCulture);

            return text;
        }

        if (TryNormalizeTimeOnlyValue(value, out var normalizedTime))
            return normalizedTime.ToString("c", CultureInfo.InvariantCulture);

        return NormalizeComparisonValue(string.Empty, value);
    }

    private static object? NormalizeJsonElement(JsonElement jsonElement)
        => jsonElement.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined
            ? null
            : jsonElement.GetRawText();

    private static bool IsDateOnlyColumn(string columnName)
        => columnName.Contains("BirthDate", StringComparison.OrdinalIgnoreCase);

    private static bool IsGuidColumn(string columnName)
        => columnName.Contains("Guid", StringComparison.OrdinalIgnoreCase);

    private static bool IsTimeOnlyColumn(string columnName)
        => columnName.Contains("TimeValue", StringComparison.OrdinalIgnoreCase);

    private static bool IsDateTimeLikeColumn(string columnName)
        => columnName.EndsWith("At", StringComparison.OrdinalIgnoreCase)
            || columnName.Contains("DateTime", StringComparison.OrdinalIgnoreCase);

    private static string? NormalizeGuidValue(object? value)
    {
        if (value is null || value is DBNull)
            return null;

        if (value is Guid guid)
            return guid.ToString("D", CultureInfo.InvariantCulture).ToLowerInvariant();

        return Convert.ToString(value, CultureInfo.InvariantCulture)?.ToLowerInvariant();
    }

    private static object? NormalizeDateOnlyComparisonValue(object value)
    {
        if (value is string or char)
        {
            var text = value.ToString();
            if (string.IsNullOrEmpty(text))
                return text;

            if (DateTimeOffset.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeOffsetValue))
                return dateTimeOffsetValue.DateTime.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

            if (DateTime.TryParse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var dateTimeValue))
                return dateTimeValue.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

            return text;
        }

        if (value is DateTime dateTime)
            return dateTime.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (value is DateTimeOffset dateTimeOffset)
            return dateTimeOffset.DateTime.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        if (TryNormalizeDateOnlyValue(value, out var normalizedDateTime))
            return normalizedDateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);

        return NormalizeComparisonValue(string.Empty, value);
    }

    private static bool TryNormalizeTimeOnlyValue(object? value, out TimeSpan timeSpan)
    {
        timeSpan = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.TimeOnly", StringComparison.Ordinal))
            return false;

        if (type.GetMethod("ToTimeSpan", Type.EmptyTypes)?.Invoke(value, null) is not TimeSpan normalized)
            return false;

        timeSpan = normalized;
        return true;
    }

    private static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }

    private static DateTime RoundToNearestSecond(DateTime value)
    {
        var ticks = value.Ticks;
        var roundedTicks = ((ticks + (TimeSpan.TicksPerSecond / 2)) / TimeSpan.TicksPerSecond) * TimeSpan.TicksPerSecond;
        return new DateTime(roundedTicks, DateTimeKind.Unspecified);
    }
}

