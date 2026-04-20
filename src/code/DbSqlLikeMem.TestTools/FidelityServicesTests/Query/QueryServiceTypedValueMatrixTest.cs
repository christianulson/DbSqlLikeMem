namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest
{
    /// <summary>
    /// EN: Inserts text, boolean, integer, exact and approximate numeric, fixed-length text, bigint, GUID, binary, time, DateTimeOffset, and boundary values into the users table and reads them back for fidelity checks.
    /// PT: Insere valores de texto, booleano, inteiro, numerico exato e aproximado, texto de tamanho fixo, bigint, GUID, binario, time, DateTimeOffset e valores de borda na tabela de usuarios e os le de volta para verificacoes de fidelidade.
    /// </summary>
    internal async Task<object?> RunTypedFieldStorageMatrixAsync(params object[] pars)
    {
        var firstCreatedAt = new DateTime(2026, 1, 15, 10, 30, 0, DateTimeKind.Utc);
        var firstUpdatedAt = new DateTime(2026, 1, 16, 11, 45, 0, DateTimeKind.Utc);
        var firstBirthDate = new DateTime(2026, 1, 18);
        var firstTimeValue = TimeSpan.FromHours(13.5);
        var firstDateTimeOffsetValue = new DateTimeOffset(2026, 1, 19, 8, 15, 30, TimeSpan.FromHours(-3));
        var firstExpectedDateTimeOffsetValue = NormalizeStoredDateTimeOffsetValue(firstDateTimeOffsetValue);
        var secondCreatedAt = new DateTime(2026, 1, 17, 9, 15, 0, DateTimeKind.Utc);
        var thirdCreatedAt = new DateTime(2026, 1, 20, 23, 59, 59, DateTimeKind.Utc);
        var thirdUpdatedAt = new DateTime(2026, 1, 21, 0, 0, 0, DateTimeKind.Utc);
        var thirdBirthDate = new DateTime(2024, 2, 29);
        var thirdTimeValue = new TimeSpan(23, 59, 59);
        var thirdDateTimeOffsetValue = new DateTimeOffset(2026, 1, 21, 10, 30, 45, TimeSpan.FromHours(5.5));
        var thirdExpectedDateTimeOffsetValue = NormalizeStoredDateTimeOffsetValue(thirdDateTimeOffsetValue);
        var thirdName = new string('N', 100);
        var thirdEmail = new string('E', 150);
        var thirdExpectedPrecisionValue = Repo.Dialect.Provider == ProviderId.Sqlite
            ? 100000000000000m
            : 99999999999999.9999m;
        var thirdBinaryValue = new byte[]
        {
            0x00, 0x01, 0x02, 0x03,
            0x04, 0x05, 0x06, 0x07,
            0x08, 0x09, 0x0A, 0x0B,
            0x0C, 0x0D, 0x0E, 0x0F,
        };
        var fourthCreatedAt = new DateTime(2026, 1, 22, 6, 45, 0, DateTimeKind.Utc);
        var fourthEmail = string.Empty;
        var fourthExpectedEmail = NormalizeOracleNullableText(fourthEmail);
        var fourthFixedCode = string.Empty;
        var fourthExpectedFixedCode = NormalizeOracleNullableText(fourthFixedCode);
        var fourthBinaryValue = Array.Empty<byte>();
        var fourthDateTimeOffsetValue = new DateTimeOffset(2026, 1, 22, 0, 0, 0, TimeSpan.Zero);
        var fourthExpectedDateTimeOffsetValue = NormalizeStoredDateTimeOffsetValue(fourthDateTimeOffsetValue);

        InsertTypedStorageRow(
            1,
            "Alice",
            "alice@example.com",
            true,
            (short)31,
            123.45m,
            firstCreatedAt,
            firstUpdatedAt,
            firstBirthDate,
            "AB",
            1234567890123L,
            9876.5432m,
            12.5d,
            Guid.Parse("11111111-2222-3333-4444-555555555555"),
            new byte[] { 1, 2, 3, 4 },
            firstTimeValue,
            firstDateTimeOffsetValue);
        InsertTypedStorageRow(
            2,
            "Bob",
            null,
            false,
            null,
            0.00m,
            secondCreatedAt,
            null,
            null,
            null,
            -42L,
            null,
            null,
            null,
            null,
            null,
            null);
        InsertTypedStorageRow(
            3,
            thirdName,
            thirdEmail,
            true,
            short.MaxValue,
            9999999999.99m,
            thirdCreatedAt,
            thirdUpdatedAt,
            thirdBirthDate,
            "ZZ",
            long.MaxValue,
            99999999999999.9999m,
            1234567.5d,
            Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
            thirdBinaryValue,
            thirdTimeValue,
            thirdDateTimeOffsetValue);
        InsertTypedStorageRow(
            4,
            "Delta",
            fourthEmail,
            false,
            (short)0,
            1.00m,
            fourthCreatedAt,
            null,
            null,
            fourthFixedCode,
            0L,
            0.0001m,
            0.0d,
            Guid.Empty,
            fourthBinaryValue,
            TimeSpan.Zero,
            fourthDateTimeOffsetValue);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    BirthDate,
    TRIM(FixedCode) AS FixedCode,
    BigCount,
    PrecisionValue,
    DoubleValue,
    GuidValue,
    BinaryValue,
    TimeValue,
    DateTimeOffsetValue
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(4);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedStorageRow(reader, Repo.Dialect.Provider, 1, "Alice", "alice@example.com", true, 31, 123.45m, firstCreatedAt, firstUpdatedAt, firstBirthDate, "AB", 1234567890123L, 9876.5432m, 12.5d, Guid.Parse("11111111-2222-3333-4444-555555555555"), new byte[] { 1, 2, 3, 4 }, firstTimeValue, firstExpectedDateTimeOffsetValue));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedStorageRow(reader, Repo.Dialect.Provider, 2, "Bob", null, false, null, 0.00m, secondCreatedAt, null, null, null, -42L, null, null, null, null, null, null));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedStorageRow(reader, Repo.Dialect.Provider, 3, thirdName, thirdEmail, true, short.MaxValue, 9999999999.99m, thirdCreatedAt, thirdUpdatedAt, thirdBirthDate, "ZZ", long.MaxValue, thirdExpectedPrecisionValue, 1234567.5d, Guid.Parse("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"), thirdBinaryValue, thirdTimeValue, thirdExpectedDateTimeOffsetValue));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedStorageRow(reader, Repo.Dialect.Provider, 4, "Delta", fourthExpectedEmail, false, 0, 1.00m, fourthCreatedAt, null, null, fourthExpectedFixedCode, 0L, 0.0001m, 0.0d, Guid.Empty, fourthBinaryValue, TimeSpan.Zero, fourthExpectedDateTimeOffsetValue));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "Name", "Email", "IsActive", "Age", "Balance", "CreatedAt", "UpdatedAt", "BirthDate", "FixedCode", "BigCount", "PrecisionValue", "DoubleValue", "GuidValue", "BinaryValue", "TimeValue", "DateTimeOffsetValue"],
            Rows = rows,
        };
    }

    private void InsertTypedStorageRow(
        int id,
        string name,
        string? email,
        bool isActive,
        short? age,
        decimal balance,
        DateTime createdAt,
        DateTime? updatedAt,
        DateTime? birthDate,
        string? fixedCode,
        long bigCount,
        decimal? precisionValue,
        double? doubleValue,
        Guid? guidValue,
        byte[]? binaryValue,
        TimeSpan? timeValue,
        DateTimeOffset? dateTimeOffsetValue)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    BirthDate,
    FixedCode,
    BigCount,
    PrecisionValue,
    DoubleValue,
    GuidValue,
    BinaryValue,
    TimeValue,
    DateTimeOffsetValue
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")},
    {Repo.Dialect.Parameter("email")},
    {Repo.Dialect.Parameter("isActive")},
    {Repo.Dialect.Parameter("age")},
    {Repo.Dialect.Parameter("balance")},
    {Repo.Dialect.Parameter("createdAt")},
    {Repo.Dialect.Parameter("updatedAt")},
    {Repo.Dialect.Parameter("birthDate")},
    {Repo.Dialect.Parameter("fixedCode")},
    {Repo.Dialect.Parameter("bigCount")},
    {Repo.Dialect.Parameter("precisionValue")},
    {Repo.Dialect.Parameter("doubleValue")},
    {Repo.Dialect.Parameter("guidValue")},
    {Repo.Dialect.Parameter("binaryValue")},
    {Repo.Dialect.Parameter("timeValue")},
    {Repo.Dialect.Parameter("dateTimeOffsetValue")}
)
""";

        AddParameter(command, "id", DbType.Int32, id);
        AddParameter(command, "name", DbType.String, name);
        AddParameter(command, "email", DbType.AnsiString, email is null ? DBNull.Value : email);
        AddParameter(command, "isActive", DbType.Boolean, isActive);
        AddParameter(command, "age", DbType.Int16, age is null ? DBNull.Value : age);
        AddParameter(command, "balance", DbType.Decimal, balance);
        AddParameter(command, "createdAt", DbType.DateTime, NormalizeNpgsqlDateTimeInput(createdAt));
        AddParameter(command, "updatedAt", DbType.DateTime, updatedAt is null ? DBNull.Value : NormalizeNpgsqlDateTimeInput(updatedAt.Value));
        AddParameter(command, "birthDate", DbType.Date, birthDate is null ? DBNull.Value : birthDate.Value);
        AddParameter(command, "fixedCode", DbType.AnsiStringFixedLength, fixedCode is null ? DBNull.Value : fixedCode);
        AddParameter(command, "bigCount", DbType.Int64, bigCount);
        AddParameter(command, "precisionValue", DbType.Decimal, precisionValue is null ? DBNull.Value : precisionValue.Value);
        AddParameter(command, "doubleValue", DbType.Double, doubleValue is null ? DBNull.Value : doubleValue.Value);
        AddParameter(command, "guidValue", DbType.Guid, guidValue is null ? DBNull.Value : guidValue.Value);
        AddParameter(command, "binaryValue", DbType.Binary, binaryValue is null ? DBNull.Value : binaryValue);
        AddParameter(command, "timeValue", DbType.Time, timeValue is null ? DBNull.Value : timeValue.Value);
        var supportsNativeDateTimeOffsetStorage = SupportsNativeDateTimeOffsetStorage();
        AddParameter(
            command,
            "dateTimeOffsetValue",
            supportsNativeDateTimeOffsetStorage ? DbType.DateTimeOffset : DbType.AnsiString,
            dateTimeOffsetValue is null
                ? DBNull.Value
                : supportsNativeDateTimeOffsetStorage
                    ? Repo.Dialect.Provider == ProviderId.Npgsql
                        ? dateTimeOffsetValue.Value.ToUniversalTime()
                        : dateTimeOffsetValue.Value
                    : dateTimeOffsetValue.Value.ToString("O", CultureInfo.InvariantCulture));

        command.ExecuteNonQuery();
    }

    private static QueryResultRowSnapshot ValidateTypedStorageRow(
        DbDataReader reader,
        ProviderId provider,
        int expectedId,
        string expectedName,
        string? expectedEmail,
        bool expectedIsActive,
        short? expectedAge,
        decimal expectedBalance,
        DateTime expectedCreatedAt,
        DateTime? expectedUpdatedAt,
        DateTime? expectedBirthDate,
        string? expectedFixedCode,
        long expectedBigCount,
        decimal? expectedPrecisionValue,
        double? expectedDoubleValue,
        Guid? expectedGuidValue,
        byte[]? expectedBinaryValue,
        TimeSpan? expectedTimeValue,
        DateTimeOffset? expectedDateTimeOffsetValue)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedName);
        GetStringOrNull(reader, 2).Should().Be(expectedEmail);
        Convert.ToBoolean(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedIsActive);

        if (expectedAge is null)
        {
            reader.IsDBNull(4).Should().BeTrue();
        }
        else
        {
            Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedAge.Value);
        }

        Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedBalance);
        NormalizeDateTimeValue(reader.GetValue(6)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(expectedCreatedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

        if (expectedUpdatedAt is null)
        {
            reader.IsDBNull(7).Should().BeTrue();
        }
        else
        {
            NormalizeDateTimeValue(reader.GetValue(7)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
                .Should().Be(expectedUpdatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        }

        if (expectedBirthDate is null)
        {
            reader.IsDBNull(8).Should().BeTrue();
        }
        else
        {
            NormalizeDateTimeValue(reader.GetValue(8)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
                .Should().Be(expectedBirthDate.Value.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        }

        GetStringOrNull(reader, 9).Should().Be(expectedFixedCode);
        Convert.ToInt64(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedBigCount);

        if (expectedPrecisionValue is null)
        {
            reader.IsDBNull(11).Should().BeTrue();
        }
        else
        {
            Convert.ToDecimal(reader.GetValue(11), CultureInfo.InvariantCulture).Should().Be(expectedPrecisionValue.Value);
        }

        if (expectedDoubleValue is null)
        {
            reader.IsDBNull(12).Should().BeTrue();
        }
        else
        {
            reader.GetDouble(12).Should().Be(expectedDoubleValue.Value);
        }

        if (expectedGuidValue is null)
        {
            reader.IsDBNull(13).Should().BeTrue();
        }
        else
        {
            NormalizeGuidValue(reader.GetValue(13)).Should().Be(expectedGuidValue.Value);
        }

        if (expectedBinaryValue is null)
        {
            reader.IsDBNull(14).Should().BeTrue();
        }
        else
        {
            NormalizeBinaryValue(reader.GetValue(14), provider).Should().Equal(expectedBinaryValue);
        }

        if (expectedTimeValue is null)
        {
            reader.IsDBNull(15).Should().BeTrue();
        }
        else
        {
            NormalizeTimeSpanValue(reader.GetValue(15)).Should().Be(expectedTimeValue.Value);
        }

        if (expectedDateTimeOffsetValue is null)
        {
            reader.IsDBNull(16).Should().BeTrue();
        }
        else
        {
            NormalizeDateTimeOffsetValue(reader.GetValue(16), provider).Should().Be(expectedDateTimeOffsetValue.Value);
        }

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private bool SupportsNativeDateTimeOffsetStorage()
        => Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure or ProviderId.Npgsql or ProviderId.Oracle;

    private string? NormalizeOracleNullableText(string? value)
        => Repo.Dialect.Provider == ProviderId.Oracle && string.IsNullOrEmpty(value) ? null : value;

    private DateTimeOffset NormalizeStoredDateTimeOffsetValue(DateTimeOffset value)
        => Repo.Dialect.Provider == ProviderId.Npgsql
            ? value.ToUniversalTime()
            : Repo.Dialect.Provider == ProviderId.Oracle
                ? new DateTimeOffset(value.DateTime, TimeSpan.Zero)
                : value;
}


