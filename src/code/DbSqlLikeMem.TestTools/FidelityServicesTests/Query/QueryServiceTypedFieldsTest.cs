namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest
{
    /// <summary>
    /// EN: Executes typed-field inserts and validates common SQL functions over numeric, text, and temporal columns.
    /// PT-br: Executa inserts com campos tipados e valida funcoes SQL comuns sobre colunas numericas, textuais e temporais.
    /// </summary>
    public async Task<int> RunTypedFieldAndFunctionBlendAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        await ExpectIntScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName}", 3, "row count");
        await ExpectIntScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE CreatedAt IS NOT NULL", 3, "created-at presence");
        await ExpectIntScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE UpdatedAt IS NULL", 3, "updated-at nullability");
        await ExpectIntScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE Email IS NULL", 1, "email nullability");
        await ExpectIntScalarAsync($"SELECT SUM(COALESCE(Age, 0)) FROM {Context.TbUsersFullName}", 58, "age sum");
        await ExpectDecimalScalarAsync($"SELECT ROUND(SUM(Balance), 2) FROM {Context.TbUsersFullName}", 35.75m, "balance sum");
        await ExpectStringScalarAsync($"SELECT UPPER(Name) FROM {Context.TbUsersFullName} WHERE Id = 1", "ALICE", "upper name");
        await ExpectStringScalarAsync($"SELECT LOWER(Name) FROM {Context.TbUsersFullName} WHERE Id = 2", "bob", "lower name");
        await ExpectStringScalarAsync($"SELECT {Repo.Dialect.StringPrefixExpression("Name", 3)} FROM {Context.TbUsersFullName} WHERE Id = 1", "Ali", "substring");
        await ExpectStringScalarAsync($"SELECT COALESCE(Email, 'missing@example.com') FROM {Context.TbUsersFullName} WHERE Id = 2", "missing@example.com", "coalesce");
        await ExpectIntScalarAsync($"SELECT ABS(COALESCE(Age, 0) - 30) FROM {Context.TbUsersFullName} WHERE Id = 2", 3, "absolute difference");
        await ExpectNullScalarAsync($"SELECT NULLIF(Name, 'Bob') FROM {Context.TbUsersFullName} WHERE Id = 2", "nullif");

        return 12;
    }

    /// <summary>
    /// EN: Executes a single large projection query over typed columns and validates each calculated column per row.
    /// PT-br: Executa uma unica consulta grande sobre colunas tipadas e valida cada coluna calculada por linha.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTypedFieldFunctionMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    Name,
    UPPER(Name) AS NameUpper,
    LOWER(Name) AS NameLower,
    COALESCE(Email, 'missing@example.com') AS EmailOrDefault,
    ROUND(Balance, 2) AS BalanceRounded,
    ROUND(Balance + 1.25, 2) AS BalancePlus,
    COALESCE(Age, 0) AS AgeOrZero,
    ABS(COALESCE(Age, 0) - 30) AS AgeDelta,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull,
    CASE WHEN ProfileJson IS NULL THEN 1 ELSE 0 END AS ProfileJsonIsNull,
    CASE WHEN CreatedAt IS NOT NULL THEN 1 ELSE 0 END AS CreatedAtPresent
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedFieldRow(reader, 1, "Alice", "ALICE", "alice", "alice@example.com", 10.50m, 11.75m, 31, 1, 0, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedFieldRow(reader, 2, "Bob", "BOB", "bob", "missing@example.com", 20.25m, 21.50m, 27, 3, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedFieldRow(reader, 3, "Carla", "CARLA", "carla", "carla@example.com", 5.00m, 6.25m, 0, 30, 0, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "Name", "NameUpper", "NameLower", "EmailOrDefault", "BalanceRounded", "BalancePlus", "AgeOrZero", "AgeDelta", "EmailIsNull", "UpdatedAtIsNull", "ProfileJsonIsNull", "CreatedAtPresent"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a second large projection query that mixes casts, predicates, arithmetic, and rounding over typed columns.
    /// PT-br: Executa uma segunda consulta grande que mistura casts, predicados, aritmetica e arredondamento sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTypedFieldCalculationMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    {Repo.Dialect.IntCastExpression("Id")} AS IdCast,
    Name,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS StartsWithA,
    CASE WHEN Name LIKE '%a' THEN 1 ELSE 0 END AS EndsWithA,
    {Repo.Dialect.IntCastExpression("COALESCE(Age, 0)")} AS AgeCast,
    COALESCE(Age, 0) + 5 AS AgePlusFive,
    ROUND(Balance * 1.10, 2) AS BalanceWithTax,
    ROUND(Balance / 3.0, 2) AS BalanceThird,
    CASE WHEN Balance > 15 THEN 1 ELSE 0 END AS BalanceGt15,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull,
    CASE WHEN CreatedAt IS NOT NULL THEN 1 ELSE 0 END AS CreatedAtPresent,
    CASE WHEN ProfileJson IS NULL THEN 1 ELSE 0 END AS ProfileJsonIsNull
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedCalculationRow(reader, 1, 1, "Alice", 1, 0, 31, 36, 11.55m, 3.50m, 0, 0, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedCalculationRow(reader, 2, 2, "Bob", 0, 0, 27, 32, 22.28m, 6.75m, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedCalculationRow(reader, 3, 3, "Carla", 0, 1, 0, 5, 5.50m, 1.67m, 0, 0, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "IdCast", "Name", "StartsWithA", "EndsWithA", "AgeCast", "AgePlusFive", "BalanceWithTax", "BalanceThird", "BalanceGt15", "EmailIsNull", "UpdatedAtIsNull", "CreatedAtPresent", "ProfileJsonIsNull"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query over JSON profile columns and typed fields.
    /// PT-br: Executa uma consulta grande sobre colunas JSON de perfil e campos tipados.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunJsonTypedFieldMatrixAsync(params object[] pars)
    {
        var aliceProfileJson = """{"profile":{"name":"Alice","active":true}}""";
        var bobProfileJson = """{"profile":{"name":"Bob","active":false}}""";

        void InsertRow(
            int id,
            string name,
            string? email,
            int? age,
            decimal balance,
            string? profileJson)
        {

            using var insertCommand = Repo.Cnn.CreateCommand();
            var idParameter = Repo.Dialect.Parameter("id");
            var nameParameter = Repo.Dialect.Parameter("name");
            var emailParameter = Repo.Dialect.Parameter("email");
            var ageParameter = Repo.Dialect.Parameter("age");
            var balanceParameter = Repo.Dialect.Parameter("balance");
            var profileJsonValue = Repo.Dialect.JsonParameter("profileJson");
            insertCommand.CommandText = $"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES
({idParameter}, {nameParameter}, {emailParameter}, {ageParameter}, {balanceParameter}, NULL, {profileJsonValue})
""";
            AddParameter(insertCommand, "id", DbType.Int32, id);
            AddParameter(insertCommand, "name", DbType.String, name);
            AddParameter(insertCommand, "email", DbType.String, email is null ? DBNull.Value : email);
            AddParameter(insertCommand, "age", DbType.Int32, age is null ? DBNull.Value : age);
            AddParameter(insertCommand, "balance", DbType.Decimal, balance);
            AddParameter(insertCommand, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);

            insertCommand.ExecuteNonQuery();
        }

        InsertRow(1, "Alice", "alice@example.com", 31, 10.50m, aliceProfileJson);
        InsertRow(2, "Bob", null, 27, 20.25m, bobProfileJson);
        InsertRow(3, "Carla", "carla@example.com", null, 5.00m, null);

        var jsonNameExpr = Repo.Dialect.JsonProfileNameExpression("ProfileJson");

        using var command = Repo.Cnn.CreateCommand();
        var profilePrefixExpr = Repo.Dialect.StringPrefixExpression($"COALESCE({jsonNameExpr}, 'missing')", 3);

        command.CommandText = $"""
SELECT
    Id,
    Name,
    {jsonNameExpr} AS ProfileName,
    UPPER({jsonNameExpr}) AS ProfileNameUpper,
    COALESCE({jsonNameExpr}, 'missing') AS ProfileNameOrDefault,
    {profilePrefixExpr} AS ProfileNamePrefix,
    CASE WHEN {jsonNameExpr} = 'Alice' THEN 1 ELSE 0 END AS IsAlice,
    CASE WHEN {jsonNameExpr} IS NULL THEN 1 ELSE 0 END AS JsonProfileIsNull,
    CASE WHEN ProfileJson IS NULL THEN 1 ELSE 0 END AS ProfileJsonIsNull,
    ROUND(Balance + COALESCE(Age, 0), 2) AS BalancePlusAge
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateJsonFieldRow(reader, 1, "Alice", "Alice", "ALICE", "Alice", "Ali", 1, 0, 0, 41.50m));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateJsonFieldRow(reader, 2, "Bob", "Bob", "BOB", "Bob", "Bob", 0, 0, 0, 47.25m));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateJsonFieldRow(reader, 3, "Carla", null, null, "missing", "mis", 0, 1, 1, 5.00m));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "Name", "ProfileName", "ProfileNameUpper", "ProfileNameOrDefault", "ProfileNamePrefix", "IsAlice", "JsonProfileIsNull", "ProfileJsonIsNull", "BalancePlusAge"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query over temporal columns and numeric calculations.
    /// PT-br: Executa uma consulta grande sobre colunas temporais e calculos numericos.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTemporalFieldMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var useUtcClock = Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure;
        var nowExpr = useUtcClock
            ? "SYSUTCDATETIME()"
            : Repo.Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = useUtcClock
            ? "DATEADD(day, 1, SYSUTCDATETIME())"
            : Repo.Dialect.TemporalDateAddExpression();

        using var command = Repo.Cnn.CreateCommand();
        var namePrefixExpr = Repo.Dialect.StringPrefixExpression("Name", 2);
        command.CommandText = $"""
SELECT
    Id,
    Name,
    CASE WHEN CreatedAt IS NOT NULL THEN 1 ELSE 0 END AS CreatedAtPresent,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull,
    CASE WHEN {nowExpr} IS NOT NULL THEN 1 ELSE 0 END AS NowPresent,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    ROUND(Balance + COALESCE(Age, 0), 2) AS BalancePlusAge,
    ABS(COALESCE(Age, 0) - 30) AS AgeDelta,
    COALESCE(Email, 'missing@example.com') AS EmailOrDefault,
    {namePrefixExpr} AS NamePrefix
FROM {Context.TbUsersFullName}
ORDER BY CreatedAt, Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalFieldRow(reader, 1, "Alice", 1, 1, 1, 1, 41.50m, 1, "alice@example.com", "Al"));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalFieldRow(reader, 2, "Bob", 1, 1, 1, 1, 47.25m, 3, "missing@example.com", "Bo"));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalFieldRow(reader, 3, "Carla", 1, 1, 1, 1, 5.00m, 30, "carla@example.com", "Ca"));

        (await reader.ReadAsync()).Should().BeFalse();


        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "Name", "CreatedAtPresent", "UpdatedAtIsNull", "NowPresent", "NextDayAfterNow", "BalancePlusAge", "AgeDelta", "EmailOrDefault", "NamePrefix"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query that blends temporal comparisons and fallback logic over typed columns.
    /// PT-br: Executa uma consulta grande que mistura comparacoes temporais e logica de fallback sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTemporalComparisonMatrixAsync(params object[] pars)
    {
        var createdAt = NormalizeTemporalMatrixDateTimeInput(new DateTime(2000, 1, 1, 12, 0, 0, DateTimeKind.Utc));
        var createdAtParameter = Repo.Dialect.Parameter("CreatedAt");

        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));

        var useUtcClock = Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure;
        var nowExpr = useUtcClock
            ? "SYSUTCDATETIME()"
            : Repo.Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = useUtcClock
            ? "DATEADD(day, 1, SYSUTCDATETIME())"
            : Repo.Dialect.TemporalDateAddExpression();

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    CASE WHEN CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS CreatedAtBeforeNow,
    CASE WHEN {nextDayExpr} > CreatedAt THEN 1 ELSE 0 END AS NextDayAfterCreatedAt,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    CASE WHEN COALESCE(UpdatedAt, CreatedAt) >= CreatedAt THEN 1 ELSE 0 END AS FallbackAtLeastCreatedAt,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull
FROM {Context.TbUsersFullName}
ORDER BY CreatedAt, Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalComparisonRow(reader, 1, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalComparisonRow(reader, 2, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalComparisonRow(reader, 3, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "CreatedAtBeforeNow", "NextDayAfterCreatedAt", "NextDayAfterNow", "FallbackAtLeastCreatedAt", "UpdatedAtIsNull"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a larger temporal arithmetic query over typed columns and validates relative date and fallback comparisons.
    /// PT-br: Executa uma consulta maior de aritmetica temporal sobre colunas tipadas e valida comparacoes relativas e de fallback.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTemporalArithmeticMatrixAsync(params object[] pars)
    {
        var createdAt = NormalizeTemporalMatrixDateTimeInput(new DateTime(2000, 1, 1, 12, 0, 0, DateTimeKind.Utc));
        var createdAtParameter = Repo.Dialect.Parameter("CreatedAt");

        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, CreatedAt, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, {createdAtParameter}, NULL, NULL)
""", addParameters: cmd => Repo.Dialect.AddParameter(cmd, "CreatedAt", DbType.DateTime2, createdAt));

        var useUtcClock = Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure;
        var nowExpr = useUtcClock
            ? "SYSUTCDATETIME()"
            : Repo.Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = useUtcClock
            ? "DATEADD(day, 1, SYSUTCDATETIME())"
            : Repo.Dialect.TemporalDateAddExpression();

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    CASE WHEN CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS CreatedAtBeforeNow,
    CASE WHEN {nextDayExpr} > CreatedAt THEN 1 ELSE 0 END AS TomorrowAfterCreatedAt,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS TomorrowAfterNow,
    CASE WHEN COALESCE(UpdatedAt, CreatedAt) <= {nextDayExpr} THEN 1 ELSE 0 END AS FallbackBeforeTomorrow,
    CASE WHEN COALESCE(UpdatedAt, CreatedAt) >= CreatedAt THEN 1 ELSE 0 END AS FallbackAtLeastCreatedAt,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull,
    CASE WHEN CreatedAt IS NOT NULL THEN 1 ELSE 0 END AS CreatedAtIsNotNull
FROM {Context.TbUsersFullName}
ORDER BY CreatedAt, Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalArithmeticRow(reader, 1, 1, 1, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalArithmeticRow(reader, 2, 1, 1, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTemporalArithmeticRow(reader, 3, 1, 1, 1, 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "CreatedAtBeforeNow", "TomorrowAfterCreatedAt", "TomorrowAfterNow", "FallbackBeforeTomorrow", "FallbackAtLeastCreatedAt", "UpdatedAtIsNull", "CreatedAtIsNotNull"],
            Rows = rows,
        };
    }

    private DateTime NormalizeTemporalMatrixDateTimeInput(DateTime value)
    {
        if (Repo.Dialect.Provider == ProviderId.Npgsql
            && value.Kind == DateTimeKind.Utc)
        {
            return DateTime.SpecifyKind(value, DateTimeKind.Unspecified);
        }

        return value;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends casts, arithmetic, and boolean-style calculations over typed columns.
    /// PT-br: Executa uma consulta grande que mistura casts, aritmetica e calculos em estilo booleano sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunCastCalculationMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var roundedBalanceSuffix = Repo.Dialect.Provider switch
        {
            ProviderId.Sqlite => ".0",
            ProviderId.Db2 => ".00",
            ProviderId.SqlServer or ProviderId.SqlAzure => ".00",
            _ => string.Empty
        };

        var agePlusBalanceSuffix = Repo.Dialect.Provider switch
        {
            ProviderId.Sqlite => ".0",
            ProviderId.Db2 => ".00",
            ProviderId.Firebird => ".00",
            ProviderId.SqlServer or ProviderId.SqlAzure => ".00",
            _ => string.Empty
        };
        var agePlusBalanceExpression = Repo.Dialect.Provider == ProviderId.Firebird
            ? "CAST(COALESCE(Age, 0) + ROUND(Balance, 0) AS DECIMAL(18,2))"
            : "COALESCE(Age, 0) + ROUND(Balance, 0)";

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    TRIM({Repo.Dialect.StringCastExpression("Id", 10)}) AS IdText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(Age, 0)", 10)}) AS AgeText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(Age, 0) + 5", 10)}) AS AgePlusFiveText,
    TRIM({Repo.Dialect.StringCastExpression("ROUND(Balance, 0)", 10)}) AS BalanceRoundedText,
    TRIM({Repo.Dialect.StringCastExpression("ROUND(Balance * 2, 0)", 10)}) AS BalanceDoubleText,
    TRIM({Repo.Dialect.StringCastExpression("ABS(COALESCE(Age, 0) - 30)", 10)}) AS AgeDeltaText,
    TRIM({Repo.Dialect.StringCastExpression("CASE WHEN Email IS NULL THEN 0 ELSE 1 END", 1)}) AS EmailFlagText,
    TRIM({Repo.Dialect.StringCastExpression("CASE WHEN Balance > 15 THEN 0 ELSE 1 END", 1)}) AS BalanceNotGt15Text,
    TRIM({Repo.Dialect.StringCastExpression(agePlusBalanceExpression, 10)}) AS AgePlusBalanceText
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateCastRow(reader, 1, "1", "31", "36", $"11{roundedBalanceSuffix}", $"21{roundedBalanceSuffix}", "1", "1", "1", $"42{agePlusBalanceSuffix}"));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateCastRow(reader, 2, "2", "27", "32", $"20{roundedBalanceSuffix}", $"41{roundedBalanceSuffix}", "3", "0", "0", $"47{agePlusBalanceSuffix}"));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateCastRow(reader, 3, "3", "0", "5", $"5{roundedBalanceSuffix}", $"10{roundedBalanceSuffix}", "30", "1", "1", $"5{agePlusBalanceSuffix}"));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "IdText", "AgeText", "AgePlusFiveText", "BalanceRoundedText", "BalanceDoubleText", "AgeDeltaText", "EmailFlagText", "BalanceNotGt15Text", "AgePlusBalanceText"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query that blends null handling, comparisons, and predicates over typed columns.
    /// PT-br: Executa uma consulta grande que mistura tratamento de null, comparacoes e predicados sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunNullComparisonMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull,
    CASE WHEN Email IS NOT NULL THEN 1 ELSE 0 END AS EmailIsNotNull,
    CASE WHEN Age IS NULL THEN 1 ELSE 0 END AS AgeIsNull,
    CASE WHEN Age BETWEEN 20 AND 30 THEN 1 ELSE 0 END AS AgeBetween20And30,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS NameStartsWithA,
    CASE WHEN Name LIKE '%a' THEN 1 ELSE 0 END AS NameEndsWithA,
    CASE WHEN NULLIF(Name, 'Bob') IS NULL THEN 1 ELSE 0 END AS NullIfBob,
    CASE WHEN COALESCE(Email, '') LIKE '%@example.com' THEN 1 ELSE 0 END AS EmailLooksLikeExample,
    CASE WHEN Id IN (1, 3) THEN 1 ELSE 0 END AS IdIn13,
    CASE WHEN Id NOT IN (2) THEN 1 ELSE 0 END AS IdNotIn2,
    CASE WHEN Balance > 15 THEN 1 ELSE 0 END AS BalanceGt15,
    CASE WHEN Balance <= 10.50 THEN 1 ELSE 0 END AS BalanceLe1050
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateNullComparisonRow(reader, 1, 0, 1, 0, 0, 1, 0, 0, 1, 1, 1, 0, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateNullComparisonRow(reader, 2, 1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateNullComparisonRow(reader, 3, 0, 1, 1, 0, 0, 1, 0, 1, 1, 1, 0, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "EmailIsNull", "EmailIsNotNull", "AgeIsNull", "AgeBetween20And30", "NameStartsWithA", "NameEndsWithA", "NullIfBob", "EmailLooksLikeExample", "IdIn13", "IdNotIn2", "BalanceGt15", "BalanceLe1050"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query that blends text lengths, trimming, and comparisons over typed columns.
    /// PT-br: Executa uma consulta grande que mistura comprimentos de texto, trim e comparacoes sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTextLengthMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var nameLenExpr = Repo.Dialect.StringLengthExpression("Name");
        var upperNameLenExpr = Repo.Dialect.StringLengthExpression("UPPER(Name)");
        var trimNameLenExpr = Repo.Dialect.StringLengthExpression("TRIM(Name)");
        var emailLenExpr = Repo.Dialect.StringLengthExpression("COALESCE(Email, 'missing@example.com')");

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    TRIM({Repo.Dialect.StringCastExpression(nameLenExpr, 10)}) AS NameLenText,
    TRIM({Repo.Dialect.StringCastExpression(emailLenExpr, 10)}) AS EmailLenText,
    TRIM({Repo.Dialect.StringCastExpression(upperNameLenExpr, 10)}) AS UpperNameLenText,
    TRIM({Repo.Dialect.StringCastExpression(trimNameLenExpr, 10)}) AS TrimmedNameLenText,
    TRIM({Repo.Dialect.StringCastExpression($"{emailLenExpr} - {nameLenExpr}", 10)}) AS EmailMinusNameText,
    CASE WHEN {nameLenExpr} >= 5 THEN 1 ELSE 0 END AS NameLenGe5,
    CASE WHEN {emailLenExpr} >= 17 THEN 1 ELSE 0 END AS EmailLenGe17,
    CASE WHEN {trimNameLenExpr} = {nameLenExpr} THEN 1 ELSE 0 END AS TrimmedNameSameLen,
    CASE WHEN {upperNameLenExpr} = {nameLenExpr} THEN 1 ELSE 0 END AS UpperNameSameLen
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextLengthRow(reader, 1, "5", "17", "5", "5", "12", 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextLengthRow(reader, 2, "3", "19", "3", "3", "16", 0, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextLengthRow(reader, 3, "5", "17", "5", "5", "12", 1, 1, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "NameLenText", "EmailLenText", "UpperNameLenText", "TrimmedNameLenText", "EmailMinusNameText", "NameLenGe5", "EmailLenGe17", "TrimmedNameSameLen", "UpperNameSameLen"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large projection query that blends case conversion, trimming, prefix extraction, and text predicates over typed columns.
    /// PT-br: Executa uma consulta grande que mistura conversao de caixa, trim, extracao de prefixo e predicados de texto sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTextCaseMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var namePrefixExpr = Repo.Dialect.StringPrefixExpression("Name", 2);
        var nameLenExpr = Repo.Dialect.StringLengthExpression("Name");
        var textMatchAlready = Repo.Dialect.Provider is ProviderId.Sqlite or ProviderId.Oracle or ProviderId.Npgsql or ProviderId.Db2 or ProviderId.Firebird ? 0 : 1;

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    UPPER(Name) AS NameUpper,
    LOWER(Name) AS NameLower,
    TRIM(Name) AS NameTrimmed,
    {namePrefixExpr} AS NamePrefix,
    TRIM({Repo.Dialect.StringCastExpression(nameLenExpr, 10)}) AS NameLenText,
    CASE WHEN UPPER(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyUpper,
    CASE WHEN LOWER(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyLower,
    CASE WHEN TRIM(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyTrimmed,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS StartsWithA,
    CASE WHEN Name LIKE '%a' THEN 1 ELSE 0 END AS EndsWithA,
    COALESCE(Email, 'missing@example.com') AS EmailOrDefault,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextCaseRow(reader, 1, "ALICE", "alice", "Alice", "Al", "5", textMatchAlready, textMatchAlready, 1, 1, 0, "alice@example.com", 0));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextCaseRow(reader, 2, "BOB", "bob", "Bob", "Bo", "3", textMatchAlready, textMatchAlready, 1, 0, 0, "missing@example.com", 1));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTextCaseRow(reader, 3, "CARLA", "carla", "Carla", "Ca", "5", textMatchAlready, textMatchAlready, 1, 0, 1, "carla@example.com", 0));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "NameUpper", "NameLower", "NameTrimmed", "NamePrefix", "NameLenText", "IsAlreadyUpper", "IsAlreadyLower", "IsAlreadyTrimmed", "StartsWithA", "EndsWithA", "EmailOrDefault", "EmailIsNull"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a large predicate query that blends LIKE, NOT LIKE, BETWEEN, and null checks over typed columns.
    /// PT-br: Executa uma consulta grande de predicados que mistura LIKE, NOT LIKE, BETWEEN e verificacoes de null sobre colunas tipadas.
    /// </summary>
    internal async Task<QueryResultSnapshot> RunTypedFieldPredicateMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    Name,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS StartsWithA,
    CASE WHEN Name NOT LIKE '%z' THEN 1 ELSE 0 END AS NotEndsWithZ,
    CASE WHEN Age BETWEEN 20 AND 30 THEN 1 ELSE 0 END AS AgeBetween20And30,
    CASE WHEN Balance BETWEEN 5 AND 20 THEN 1 ELSE 0 END AS BalanceBetween5And20,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull,
    CASE WHEN Email IS NOT NULL THEN 1 ELSE 0 END AS EmailIsNotNull,
    CASE WHEN Age IS NULL THEN 1 ELSE 0 END AS AgeIsNull
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();
        var rows = new List<QueryResultRowSnapshot>(3);

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedPredicateRow(reader, 1, "Alice", 1, 1, 0, 1, 0, 1, 0));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedPredicateRow(reader, 2, "Bob", 0, 1, 1, 0, 1, 0, 0));

        (await reader.ReadAsync()).Should().BeTrue();
        rows.Add(ValidateTypedPredicateRow(reader, 3, "Carla", 0, 1, 0, 1, 0, 1, 1));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["Id", "Name", "StartsWithA", "NotEndsWithZ", "AgeBetween20And30", "BalanceBetween5And20", "EmailIsNull", "EmailIsNotNull", "AgeIsNull"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a compound predicate query that blends OR, AND, LIKE, and null checks over typed columns.
    /// PT-br: Executa uma consulta de predicado composto que mistura OR, AND, LIKE e verificacoes de null sobre colunas tipadas.
    /// </summary>
    public async Task<int> RunTypedFieldCompoundPredicateMatrixAsync(params object[] pars)
    {
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync($"""
SELECT COUNT(*)
FROM {Context.TbUsersFullName}
WHERE (Email IS NULL OR Age IS NULL)
  AND (Name LIKE 'B%' OR Name LIKE 'C%')
"""), CultureInfo.InvariantCulture);
        value.Should().Be(2);

        return value;
    }

    private static QueryResultRowSnapshot ValidateTypedCalculationRow(
        DbDataReader reader,
        int expectedId,
        int expectedIdCast,
        string expectedName,
        int expectedStartsWithA,
        int expectedEndsWithA,
        int expectedAgeCast,
        int expectedAgePlusFive,
        decimal expectedBalanceWithTax,
        decimal expectedBalanceThird,
        int expectedBalanceGt15,
        int expectedEmailIsNull,
        int expectedUpdatedAtIsNull,
        int expectedCreatedAtPresent,
        int expectedProfileJsonIsNull)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedIdCast);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedStartsWithA);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedEndsWithA);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedAgeCast);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedAgePlusFive);
        Convert.ToDecimal(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedBalanceWithTax);
        Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedBalanceThird);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedBalanceGt15);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNull);
        Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture).Should().Be(expectedUpdatedAtIsNull);
        Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtPresent);
        Convert.ToInt32(reader.GetValue(13), CultureInfo.InvariantCulture).Should().Be(expectedProfileJsonIsNull);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateJsonFieldRow(
        DbDataReader reader,
        int expectedId,
        string expectedName,
        string? expectedProfileName,
        string? expectedProfileNameUpper,
        string expectedProfileNameOrDefault,
        string expectedProfileNamePrefix,
        int expectedIsAlice,
        int expectedJsonProfileIsNull,
        int expectedProfileJsonIsNull,
        decimal expectedBalancePlusAge)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedName);
        GetStringOrNull(reader, 2).Should().Be(expectedProfileName);
        GetStringOrNull(reader, 3).Should().Be(expectedProfileNameUpper);
        GetStringOrNull(reader, 4).Should().Be(expectedProfileNameOrDefault);
        GetStringOrNull(reader, 5).Should().Be(expectedProfileNamePrefix);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedIsAlice);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedJsonProfileIsNull);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedProfileJsonIsNull);
        Convert.ToDecimal(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedBalancePlusAge);

        return new QueryResultRowSnapshot
        {
            Values =
            [
                expectedId,
                expectedName,
                expectedProfileName,
                expectedProfileNameUpper,
                expectedProfileNameOrDefault,
                expectedProfileNamePrefix,
                expectedIsAlice,
                expectedJsonProfileIsNull,
                expectedProfileJsonIsNull,
                expectedBalancePlusAge
            ]
        };
    }

    private static QueryResultRowSnapshot ValidateTemporalFieldRow(
        DbDataReader reader,
        int expectedId,
        string expectedName,
        int expectedCreatedAtPresent,
        int expectedUpdatedAtIsNull,
        int expectedNowPresent,
        int expectedNextDayAfterNow,
        decimal expectedBalancePlusAge,
        int expectedAgeDelta,
        string expectedEmailOrDefault,
        string expectedNamePrefix)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtPresent);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedUpdatedAtIsNull);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedNowPresent);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedNextDayAfterNow);
        Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedBalancePlusAge);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedAgeDelta);
        Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedEmailOrDefault);
        Convert.ToString(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedNamePrefix);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateTemporalComparisonRow(
        DbDataReader reader,
        int expectedId,
        int expectedCreatedAtBeforeNow,
        int expectedNextDayAfterCreatedAt,
        int expectedNextDayAfterNow,
        int expectedFallbackAtLeastCreatedAt,
        int expectedUpdatedAtIsNull)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtBeforeNow);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedNextDayAfterCreatedAt);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedNextDayAfterNow);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedFallbackAtLeastCreatedAt);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedUpdatedAtIsNull);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateTemporalArithmeticRow(
        DbDataReader reader,
        int expectedId,
        int expectedCreatedAtBeforeNow,
        int expectedTomorrowAfterCreatedAt,
        int expectedTomorrowAfterNow,
        int expectedFallbackBeforeTomorrow,
        int expectedFallbackAtLeastCreatedAt,
        int expectedUpdatedAtIsNull,
        int expectedCreatedAtIsNotNull)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtBeforeNow);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedTomorrowAfterCreatedAt);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTomorrowAfterNow);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedFallbackBeforeTomorrow);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedFallbackAtLeastCreatedAt);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedUpdatedAtIsNull);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtIsNotNull);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateCastRow(
        DbDataReader reader,
        int expectedId,
        string expectedIdText,
        string expectedAgeText,
        string expectedAgePlusFiveText,
        string expectedBalanceRoundedText,
        string expectedBalanceDoubleText,
        string expectedAgeDeltaText,
        string expectedEmailFlagText,
        string expectedBalanceNotGt15Text,
        string expectedAgePlusBalanceText)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedIdText);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedAgeText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedAgePlusFiveText);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedBalanceRoundedText);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedBalanceDoubleText);
        Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedAgeDeltaText);
        Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedEmailFlagText);
        Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedBalanceNotGt15Text);
        Convert.ToString(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedAgePlusBalanceText);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateNullComparisonRow(
        DbDataReader reader,
        int expectedId,
        int expectedEmailIsNull,
        int expectedEmailIsNotNull,
        int expectedAgeIsNull,
        int expectedAgeBetween20And30,
        int expectedNameStartsWithA,
        int expectedNameEndsWithA,
        int expectedNullIfBob,
        int expectedEmailLooksLikeExample,
        int expectedIdIn13,
        int expectedIdNotIn2,
        int expectedBalanceGt15,
        int expectedBalanceLe1050)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNull);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNotNull);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedAgeIsNull);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedAgeBetween20And30);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedNameStartsWithA);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNameEndsWithA);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedNullIfBob);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedEmailLooksLikeExample);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedIdIn13);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedIdNotIn2);
        Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture).Should().Be(expectedBalanceGt15);
        Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture).Should().Be(expectedBalanceLe1050);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateTextLengthRow(
        DbDataReader reader,
        int expectedId,
        string expectedNameLenText,
        string expectedEmailLenText,
        string expectedUpperNameLenText,
        string expectedTrimmedNameLenText,
        string expectedEmailMinusNameText,
        int expectedNameLenGe5,
        int expectedEmailLenGe17,
        int expectedTrimmedNameSameLen,
        int expectedUpperNameSameLen)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedNameLenText);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedEmailLenText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedUpperNameLenText);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedTrimmedNameLenText);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedEmailMinusNameText);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNameLenGe5);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedEmailLenGe17);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedTrimmedNameSameLen);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedUpperNameSameLen);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateTextCaseRow(
        DbDataReader reader,
        int expectedId,
        string expectedNameUpper,
        string expectedNameLower,
        string expectedNameTrimmed,
        string expectedNamePrefix,
        string expectedNameLenText,
        int expectedIsAlreadyUpper,
        int expectedIsAlreadyLower,
        int expectedIsAlreadyTrimmed,
        int expectedStartsWithA,
        int expectedEndsWithA,
        string expectedEmailOrDefault,
        int expectedEmailIsNull)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedNameUpper);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedNameLower);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedNameTrimmed);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedNamePrefix);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedNameLenText);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedIsAlreadyUpper);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedIsAlreadyLower);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedIsAlreadyTrimmed);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedStartsWithA);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedEndsWithA);
        Convert.ToString(reader.GetValue(11), CultureInfo.InvariantCulture).Should().Be(expectedEmailOrDefault);
        Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNull);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static QueryResultRowSnapshot ValidateTypedPredicateRow(
        DbDataReader reader,
        int expectedId,
        string expectedName,
        int expectedStartsWithA,
        int expectedNotEndsWithZ,
        int expectedAgeBetween20And30,
        int expectedBalanceBetween5And20,
        int expectedEmailIsNull,
        int expectedEmailIsNotNull,
        int expectedAgeIsNull)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedStartsWithA);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedNotEndsWithZ);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedAgeBetween20And30);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedBalanceBetween5And20);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNull);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNotNull);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedAgeIsNull);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private static string? GetStringOrNull(DbDataReader reader, int ordinal)
    {
        var rawValue = reader.GetValue(ordinal);
        return rawValue is DBNull ? null : Convert.ToString(rawValue, CultureInfo.InvariantCulture);
    }

    private static QueryResultRowSnapshot ValidateTypedFieldRow(
        DbDataReader reader,
        int expectedId,
        string expectedName,
        string expectedUpperName,
        string expectedLowerName,
        string expectedEmailOrDefault,
        decimal expectedBalanceRounded,
        decimal expectedBalancePlus,
        int expectedAgeOrZero,
        int expectedAgeDelta,
        int expectedEmailIsNull,
        int expectedUpdatedAtIsNull,
        int expectedProfileJsonIsNull,
        int expectedCreatedAtPresent)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedUpperName);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedLowerName);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedEmailOrDefault);
        Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedBalanceRounded);
        Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedBalancePlus);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedAgeOrZero);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedAgeDelta);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedEmailIsNull);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedUpdatedAtIsNull);
        Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture).Should().Be(expectedProfileJsonIsNull);
        Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture).Should().Be(expectedCreatedAtPresent);

        return QueryResultSnapshotReader.CaptureRow(reader);
    }

    private async Task ExpectIntScalarAsync(string sql, int expected, string label)
    {
        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync(sql), CultureInfo.InvariantCulture);
        if (value != expected)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Repo.Dialect.DisplayName}: {value}.");
        }
    }

    private async Task ExpectDecimalScalarAsync(string sql, decimal expected, string label)
    {
        var value = Convert.ToDecimal(await Repo.ExecuteScalarAsync(sql), CultureInfo.InvariantCulture);
        if (value != expected)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Repo.Dialect.DisplayName}: {value}.");
        }
    }

    private async Task ExpectStringScalarAsync(string sql, string expected, string label)
    {
        var rawValue = await Repo.ExecuteScalarAsync(sql);
        var value = rawValue is null or DBNull ? null : Convert.ToString(rawValue, CultureInfo.InvariantCulture);
        if (!string.Equals(value, expected, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Repo.Dialect.DisplayName}: {value ?? "<null>"}.");
        }
    }

    private async Task ExpectNullScalarAsync(string sql, string label)
    {
        var rawValue = await Repo.ExecuteScalarAsync(sql);
        if (rawValue is not null and not DBNull)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Repo.Dialect.DisplayName}: {Convert.ToString(rawValue, CultureInfo.InvariantCulture)}.");
        }
    }
}
