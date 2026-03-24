namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Executes typed-field inserts and validates common SQL functions over numeric, text, and temporal columns.
    /// PT: Executa inserts com campos tipados e valida funcoes SQL comuns sobre colunas numericas, textuais e temporais.
    /// </summary>
    public int RunTypedFieldAndFunctionBlend(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        ExpectIntScalar($"SELECT COUNT(*) FROM {tableName}", 3, "row count");
        ExpectIntScalar($"SELECT COUNT(*) FROM {tableName} WHERE CreatedAt IS NOT NULL", 3, "created-at presence");
        ExpectIntScalar($"SELECT COUNT(*) FROM {tableName} WHERE UpdatedAt IS NULL", 3, "updated-at nullability");
        ExpectIntScalar($"SELECT COUNT(*) FROM {tableName} WHERE Email IS NULL", 1, "email nullability");
        ExpectIntScalar($"SELECT SUM(COALESCE(Age, 0)) FROM {tableName}", 58, "age sum");
        ExpectDecimalScalar($"SELECT ROUND(SUM(Balance), 2) FROM {tableName}", 35.75m, "balance sum");
        ExpectStringScalar($"SELECT UPPER(Name) FROM {tableName} WHERE Id = 1", "ALICE", "upper name");
        ExpectStringScalar($"SELECT LOWER(Name) FROM {tableName} WHERE Id = 2", "bob", "lower name");
        ExpectStringScalar($"SELECT {Dialect.StringPrefixExpression("Name", 3)} FROM {tableName} WHERE Id = 1", "Ali", "substring");
        ExpectStringScalar($"SELECT COALESCE(Email, 'missing@example.com') FROM {tableName} WHERE Id = 2", "missing@example.com", "coalesce");
        ExpectIntScalar($"SELECT ABS(COALESCE(Age, 0) - 30) FROM {tableName} WHERE Id = 2", 3, "absolute difference");
        ExpectNullScalar($"SELECT NULLIF(Name, 'Bob') FROM {tableName} WHERE Id = 2", "nullif");

        GC.KeepAlive(tableName);
        return 12;
    }

    /// <summary>
    /// EN: Executes a single large projection query over typed columns and validates each calculated column per row.
    /// PT: Executa uma unica consulta grande sobre colunas tipadas e valida cada coluna calculada por linha.
    /// </summary>
    public int RunTypedFieldFunctionMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
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
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTypedFieldRow(reader, 1, "Alice", "ALICE", "alice", "alice@example.com", 10.50m, 11.75m, 31, 1, 0, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTypedFieldRow(reader, 2, "Bob", "BOB", "bob", "missing@example.com", 20.25m, 21.50m, 27, 3, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTypedFieldRow(reader, 3, "Carla", "CARLA", "carla", "carla@example.com", 5.00m, 6.25m, 0, 30, 0, 1, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a second large projection query that mixes casts, predicates, arithmetic, and rounding over typed columns.
    /// PT: Executa uma segunda consulta grande que mistura casts, predicados, aritmetica e arredondamento sobre colunas tipadas.
    /// </summary>
    public int RunTypedFieldCalculationMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    CAST(Id AS INTEGER) AS IdCast,
    Name,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS StartsWithA,
    CASE WHEN Name LIKE '%a' THEN 1 ELSE 0 END AS EndsWithA,
    CAST(COALESCE(Age, 0) AS INTEGER) AS AgeCast,
    COALESCE(Age, 0) + 5 AS AgePlusFive,
    ROUND(Balance * 1.10, 2) AS BalanceWithTax,
    ROUND(Balance / 3.0, 2) AS BalanceThird,
    CASE WHEN Balance > 15 THEN 1 ELSE 0 END AS BalanceGt15,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull,
    CASE WHEN CreatedAt IS NOT NULL THEN 1 ELSE 0 END AS CreatedAtPresent,
    CASE WHEN ProfileJson IS NULL THEN 1 ELSE 0 END AS ProfileJsonIsNull
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTypedCalculationRow(reader, 1, 1, "Alice", 1, 0, 31, 36, 11.55m, 3.50m, 0, 0, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTypedCalculationRow(reader, 2, 2, "Bob", 0, 1, 27, 32, 22.28m, 6.75m, 1, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTypedCalculationRow(reader, 3, 3, "Carla", 0, 1, 0, 5, 5.50m, 1.67m, 0, 1, 1, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query over JSON profile columns and typed fields.
    /// PT: Executa uma consulta grande sobre colunas JSON de perfil e campos tipados.
    /// </summary>
    public int RunJsonTypedFieldMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";
        var aliceProfileJson = """{"profile":{"name":"Alice","active":true}}""";
        var bobProfileJson = """{"profile":{"name":"Bob","active":false}}""";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES
(1, 'Alice', 'alice@example.com', 31, 10.50, NULL, '{aliceProfileJson}')
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES
(2, 'Bob', NULL, 27, 20.25, NULL, '{bobProfileJson}')
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES
(3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var jsonNameExpr = Dialect.JsonProfileNameExpression("ProfileJson");

        using var command = Connection.CreateCommand();
        var profilePrefixExpr = Dialect.StringPrefixExpression($"COALESCE({jsonNameExpr}, 'missing')", 3);

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
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJsonFieldRow(reader, 1, "Alice", "Alice", "ALICE", "Alice", "Ali", 1, 0, 0, 41.50m);

        Assert.True(reader.Read());
        ValidateJsonFieldRow(reader, 2, "Bob", "Bob", "BOB", "Bob", "Bob", 0, 0, 0, 47.25m);

        Assert.True(reader.Read());
        ValidateJsonFieldRow(reader, 3, "Carla", null, null, "missing", "mis", 0, 1, 1, 5.00m);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query over temporal columns and numeric calculations.
    /// PT: Executa uma consulta grande sobre colunas temporais e calculos numericos.
    /// </summary>
    public int RunTemporalFieldMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        var namePrefixExpr = Dialect.StringPrefixExpression("Name", 2);

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
FROM {tableName}
ORDER BY CreatedAt, Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTemporalFieldRow(reader, 1, "Alice", 1, 1, 1, 1, 41.50m, 1, "alice@example.com", "Al");

        Assert.True(reader.Read());
        ValidateTemporalFieldRow(reader, 2, "Bob", 1, 1, 1, 1, 47.25m, 3, "missing@example.com", "Bo");

        Assert.True(reader.Read());
        ValidateTemporalFieldRow(reader, 3, "Carla", 1, 1, 1, 1, 5.00m, 30, "carla@example.com", "Ca");

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends temporal comparisons and fallback logic over typed columns.
    /// PT: Executa uma consulta grande que mistura comparacoes temporais e logica de fallback sobre colunas tipadas.
    /// </summary>
    public int RunTemporalComparisonMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    CASE WHEN CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS CreatedAtBeforeNow,
    CASE WHEN {nextDayExpr} > CreatedAt THEN 1 ELSE 0 END AS NextDayAfterCreatedAt,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    CASE WHEN COALESCE(UpdatedAt, CreatedAt) >= CreatedAt THEN 1 ELSE 0 END AS FallbackAtLeastCreatedAt,
    CASE WHEN UpdatedAt IS NULL THEN 1 ELSE 0 END AS UpdatedAtIsNull
FROM {tableName}
ORDER BY CreatedAt, Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTemporalComparisonRow(reader, 1, 1, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTemporalComparisonRow(reader, 2, 1, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTemporalComparisonRow(reader, 3, 1, 1, 1, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a larger temporal arithmetic query over typed columns and validates relative date and fallback comparisons.
    /// PT: Executa uma consulta maior de aritmetica temporal sobre colunas tipadas e valida comparacoes relativas e de fallback.
    /// </summary>
    public int RunTemporalArithmeticMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
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
FROM {tableName}
ORDER BY CreatedAt, Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTemporalArithmeticRow(reader, 1, 1, 1, 1, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTemporalArithmeticRow(reader, 2, 1, 1, 1, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTemporalArithmeticRow(reader, 3, 1, 1, 1, 1, 1, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends casts, arithmetic, and boolean-style calculations over typed columns.
    /// PT: Executa uma consulta grande que mistura casts, aritmetica e calculos em estilo booleano sobre colunas tipadas.
    /// </summary>
    public int RunCastCalculationMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    TRIM(CAST(Id AS CHAR(10))) AS IdText,
    TRIM(CAST(COALESCE(Age, 0) AS CHAR(10))) AS AgeText,
    TRIM(CAST(COALESCE(Age, 0) + 5 AS CHAR(10))) AS AgePlusFiveText,
    TRIM(CAST(ROUND(Balance, 0) AS CHAR(10))) AS BalanceRoundedText,
    TRIM(CAST(ROUND(Balance * 2, 0) AS CHAR(10))) AS BalanceDoubleText,
    TRIM(CAST(ABS(COALESCE(Age, 0) - 30) AS CHAR(10))) AS AgeDeltaText,
    TRIM(CAST(CASE WHEN Email IS NULL THEN 0 ELSE 1 END AS CHAR(1))) AS EmailFlagText,
    TRIM(CAST(CASE WHEN Balance > 15 THEN 0 ELSE 1 END AS CHAR(1))) AS BalanceNotGt15Text,
    TRIM(CAST(COALESCE(Age, 0) + ROUND(Balance, 0) AS CHAR(10))) AS AgePlusBalanceText
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateCastRow(reader, 1, "1", "31", "36", "11", "21", "1", "1", "1", "42");

        Assert.True(reader.Read());
        ValidateCastRow(reader, 2, "2", "27", "32", "20", "40", "3", "0", "0", "47");

        Assert.True(reader.Read());
        ValidateCastRow(reader, 3, "3", "0", "5", "5", "10", "30", "1", "1", "5");

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends null handling, comparisons, and predicates over typed columns.
    /// PT: Executa uma consulta grande que mistura tratamento de null, comparacoes e predicados sobre colunas tipadas.
    /// </summary>
    public int RunNullComparisonMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
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
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateNullComparisonRow(reader, 1, 0, 1, 0, 0, 1, 1, 0, 1, 1, 1, 0, 1);

        Assert.True(reader.Read());
        ValidateNullComparisonRow(reader, 2, 1, 0, 0, 1, 0, 0, 1, 1, 0, 0, 1, 0);

        Assert.True(reader.Read());
        ValidateNullComparisonRow(reader, 3, 0, 1, 1, 0, 0, 1, 0, 1, 1, 0, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends text lengths, trimming, and comparisons over typed columns.
    /// PT: Executa uma consulta grande que mistura comprimentos de texto, trim e comparacoes sobre colunas tipadas.
    /// </summary>
    public int RunTextLengthMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var nameLenExpr = Dialect.StringLengthExpression("Name");
        var upperNameLenExpr = Dialect.StringLengthExpression("UPPER(Name)");
        var trimNameLenExpr = Dialect.StringLengthExpression("TRIM(Name)");
        var emailLenExpr = Dialect.StringLengthExpression("COALESCE(Email, 'missing@example.com')");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    TRIM(CAST({nameLenExpr} AS CHAR(10))) AS NameLenText,
    TRIM(CAST({emailLenExpr} AS CHAR(10))) AS EmailLenText,
    TRIM(CAST({upperNameLenExpr} AS CHAR(10))) AS UpperNameLenText,
    TRIM(CAST({trimNameLenExpr} AS CHAR(10))) AS TrimmedNameLenText,
    TRIM(CAST({emailLenExpr} - {nameLenExpr} AS CHAR(10))) AS EmailMinusNameText,
    CASE WHEN {nameLenExpr} >= 5 THEN 1 ELSE 0 END AS NameLenGe5,
    CASE WHEN {emailLenExpr} >= 17 THEN 1 ELSE 0 END AS EmailLenGe17,
    CASE WHEN {trimNameLenExpr} = {nameLenExpr} THEN 1 ELSE 0 END AS TrimmedNameSameLen,
    CASE WHEN {upperNameLenExpr} = {nameLenExpr} THEN 1 ELSE 0 END AS UpperNameSameLen
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTextLengthRow(reader, 1, "5", "17", "5", "5", "12", 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTextLengthRow(reader, 2, "3", "19", "3", "3", "16", 0, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateTextLengthRow(reader, 3, "5", "17", "5", "5", "12", 1, 1, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large projection query that blends case conversion, trimming, prefix extraction, and text predicates over typed columns.
    /// PT: Executa uma consulta grande que mistura conversao de caixa, trim, extracao de prefixo e predicados de texto sobre colunas tipadas.
    /// </summary>
    public int RunTextCaseMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        var namePrefixExpr = Dialect.StringPrefixExpression("Name", 2);
        var nameLenExpr = Dialect.StringLengthExpression("Name");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Id,
    UPPER(Name) AS NameUpper,
    LOWER(Name) AS NameLower,
    TRIM(Name) AS NameTrimmed,
    {namePrefixExpr} AS NamePrefix,
    TRIM(CAST({nameLenExpr} AS CHAR(10))) AS NameLenText,
    CASE WHEN UPPER(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyUpper,
    CASE WHEN LOWER(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyLower,
    CASE WHEN TRIM(Name) = Name THEN 1 ELSE 0 END AS IsAlreadyTrimmed,
    CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END AS StartsWithA,
    CASE WHEN Name LIKE '%a' THEN 1 ELSE 0 END AS EndsWithA,
    COALESCE(Email, 'missing@example.com') AS EmailOrDefault,
    CASE WHEN Email IS NULL THEN 1 ELSE 0 END AS EmailIsNull
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTextCaseRow(reader, 1, "ALICE", "alice", "Alice", "Al", "5", 0, 0, 1, 1, 0, "alice@example.com", 0);

        Assert.True(reader.Read());
        ValidateTextCaseRow(reader, 2, "BOB", "bob", "Bob", "Bo", "3", 0, 0, 1, 0, 1, "missing@example.com", 1);

        Assert.True(reader.Read());
        ValidateTextCaseRow(reader, 3, "CARLA", "carla", "Carla", "Ca", "5", 0, 0, 1, 0, 1, "carla@example.com", 0);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a large predicate query that blends LIKE, NOT LIKE, BETWEEN, and null checks over typed columns.
    /// PT: Executa uma consulta grande de predicados que mistura LIKE, NOT LIKE, BETWEEN e verificacoes de null sobre colunas tipadas.
    /// </summary>
    public int RunTypedFieldPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
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
FROM {tableName}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateTypedPredicateRow(reader, 1, "Alice", 1, 1, 0, 1, 0, 1, 0);

        Assert.True(reader.Read());
        ValidateTypedPredicateRow(reader, 2, "Bob", 0, 1, 1, 0, 1, 0, 0);

        Assert.True(reader.Read());
        ValidateTypedPredicateRow(reader, 3, "Carla", 0, 1, 0, 1, 0, 1, 1);

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        return 3;
    }

    /// <summary>
    /// EN: Executes a compound predicate query that blends OR, AND, LIKE, and null checks over typed columns.
    /// PT: Executa uma consulta de predicado composto que mistura OR, AND, LIKE e verificacoes de null sobre colunas tipadas.
    /// </summary>
    public int RunTypedFieldCompoundPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";

        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (1, 'Alice', 'alice@example.com', 31, 10.50, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (2, 'Bob', NULL, 27, 20.25, NULL, NULL)
""");
        ExecuteNonQuery($"""
INSERT INTO {tableName} (Id, Name, Email, Age, Balance, UpdatedAt, ProfileJson) VALUES (3, 'Carla', 'carla@example.com', NULL, 5.00, NULL, NULL)
""");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT COUNT(*)
FROM {tableName}
WHERE (Email IS NULL OR Age IS NULL)
  AND (Name LIKE 'B%' OR Name LIKE 'C%')
""";

        var value = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(2, value);
        GC.KeepAlive(tableName);
        return value;
    }

    private static void ValidateTypedCalculationRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIdCast, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedStartsWithA, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEndsWithA, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeCast, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgePlusFive, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceWithTax, Convert.ToDecimal(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceThird, Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceGt15, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNull, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpdatedAtIsNull, Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtPresent, Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture));
        Assert.Equal(expectedProfileJsonIsNull, Convert.ToInt32(reader.GetValue(13), CultureInfo.InvariantCulture));
    }

    private static void ValidateJsonFieldRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedProfileName, GetStringOrNull(reader, 2));
        Assert.Equal(expectedProfileNameUpper, GetStringOrNull(reader, 3));
        Assert.Equal(expectedProfileNameOrDefault, GetStringOrNull(reader, 4));
        Assert.Equal(expectedProfileNamePrefix, GetStringOrNull(reader, 5));
        Assert.Equal(expectedIsAlice, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedJsonProfileIsNull, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedProfileJsonIsNull, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalancePlusAge, Convert.ToDecimal(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateTemporalFieldRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtPresent, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpdatedAtIsNull, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNowPresent, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNextDayAfterNow, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalancePlusAge, Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeDelta, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailOrDefault, Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNamePrefix, Convert.ToString(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateTemporalComparisonRow(
        DbDataReader reader,
        int expectedId,
        int expectedCreatedAtBeforeNow,
        int expectedNextDayAfterCreatedAt,
        int expectedNextDayAfterNow,
        int expectedFallbackAtLeastCreatedAt,
        int expectedUpdatedAtIsNull)
    {
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtBeforeNow, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNextDayAfterCreatedAt, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNextDayAfterNow, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFallbackAtLeastCreatedAt, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpdatedAtIsNull, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
    }

    private static void ValidateTemporalArithmeticRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtBeforeNow, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTomorrowAfterCreatedAt, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTomorrowAfterNow, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFallbackBeforeTomorrow, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFallbackAtLeastCreatedAt, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpdatedAtIsNull, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtIsNotNull, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
    }

    private static void ValidateCastRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIdText, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgePlusFiveText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceRoundedText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceDoubleText, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeDeltaText, Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailFlagText, Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceNotGt15Text, Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgePlusBalanceText, Convert.ToString(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateNullComparisonRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNull, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNotNull, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeIsNull, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeBetween20And30, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameStartsWithA, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameEndsWithA, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNullIfBob, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailLooksLikeExample, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIdIn13, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIdNotIn2, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceGt15, Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceLe1050, Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture));
    }

    private static void ValidateTextLengthRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenText, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailLenText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpperNameLenText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTrimmedNameLenText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailMinusNameText, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenGe5, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailLenGe17, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTrimmedNameSameLen, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpperNameSameLen, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateTextCaseRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameUpper, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLower, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameTrimmed, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNamePrefix, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenText, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIsAlreadyUpper, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIsAlreadyLower, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIsAlreadyTrimmed, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedStartsWithA, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEndsWithA, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailOrDefault, Convert.ToString(reader.GetValue(11), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNull, Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture));
    }

    private static void ValidateTypedPredicateRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedStartsWithA, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNotEndsWithZ, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeBetween20And30, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceBetween5And20, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNull, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNotNull, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeIsNull, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
    }

    private static string? GetStringOrNull(DbDataReader reader, int ordinal)
    {
        var rawValue = reader.GetValue(ordinal);
        return rawValue is DBNull ? null : Convert.ToString(rawValue, CultureInfo.InvariantCulture);
    }

    private static void ValidateTypedFieldRow(
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
        Assert.Equal(expectedId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpperName, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedLowerName, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailOrDefault, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalanceRounded, Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBalancePlus, Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeOrZero, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAgeDelta, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedEmailIsNull, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUpdatedAtIsNull, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
        Assert.Equal(expectedProfileJsonIsNull, Convert.ToInt32(reader.GetValue(11), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCreatedAtPresent, Convert.ToInt32(reader.GetValue(12), CultureInfo.InvariantCulture));
    }

    private void ExpectIntScalar(string sql, int expected, string label)
    {
        var value = Convert.ToInt32(ExecuteScalar(sql), CultureInfo.InvariantCulture);
        if (value != expected)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Dialect.DisplayName}: {value}.");
        }
    }

    private void ExpectDecimalScalar(string sql, decimal expected, string label)
    {
        var value = Convert.ToDecimal(ExecuteScalar(sql), CultureInfo.InvariantCulture);
        if (value != expected)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Dialect.DisplayName}: {value}.");
        }
    }

    private void ExpectStringScalar(string sql, string expected, string label)
    {
        var rawValue = ExecuteScalar(sql);
        var value = rawValue is null or DBNull ? null : Convert.ToString(rawValue, CultureInfo.InvariantCulture);
        if (!string.Equals(value, expected, StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        }
    }

    private void ExpectNullScalar(string sql, string label)
    {
        var rawValue = ExecuteScalar(sql);
        if (rawValue is not null and not DBNull)
        {
            throw new InvalidOperationException($"Unexpected {label} result for {Dialect.DisplayName}: {Convert.ToString(rawValue, CultureInfo.InvariantCulture)}.");
        }
    }
}

