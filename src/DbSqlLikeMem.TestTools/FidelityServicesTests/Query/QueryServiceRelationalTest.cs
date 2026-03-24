namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Executes a large grouped join query over users and orders and validates the projected rows.
    /// PT: Executa uma consulta grande com junção agrupada entre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public int RunJoinTypedExpressionMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    UPPER(u.Name) AS UserNameUpper,
    LOWER(u.Name) AS UserNameLower,
    COUNT(o.Id) AS OrderCount,
    SUM(o.Quantity) AS TotalQuantity,
    ROUND(SUM(o.Amount), 2) AS TotalAmount,
    ROUND(AVG(o.Amount), 2) AS AvgAmount,
    COALESCE(MIN(o.Note), 'none') AS FirstNote,
    MAX(o.Note) AS LastNote,
    CASE WHEN COUNT(o.Id) > 1 THEN 1 ELSE 0 END AS HasMultipleOrders,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS AmountAtLeastThree
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinTypedRow(reader, 1, "ALICE", "alice", 2, 3, 4.00m, 2.00m, "A", "B", 1, 1);

        Assert.True(reader.Read());
        ValidateJoinTypedRow(reader, 2, "BOB", "bob", 1, 4, 5.50m, 5.50m, "C", "C", 0, 1);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 2;
    }

    /// <summary>
    /// EN: Executes a left join aggregate query that keeps users without orders and validates null-handling behavior.
    /// PT: Executa uma consulta agregada com left join que preserva usuarios sem pedidos e valida o tratamento de null.
    /// </summary>
    public int RunJoinNullAggregateMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    COUNT(o.Id) AS OrderCount,
    COALESCE(SUM(o.Quantity), 0) AS TotalQuantity,
    COALESCE(ROUND(SUM(o.Amount), 2), 0) AS TotalAmount,
    COALESCE(MIN(o.Note), 'none') AS FirstNote,
    CASE WHEN COUNT(o.Id) = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN SUM(o.Amount) IS NULL THEN 1 ELSE 0 END AS AmountIsNull,
    CASE WHEN MAX(o.Quantity) > 1 THEN 1 ELSE 0 END AS HasLargeQuantity
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinNullAggregateRow(reader, 1, "Alice", 2, 3, 4.00m, "A", 0, 0, 1);

        Assert.True(reader.Read());
        ValidateJoinNullAggregateRow(reader, 2, "Bob", 1, 4, 5.50m, "C", 0, 0, 1);

        Assert.True(reader.Read());
        ValidateJoinNullAggregateRow(reader, 3, "Carla", 0, 0, 0.00m, "none", 1, 1, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped left join query that combines casts, null handling, and aggregate formatting.
    /// PT: Executa uma consulta agrupada com left join que combina casts, tratamento de null e formatacao de agregados.
    /// </summary>
    public int RunJoinCastNullMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM(CAST(COUNT(o.Id) AS CHAR(10))) AS OrderCountText,
    TRIM(CAST(COALESCE(SUM(o.Quantity), 0) AS CHAR(10))) AS TotalQuantityText,
    TRIM(CAST(COALESCE(ROUND(SUM(o.Amount), 2), 0) AS CHAR(20))) AS TotalAmountText,
    CASE WHEN COUNT(o.Id) = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN MIN(o.Note) IS NULL THEN 1 ELSE 0 END AS NotesAreNull,
    CASE WHEN MAX(o.Note) IS NOT NULL THEN 1 ELSE 0 END AS HasNote,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS MeetsAmountThreshold
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinCastNullRow(reader, 1, "Alice", "2", "3", "4.00", 0, 0, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinCastNullRow(reader, 2, "Bob", "1", "4", "5.50", 0, 0, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinCastNullRow(reader, 3, "Carla", "0", "0", "0.00", 1, 1, 0, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped left join query that casts numeric aggregates to text and combines them with comparisons.
    /// PT: Executa uma consulta agrupada com left join que converte agregados numericos para texto e os combina com comparacoes.
    /// </summary>
    public int RunJoinCastTextComparisonMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM(CAST(COUNT(o.Id) AS CHAR(10))) AS OrderCountText,
    TRIM(CAST(COALESCE(SUM(o.Quantity), 0) AS CHAR(10))) AS TotalQuantityText,
    TRIM(CAST(COALESCE(ROUND(SUM(o.Amount), 2), 0) AS CHAR(20))) AS TotalAmountText,
    CASE WHEN TRIM(CAST(COUNT(o.Id) AS CHAR(10))) = '0' THEN 1 ELSE 0 END AS CountTextIsZero,
    CASE WHEN TRIM(CAST(COALESCE(SUM(o.Quantity), 0) AS CHAR(10))) <> '0' THEN 1 ELSE 0 END AS QuantityTextNonZero,
    CASE WHEN COALESCE(MIN(o.Note), 'none') = 'none' THEN 1 ELSE 0 END AS NotesAreMissing,
    CASE WHEN MAX(o.Note) IS NOT NULL THEN 1 ELSE 0 END AS HasAnyNote
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinCastTextRow(reader, 1, "Alice", "2", "3", "4.00", 0, 1, 0, 1);

        Assert.True(reader.Read());
        ValidateJoinCastTextRow(reader, 2, "Bob", "1", "4", "5.50", 0, 1, 0, 1);

        Assert.True(reader.Read());
        ValidateJoinCastTextRow(reader, 3, "Carla", "0", "0", "0.00", 1, 0, 1, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped join query with HAVING filters and validates casted aggregate outputs.
    /// PT: Executa uma consulta agrupada com filtros HAVING e valida saidas agregadas convertidas.
    /// </summary>
    public int RunJoinHavingCastMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM(CAST(COUNT(o.Id) AS CHAR(10))) AS OrderCountText,
    TRIM(CAST(COALESCE(SUM(o.Quantity), 0) AS CHAR(10))) AS TotalQuantityText,
    TRIM(CAST(COALESCE(ROUND(SUM(o.Amount), 2), 0) AS CHAR(20))) AS TotalAmountText,
    CASE WHEN COUNT(o.Id) >= 2 THEN 1 ELSE 0 END AS HasTwoOrMoreOrders,
    CASE WHEN SUM(o.Amount) >= 4 THEN 1 ELSE 0 END AS AmountAtLeastFour,
    CASE WHEN MIN(o.Note) = 'A' THEN 1 ELSE 0 END AS StartsAtA
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
HAVING COUNT(o.Id) >= 2
   AND SUM(o.Amount) >= 4
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinHavingCastRow(reader, 1, "Alice", "2", "3", "4.00", 1, 1, 1);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 1;
    }

    /// <summary>
    /// EN: Executes a grouped join query that mixes string-length expressions with numeric conversions and aggregates.
    /// PT: Executa uma consulta agrupada que mistura expressoes de comprimento de texto com conversoes numericas e agregados.
    /// </summary>
    public int RunJoinLengthNumericMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        var nameLenExpr = Dialect.StringLengthExpression("u.Name");
        var noteLenExpr = Dialect.StringLengthExpression("o.Note");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM(CAST({nameLenExpr} AS CHAR(10))) AS NameLenText,
    TRIM(CAST(COALESCE(SUM(o.Quantity), 0) AS CHAR(10))) AS TotalQuantityText,
    TRIM(CAST(COALESCE(ROUND(SUM(o.Amount), 2), 0) AS CHAR(20))) AS TotalAmountText,
    TRIM(CAST(COALESCE(MAX({noteLenExpr}), 0) AS CHAR(10))) AS MaxNoteLenText,
    CASE WHEN {nameLenExpr} >= 4 THEN 1 ELSE 0 END AS NameLenGe4,
    CASE WHEN COALESCE(SUM(o.Quantity), 0) >= 3 THEN 1 ELSE 0 END AS QuantityGe3,
    CASE WHEN COALESCE(ROUND(SUM(o.Amount), 2), 0) >= 4 THEN 1 ELSE 0 END AS AmountGe4,
    CASE WHEN COALESCE(MAX({noteLenExpr}), 0) >= 1 THEN 1 ELSE 0 END AS HasNotes
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinLengthNumericRow(reader, 1, "Alice", "5", "3", "4.00", "1", 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinLengthNumericRow(reader, 2, "Bob", "3", "4", "5.50", "1", 0, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinLengthNumericRow(reader, 3, "Carla", "5", "0", "0.00", "0", 1, 0, 0, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped join query that blends string case, string length, and aggregate comparisons.
    /// PT: Executa uma consulta agrupada que mistura caixa de texto, comprimento de texto e comparacoes agregadas.
    /// </summary>
    public int RunJoinTextCaseLengthMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        var nameLenExpr = Dialect.StringLengthExpression("u.Name");
        var noteLenExpr = Dialect.StringLengthExpression("o.Note");

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    UPPER(u.Name) AS NameUpper,
    LOWER(u.Name) AS NameLower,
    TRIM(u.Name) AS NameTrimmed,
    TRIM(CAST({nameLenExpr} AS CHAR(10))) AS NameLenText,
    TRIM(CAST(COALESCE(MAX({noteLenExpr}), 0) AS CHAR(10))) AS MaxNoteLenText,
    CASE WHEN {nameLenExpr} >= 4 THEN 1 ELSE 0 END AS NameLenGe4,
    CASE WHEN UPPER(u.Name) = u.Name THEN 1 ELSE 0 END AS IsUpperAlready,
    CASE WHEN LOWER(u.Name) = u.Name THEN 1 ELSE 0 END AS IsLowerAlready,
    CASE WHEN COUNT(o.Id) >= 2 THEN 1 ELSE 0 END AS TwoOrMoreOrders,
    CASE WHEN SUM(o.Quantity) >= 3 THEN 1 ELSE 0 END AS QuantityGe3
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinTextCaseLengthRow(reader, 1, "ALICE", "alice", "Alice", "5", "1", 1, 0, 0, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinTextCaseLengthRow(reader, 2, "BOB", "bob", "Bob", "3", "1", 0, 0, 0, 0, 0);

        Assert.True(reader.Read());
        ValidateJoinTextCaseLengthRow(reader, 3, "CARLA", "carla", "Carla", "5", "0", 1, 0, 0, 0, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped left join report that combines distinct counts, CASE expressions, and repeated values.
    /// PT: Executa um relatorio agrupado com left join que combina contagens distintas, expressoes CASE e valores repetidos.
    /// </summary>
    public int RunJoinDistinctCaseMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    COUNT(o.Id) AS OrderCount,
    COUNT(DISTINCT o.Note) AS DistinctNoteCount,
    SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) AS NoteACount,
    CASE WHEN COUNT(DISTINCT o.Note) >= 2 THEN 1 ELSE 0 END AS HasMultipleDistinctNotes,
    CASE WHEN SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END AS HasRepeatedNoteA
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinDistinctCaseRow(reader, 2, 1, 1, 0, 0, 0);

        Assert.True(reader.Read());
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped left join report with HAVING and distinct note counts.
    /// PT: Executa um relatorio agrupado com left join, HAVING e contagens distintas de notas.
    /// </summary>
    public int RunJoinDistinctHavingMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    COUNT(o.Id) AS OrderCount,
    COUNT(DISTINCT o.Note) AS DistinctNoteCount,
    SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) AS NoteACount,
    CASE WHEN COUNT(DISTINCT o.Note) >= 2 THEN 1 ELSE 0 END AS HasMultipleDistinctNotes
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id
HAVING COUNT(DISTINCT o.Note) >= 2 OR COUNT(o.Id) = 0
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 0);

        Assert.True(reader.Read());
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 2;
    }

    /// <summary>
    /// EN: Executes a grouped left join query that blends temporal comparisons with aggregate counts.
    /// PT: Executa uma consulta agrupada com left join que mistura comparacoes temporais com contagens agregadas.
    /// </summary>
    public int RunJoinTemporalMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    COUNT(o.Id) AS OrderCount,
    CASE WHEN COUNT(o.Id) = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN COALESCE(MIN(o.OrderedAt), u.CreatedAt) <= {nowExpr} THEN 1 ELSE 0 END AS MinOrderedBeforeNow,
    CASE WHEN COALESCE(MAX(o.OrderedAt), u.CreatedAt) < {nextDayExpr} THEN 1 ELSE 0 END AS MaxOrderedBeforeNextDay,
    SUM(CASE WHEN o.DeliveredAt IS NULL THEN 1 ELSE 0 END) AS PendingDeliveries,
    CASE WHEN u.CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS UserCreatedBeforeNow
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.CreatedAt
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinTemporalRow(reader, 1, 2, 0, 1, 1, 2, 1);

        Assert.True(reader.Read());
        ValidateJoinTemporalRow(reader, 2, 1, 0, 1, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinTemporalRow(reader, 3, 0, 1, 1, 1, 1, 1);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a joined window-function query over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public int RunJoinWindowMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    COUNT(o.Id) OVER (PARTITION BY u.Id) AS OrdersPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinWindowRow(reader, 1, 10, 1, 2, null);

        Assert.True(reader.Read());
        ValidateJoinWindowRow(reader, 1, 11, 2, 2, "A");

        Assert.True(reader.Read());
        ValidateJoinWindowRow(reader, 2, 12, 1, 1, null);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a joined window-function query with temporal comparisons over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela e comparacoes temporais em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public int RunJoinWindowTemporalMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    COUNT(o.Id) OVER (PARTITION BY u.Id) AS OrdersPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote,
    CASE WHEN o.OrderedAt <= {nowExpr} THEN 1 ELSE 0 END AS OrderedBeforeNow,
    CASE WHEN {nextDayExpr} > o.OrderedAt THEN 1 ELSE 0 END AS NextDayAfterOrder,
    CASE WHEN u.CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS UserCreatedBeforeNow
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinWindowTemporalRow(reader, 1, 10, 1, 2, null, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinWindowTemporalRow(reader, 1, 11, 2, 2, "A", 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinWindowTemporalRow(reader, 2, 12, 1, 1, null, 1, 1, 1);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a joined window-function query with temporal and aggregate comparisons over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela, comparacoes temporais e agregadas em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public int RunJoinWindowAggregateTemporalMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    COUNT(o.Id) OVER (PARTITION BY u.Id) AS OrdersPerUser,
    SUM(o.Quantity) OVER (PARTITION BY u.Id) AS QuantityPerUser,
    ROUND(SUM(o.Amount) OVER (PARTITION BY u.Id), 2) AS AmountPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote,
    CASE WHEN o.OrderedAt <= {nowExpr} THEN 1 ELSE 0 END AS OrderedBeforeNow,
    CASE WHEN {nextDayExpr} > o.OrderedAt THEN 1 ELSE 0 END AS NextDayAfterOrder,
    CASE WHEN u.CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS UserCreatedBeforeNow
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 10, 1, 2, 3, 4.00m, null, 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 11, 2, 2, 3, 4.00m, "A", 1, 1, 1);

        Assert.True(reader.Read());
        ValidateJoinWindowAggregateTemporalRow(reader, 2, 12, 1, 1, 4, 5.50m, null, 1, 1, 1);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    private static void ValidateJoinTypedRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserNameUpper,
        string expectedUserNameLower,
        int expectedOrderCount,
        int expectedTotalQuantity,
        decimal expectedTotalAmount,
        decimal expectedAvgAmount,
        string expectedFirstNote,
        string expectedLastNote,
        int expectedHasMultipleOrders,
        int expectedAmountAtLeastThree)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserNameUpper, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserNameLower, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCount, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantity, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmount, Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAvgAmount, Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFirstNote, Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedLastNote, Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasMultipleOrders, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAmountAtLeastThree, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinNullAggregateRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        int expectedOrderCount,
        int expectedTotalQuantity,
        decimal expectedTotalAmount,
        string expectedFirstNote,
        int expectedHasNoOrders,
        int expectedAmountIsNull,
        int expectedHasLargeQuantity)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCount, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantity, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmount, Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFirstNote, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNoOrders, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAmountIsNull, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasLargeQuantity, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinCastNullRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        string expectedOrderCountText,
        string expectedTotalQuantityText,
        string expectedTotalAmountText,
        int expectedHasNoOrders,
        int expectedNotesAreNull,
        int expectedHasNote,
        int expectedMeetsAmountThreshold)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCountText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantityText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmountText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNoOrders, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNotesAreNull, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNote, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedMeetsAmountThreshold, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinCastTextRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        string expectedOrderCountText,
        string expectedTotalQuantityText,
        string expectedTotalAmountText,
        int expectedCountTextIsZero,
        int expectedQuantityTextNonZero,
        int expectedNotesAreMissing,
        int expectedHasAnyNote)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCountText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantityText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmountText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCountTextIsZero, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedQuantityTextNonZero, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNotesAreMissing, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasAnyNote, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinHavingCastRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        string expectedOrderCountText,
        string expectedTotalQuantityText,
        string expectedTotalAmountText,
        int expectedHasTwoOrMoreOrders,
        int expectedAmountAtLeastFour,
        int expectedStartsAtA)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCountText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantityText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmountText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasTwoOrMoreOrders, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAmountAtLeastFour, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedStartsAtA, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinLengthNumericRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        string expectedNameLenText,
        string expectedTotalQuantityText,
        string expectedTotalAmountText,
        string expectedMaxNoteLenText,
        int expectedNameLenGe4,
        int expectedQuantityGe3,
        int expectedAmountGe4,
        int expectedHasNotes)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalQuantityText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalAmountText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedMaxNoteLenText, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenGe4, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedQuantityGe3, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAmountGe4, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNotes, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinTextCaseLengthRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedNameUpper,
        string expectedNameLower,
        string expectedNameTrimmed,
        string expectedNameLenText,
        string expectedMaxNoteLenText,
        int expectedNameLenGe4,
        int expectedIsUpperAlready,
        int expectedIsLowerAlready,
        int expectedTwoOrMoreOrders,
        int expectedQuantityGe3)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameUpper, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLower, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameTrimmed, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenText, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedMaxNoteLenText, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNameLenGe4, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIsUpperAlready, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedIsLowerAlready, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTwoOrMoreOrders, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(expectedQuantityGe3, Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinDistinctCaseRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderCount,
        int expectedDistinctNoteCount,
        int expectedNoteACount,
        int expectedHasMultipleDistinctNotes,
        int expectedHasRepeatedNoteA)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCount, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedDistinctNoteCount, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNoteACount, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasMultipleDistinctNotes, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasRepeatedNoteA, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
    }

    private static void ValidateSelectScalarCaseRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        int expectedOrderCount,
        int expectedHasNoOrders,
        int expectedHasManyOrders)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCount, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNoOrders, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasManyOrders, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinWindowRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderId,
        int expectedRowNumberInUser,
        int expectedOrdersPerUser,
        string? expectedPreviousNote)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderId, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedRowNumberInUser, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrdersPerUser, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));

        var previousNote = reader.IsDBNull(4) ? null : Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture);
        Assert.Equal(expectedPreviousNote, previousNote);
    }

    private static void ValidateJoinWindowTemporalRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderId,
        int expectedRowNumberInUser,
        int expectedOrdersPerUser,
        string? expectedPreviousNote,
        int expectedOrderedBeforeNow,
        int expectedNextDayAfterOrder,
        int expectedUserCreatedBeforeNow)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderId, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedRowNumberInUser, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrdersPerUser, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));

        var previousNote = reader.IsDBNull(4) ? null : Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture);
        Assert.Equal(expectedPreviousNote, previousNote);
        Assert.Equal(expectedOrderedBeforeNow, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNextDayAfterOrder, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserCreatedBeforeNow, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinWindowAggregateTemporalRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderId,
        int expectedRowNumberInUser,
        int expectedOrdersPerUser,
        int expectedQuantityPerUser,
        decimal expectedAmountPerUser,
        string? expectedPreviousNote,
        int expectedOrderedBeforeNow,
        int expectedNextDayAfterOrder,
        int expectedUserCreatedBeforeNow)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderId, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedRowNumberInUser, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrdersPerUser, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedQuantityPerUser, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAmountPerUser, Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture));

        var previousNote = reader.IsDBNull(6) ? null : Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture);
        Assert.Equal(expectedPreviousNote, previousNote);
        Assert.Equal(expectedOrderedBeforeNow, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(expectedNextDayAfterOrder, Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserCreatedBeforeNow, Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture));
    }

    private static void ValidateJoinTemporalRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderCount,
        int expectedHasNoOrders,
        int expectedMinOrderedBeforeNow,
        int expectedMaxOrderedBeforeNextDay,
        int expectedPendingDeliveries,
        int expectedUserCreatedBeforeNow)
    {
        Assert.Equal(expectedUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedOrderCount, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasNoOrders, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedMinOrderedBeforeNow, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedMaxOrderedBeforeNextDay, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedPendingDeliveries, Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedUserCreatedBeforeNow, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Counts the rows returned by a select over the configured users table.
    /// PT: Conta as linhas retornadas por um select na tabela de usuarios configurada.
    /// </summary>
    public int RunRowCountAfterSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var count = CountReaderRows($"SELECT * FROM {users}");
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected select rowcount for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Executes a simple CTE query against the configured users table.
    /// PT: Executa uma consulta CTE simples na tabela de usuarios configurada.
    /// </summary>
    public int RunCteSimple(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CteSimple(users)), CultureInfo.InvariantCulture);
        if (value != 1)
        {
            throw new InvalidOperationException($"Unexpected CTE result for {Dialect.DisplayName}: {value}.");
        }

        return value;
    }

    /// <summary>
    /// EN: Executes a ROW_NUMBER window query against the configured users table.
    /// PT: Executa uma consulta de janela ROW_NUMBER na tabela de usuarios configurada.
    /// </summary>
    public int RunWindowRowNumber(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowRowNumber(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a LAG window query against the configured users table.
    /// PT: Executa uma consulta de janela LAG na tabela de usuarios configurada.
    /// </summary>
    public int RunWindowLag(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowLag(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a LEAD window query against the configured users table.
    /// PT: Executa uma consulta de janela LEAD na tabela de usuarios configurada.
    /// </summary>
    public int RunWindowLead(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowLead(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an EXISTS predicate query against the configured users and orders tables.
    /// PT: Executa uma consulta com predicado EXISTS nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectExistsPredicate(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectExistsPredicate(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate query against the configured users and orders tables.
    /// PT: Executa uma consulta com predicado NOT EXISTS nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectNotExistsPredicate(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectNotExistsPredicate(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a correlated count query against the configured users and orders tables.
    /// PT: Executa uma consulta de contagem correlacionada nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectCorrelatedCount(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectCorrelatedCount(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a per-row scalar subquery report with CASE expressions against the configured users and orders tables.
    /// PT: Executa um relatorio com subconsulta escalar por linha e expressoes CASE nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectScalarCaseMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = $"{users}";
        var ordersTable = $"{orders}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id) AS OrderCount,
    CASE WHEN (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id) = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN (SELECT COUNT(*) FROM {ordersTable} o WHERE o.{usersTable}Id = u.Id) >= 2 THEN 1 ELSE 0 END AS HasManyOrders
FROM {usersTable} u
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateSelectScalarCaseRow(reader, 1, "Alice", 3, 0, 1);

        Assert.True(reader.Read());
        ValidateSelectScalarCaseRow(reader, 2, "Bob", 1, 0, 0);

        Assert.True(reader.Read());
        ValidateSelectScalarCaseRow(reader, 3, "Carla", 0, 1, 0);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        GC.KeepAlive(ordersTable);
        return 3;
    }

    /// <summary>
    /// EN: Executes a GROUP BY HAVING query against the configured users and orders tables.
    /// PT: Executa uma consulta GROUP BY HAVING nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunGroupByHaving(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.GroupByHaving(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a UNION ALL projection query against the configured users table.
    /// PT: Executa uma consulta de projeção UNION ALL na tabela de usuarios configurada.
    /// </summary>
    public int RunUnionAllProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.UnionAllProjection(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a UNION projection query against the configured users table.
    /// PT: Executa uma consulta de projeção UNION na tabela de usuarios configurada.
    /// </summary>
    public int RunUnionDistinctProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.UnionDistinctProjection(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a DISTINCT projection query against the configured users table.
    /// PT: Executa uma consulta de projeção DISTINCT na tabela de usuarios configurada.
    /// </summary>
    public int RunDistinctProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.DistinctProjection(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a multi-join aggregate query against the configured users and orders tables.
    /// PT: Executa uma consulta agregada com multiplos joins nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunMultiJoinAggregate(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.MultiJoinAggregate(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a scalar subquery projection against the configured users and orders tables.
    /// PT: Executa uma projeção com subconsulta escalar nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public object? RunSelectScalarSubquery(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = ExecuteScalar(Dialect.SelectScalarSubquery(users, orders));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an IN subquery predicate against the configured users and orders tables.
    /// PT: Executa um predicado IN com subconsulta nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectInSubquery(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectInSubquery(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a NOT IN subquery predicate against the configured users and orders tables.
    /// PT: Executa um predicado NOT IN com subconsulta nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunSelectNotInSubquery(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectNotInSubquery(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a CROSS APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo CROSS APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunCrossApplyProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CrossApplyProjection(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an OUTER APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo OUTER APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public int RunOuterApplyProjection(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.OuterApplyProjection(users, orders)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a partition-pruning style select against the configured users table.
    /// PT: Executa um select no estilo partition pruning na tabela de usuarios configurada.
    /// </summary>
    public int RunPartitionPruningSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var value = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(*) FROM {users} WHERE Id BETWEEN 5 AND 10"), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a pivot-style count query against the configured users table.
    /// PT: Executa uma consulta de contagem no estilo pivot na tabela de usuarios configurada.
    /// </summary>
    public int RunPivotCount(params object[] pars)
    {
        var users = (string)pars[0];
        var sql = $"SELECT SUM(CASE WHEN Name LIKE 'A%' THEN 1 ELSE 0 END) + SUM(CASE WHEN Name LIKE 'B%' THEN 1 ELSE 0 END) FROM {users}";
        var value = Convert.ToInt32(ExecuteScalar(sql), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    private int CountReaderRows(string sql, DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        using var reader = command.ExecuteReader();
        var count = 0;
        while (reader.Read())
        {
            count++;
        }

        return count;
    }
}

