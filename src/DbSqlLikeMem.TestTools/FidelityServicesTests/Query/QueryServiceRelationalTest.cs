namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    private string BuildFirstNoteSubquery(string ordersTable, string usersTable, string orderByDirection)
        => Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure
            ? $"""
    SELECT TOP 1 o2.Note
    FROM {ordersTable} o2
    WHERE o2.{usersTable}Id = u.Id
    ORDER BY o2.Note {orderByDirection}
"""
            : Dialect.Provider == ProviderId.Oracle
                ? orderByDirection.Equals("ASC", StringComparison.OrdinalIgnoreCase)
                    ? $"""
    SELECT MIN(o2.Note)
    FROM {ordersTable} o2
    WHERE o2.{usersTable}Id = u.Id
"""
                    : $"""
    SELECT MAX(o2.Note)
    FROM {ordersTable} o2
    WHERE o2.{usersTable}Id = u.Id
"""
            : $"""
    SELECT o2.Note
    FROM {ordersTable} o2
    WHERE o2.{usersTable}Id = u.Id
    ORDER BY o2.Note {orderByDirection}
    LIMIT 1
""";

    private string BuildOrderCountExpression(string ordersTable, string usersTable)
        => Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure
            ? $"COUNT(o.{usersTable}Id)"
            : Dialect.Provider == ProviderId.Oracle
                ? $"""
(SELECT COUNT(*)
 FROM {ordersTable} o2
 WHERE o2.{usersTable}Id = u.Id)
"""
            : Dialect.Provider == ProviderId.Db2
                ? $"COUNT(o.{usersTable}Id)"
            : "COUNT(o.Id)";

    /// <summary>
    /// EN: Executes a large grouped join query over users and orders and validates the projected rows.
    /// PT: Executa uma consulta grande com junção agrupada entre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public int RunJoinTypedExpressionMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    UPPER(u.Name) AS UserNameUpper,
    LOWER(u.Name) AS UserNameLower,
    {orderCountExpr} AS OrderCount,
    SUM(o.Quantity) AS TotalQuantity,
    ROUND(SUM(o.Amount), 2) AS TotalAmount,
    ROUND(AVG(o.Amount), 2) AS AvgAmount,
    COALESCE(({BuildFirstNoteSubquery(ordersTable, usersTable, "ASC")}), 'none') AS FirstNote,
    ({BuildFirstNoteSubquery(ordersTable, usersTable, "DESC")}) AS LastNote,
    CASE WHEN {orderCountExpr} > 1 THEN 1 ELSE 0 END AS HasMultipleOrders,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS AmountAtLeastThree
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinTypedRow(reader, 1, "ALICE", "alice", 2, 3, 4.00m, 2.00m, "A", "B", 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinTypedRow(reader, 2, "BOB", "bob", 1, 4, 5.50m, 5.50m, "C", "C", 0, 1);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    {orderCountExpr} AS OrderCount,
    COALESCE(SUM(o.Quantity), 0) AS TotalQuantity,
    COALESCE(ROUND(SUM(o.Amount), 2), 0) AS TotalAmount,
    COALESCE(({BuildFirstNoteSubquery(ordersTable, usersTable, "ASC")}), 'none') AS FirstNote,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN SUM(o.Amount) IS NULL THEN 1 ELSE 0 END AS AmountIsNull,
    CASE WHEN MAX(o.Quantity) > 1 THEN 1 ELSE 0 END AS HasLargeQuantity
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 1, "Alice", 2, 3, 4.00m, "A", 0, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 2, "Bob", 1, 4, 5.50m, "C", 0, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 3, "Carla", 0, 0, 0.00m, "none", 1, 1, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Dialect.StringCastExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 20)}) AS TotalAmountText,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS NotesAreNull,
    CASE WHEN {orderCountExpr} > 0 THEN 1 ELSE 0 END AS HasNote,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS MeetsAmountThreshold
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinCastNullRow(reader, 1, "Alice", "2", "3", "4.00", 0, 0, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinCastNullRow(reader, 2, "Bob", "1", "4", "5.50", 0, 0, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinCastNullRow(reader, 3, "Carla", "0", "0", "0.00", 1, 1, 0, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Dialect.StringCastExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 20)}) AS TotalAmountText,
    CASE WHEN TRIM({Dialect.StringCastExpression(orderCountExpr, 10)}) = '0' THEN 1 ELSE 0 END AS CountTextIsZero,
    CASE WHEN TRIM({Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) <> '0' THEN 1 ELSE 0 END AS QuantityTextNonZero,
    CASE WHEN COALESCE(MIN(o.Note), 'none') = 'none' THEN 1 ELSE 0 END AS NotesAreMissing,
    CASE WHEN MAX(o.Note) IS NOT NULL THEN 1 ELSE 0 END AS HasAnyNote
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinCastTextRow(reader, 1, "Alice", "2", "3", "4.00", 0, 1, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinCastTextRow(reader, 2, "Bob", "1", "4", "5.50", 0, 1, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinCastTextRow(reader, 3, "Carla", "0", "0", "0.00", 1, 0, 1, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Dialect.StringCastExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 20)}) AS TotalAmountText,
    CASE WHEN {orderCountExpr} >= 2 THEN 1 ELSE 0 END AS HasTwoOrMoreOrders,
    CASE WHEN SUM(o.Amount) >= 4 THEN 1 ELSE 0 END AS AmountAtLeastFour,
    CASE WHEN MIN(o.Note) = 'A' THEN 1 ELSE 0 END AS StartsAtA
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
HAVING {orderCountExpr} >= 2
   AND SUM(o.Amount) >= 4
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinHavingCastRow(reader, 1, "Alice", "2", "3", "4.00", 1, 1, 1);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);

        var nameLenExpr = Dialect.Provider is ProviderId.Db2 or ProviderId.Oracle
            ? "MAX(LENGTH(u.Name))"
            : $"MAX({Dialect.StringLengthExpression("u.Name")})";
        var noteLenSource = Dialect.Provider == ProviderId.Db2 ? "Note" : "o.Note";
        var noteLenExpr = Dialect.StringLengthExpression(noteLenSource);
        var nameLenTextExpr = $"TRIM({Dialect.StringCastExpression(nameLenExpr, 10)})";
        var noteLenTextExpr = $"TRIM({Dialect.StringCastExpression($"COALESCE(MAX({noteLenExpr}), 0)", 10)})";
        var textMatchAlready = Dialect.Provider == ProviderId.Sqlite ? 0 : 1;

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    {nameLenTextExpr} AS NameLenText,
    TRIM({Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Dialect.StringCastExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 20)}) AS TotalAmountText,
    {noteLenTextExpr} AS MaxNoteLenText,
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

        reader.Read().Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 1, "Alice", "5", "3", "4.00", "1", 1, 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 2, "Bob", "3", "4", "5.50", "1", 0, 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 3, "Carla", "5", "0", "0.00", "0", 1, 0, 0, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        var nameLenExpr = Dialect.Provider is ProviderId.Db2 or ProviderId.Oracle
            ? "MAX(LENGTH(u.Name))"
            : $"MAX({Dialect.StringLengthExpression("u.Name")})";
        var noteLenSource = Dialect.Provider == ProviderId.Db2 ? "Note" : "o.Note";
        var noteLenExpr = Dialect.StringLengthExpression(noteLenSource);
        var nameLenTextExpr = $"TRIM({Dialect.StringCastExpression(nameLenExpr, 10)})";
        var noteLenTextExpr = $"TRIM({Dialect.StringCastExpression($"COALESCE(MAX({noteLenExpr}), 0)", 10)})";
        var textMatchAlready = Dialect.Provider == ProviderId.Sqlite ? 0 : 1;

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    UPPER(u.Name) AS NameUpper,
    LOWER(u.Name) AS NameLower,
    TRIM(u.Name) AS NameTrimmed,
    {nameLenTextExpr} AS NameLenText,
    {noteLenTextExpr} AS MaxNoteLenText,
    CASE WHEN {nameLenExpr} >= 4 THEN 1 ELSE 0 END AS NameLenGe4,
    CASE WHEN UPPER(u.Name) = u.Name THEN 1 ELSE 0 END AS IsUpperAlready,
    CASE WHEN LOWER(u.Name) = u.Name THEN 1 ELSE 0 END AS IsLowerAlready,
    CASE WHEN {orderCountExpr} >= 2 THEN 1 ELSE 0 END AS TwoOrMoreOrders,
    CASE WHEN SUM(o.Quantity) >= 3 THEN 1 ELSE 0 END AS QuantityGe3
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 1, "ALICE", "alice", "Alice", "5", "1", 1, textMatchAlready, textMatchAlready, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 2, "BOB", "bob", "Bob", "3", "1", 0, textMatchAlready, textMatchAlready, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 3, "CARLA", "carla", "Carla", "5", "0", 1, textMatchAlready, textMatchAlready, 0, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
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

        reader.Read().Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 2, 1, 1, 0, 0, 0);

        reader.Read().Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
    COUNT(DISTINCT o.Note) AS DistinctNoteCount,
    SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) AS NoteACount,
    CASE WHEN COUNT(DISTINCT o.Note) >= 2 THEN 1 ELSE 0 END AS HasMultipleDistinctNotes,
    CASE WHEN SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END AS HasRepeatedNoteA
FROM {usersTable} u
LEFT JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id
    HAVING COUNT(DISTINCT o.Note) >= 2 OR {orderCountExpr} = 0
ORDER BY u.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = BuildOrderCountExpression(ordersTable, usersTable);

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
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

        reader.Read().Should().BeTrue();
        ValidateJoinTemporalRow(reader, 1, 2, 0, 1, 1, 2, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinTemporalRow(reader, 2, 1, 0, 1, 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinTemporalRow(reader, 3, 0, 1, 1, 1, 1, 1);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = $"COUNT(o.{usersTable}Id)";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    {orderCountExpr} OVER (PARTITION BY u.Id) AS OrdersPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinWindowRow(reader, 1, 10, 1, 2, null);

        reader.Read().Should().BeTrue();
        ValidateJoinWindowRow(reader, 1, 11, 2, 2, "A");

        reader.Read().Should().BeTrue();
        ValidateJoinWindowRow(reader, 2, 12, 1, 1, null);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = $"COUNT(o.{usersTable}Id)";

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    {orderCountExpr} OVER (PARTITION BY u.Id) AS OrdersPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote,
    CASE WHEN o.OrderedAt <= {nowExpr} THEN 1 ELSE 0 END AS OrderedBeforeNow,
    CASE WHEN {nextDayExpr} > o.OrderedAt THEN 1 ELSE 0 END AS NextDayAfterOrder,
    CASE WHEN u.CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS UserCreatedBeforeNow
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 1, 10, 1, 2, null, 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 1, 11, 2, 2, "A", 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 2, 12, 1, 1, null, 1, 1, 1);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var orderCountExpr = $"COUNT(o.{usersTable}Id)";

        var nowExpr = Dialect.TemporalCurrentTimestampExpression();
        var nextDayExpr = Dialect.TemporalDateAddExpression();

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    {orderCountExpr} OVER (PARTITION BY u.Id) AS OrdersPerUser,
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

        reader.Read().Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 10, 1, 2, 2, 0.00m, null, 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 11, 2, 2, 2, 0.00m, "A", 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 2, 12, 1, 1, 1, 0.00m, null, 1, 1, 1);

        reader.Read().Should().BeFalse();
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserNameUpper);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedUserNameLower);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedOrderCount);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantity);
        Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedTotalAmount);
        Convert.ToDecimal(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedAvgAmount);
        Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedFirstNote);
        Convert.ToString(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedLastNote);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedHasMultipleOrders);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedAmountAtLeastThree);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedOrderCount);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantity);
        Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedTotalAmount);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedFirstNote);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedHasNoOrders);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedAmountIsNull);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedHasLargeQuantity);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedOrderCountText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantityText);
        GetAmountText(reader.GetValue(4)).Should().Be(expectedTotalAmountText);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedHasNoOrders);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNotesAreNull);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedHasNote);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedMeetsAmountThreshold);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedOrderCountText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantityText);
        GetAmountText(reader.GetValue(4)).Should().Be(expectedTotalAmountText);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedCountTextIsZero);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedQuantityTextNonZero);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedNotesAreMissing);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedHasAnyNote);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedOrderCountText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantityText);
        GetAmountText(reader.GetValue(4)).Should().Be(expectedTotalAmountText);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedHasTwoOrMoreOrders);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedAmountAtLeastFour);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedStartsAtA);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedNameLenText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedTotalQuantityText);
        GetAmountText(reader.GetValue(4)).Should().Be(expectedTotalAmountText);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedMaxNoteLenText);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNameLenGe4);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedQuantityGe3);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedAmountGe4);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedHasNotes);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedNameUpper);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedNameLower);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedNameTrimmed);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedNameLenText);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedMaxNoteLenText);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNameLenGe4);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedIsUpperAlready);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedIsLowerAlready);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedTwoOrMoreOrders);
        Convert.ToInt32(reader.GetValue(10), CultureInfo.InvariantCulture).Should().Be(expectedQuantityGe3);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedOrderCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDistinctNoteCount);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedNoteACount);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedHasMultipleDistinctNotes);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedHasRepeatedNoteA);
    }

    private static string? GetAmountText(object? value)
    {
        if (value is null or DBNull)
            return null;

        if (value is string text)
        {
            var normalizedText = NormalizeAmountText(text);
            if (normalizedText is not null)
                return normalizedText;
        }

        if (value is IConvertible)
        {
            try
            {
                return Convert.ToDecimal(value, CultureInfo.InvariantCulture).ToString("0.00", CultureInfo.InvariantCulture);
            }
            catch
            {
            }
        }

        var convertedText = Convert.ToString(value, CultureInfo.InvariantCulture);
        if (string.IsNullOrWhiteSpace(convertedText))
            return convertedText;

        return NormalizeAmountText(convertedText) ?? convertedText;
    }

    private static string? NormalizeAmountText(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return text;

        var trimmed = text!.Trim().Replace(',', '.');
        var sign = trimmed.StartsWith("-", StringComparison.Ordinal) ? "-" : string.Empty;
        var unsigned = sign.Length > 0 ? trimmed[1..] : trimmed;

        var parts = unsigned.Split(['.'], 2);
        if (parts.Length == 1)
        {
            return $"{sign}{parts[0]}.00";
        }

        if (parts.Length == 2)
        {
            var fractional = parts[1];
            if (fractional.Length == 0)
                return $"{sign}{parts[0]}.00";

            if (fractional.Length == 1)
                return $"{sign}{parts[0]}.{fractional}0";

            if (fractional.Length == 2)
                return $"{sign}{parts[0]}.{fractional}";
        }

        return trimmed;
    }

    private static void ValidateSelectScalarCaseRow(
        DbDataReader reader,
        int expectedUserId,
        string expectedUserName,
        int expectedOrderCount,
        int expectedHasNoOrders,
        int expectedHasManyOrders)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedUserName);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedOrderCount);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedHasNoOrders);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedHasManyOrders);
    }

    private static void ValidateJoinWindowRow(
        DbDataReader reader,
        int expectedUserId,
        int expectedOrderId,
        int expectedRowNumberInUser,
        int expectedOrdersPerUser,
        string? expectedPreviousNote)
    {
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedOrderId);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedRowNumberInUser);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedOrdersPerUser);

        var previousNote = reader.IsDBNull(4) ? null : Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture);
        previousNote.Should().Be(expectedPreviousNote);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedOrderId);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedRowNumberInUser);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedOrdersPerUser);

        var previousNote = reader.IsDBNull(4) ? null : Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture);
        previousNote.Should().Be(expectedPreviousNote);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedOrderedBeforeNow);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedNextDayAfterOrder);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedUserCreatedBeforeNow);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedOrderId);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedRowNumberInUser);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedOrdersPerUser);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedQuantityPerUser);
        Convert.ToDecimal(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedAmountPerUser);

        var previousNote = reader.IsDBNull(6) ? null : Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture);
        previousNote.Should().Be(expectedPreviousNote);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedOrderedBeforeNow);
        Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(expectedNextDayAfterOrder);
        Convert.ToInt32(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(expectedUserCreatedBeforeNow);
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
        Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedUserId);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedOrderCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedHasNoOrders);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedMinOrderedBeforeNow);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedMaxOrderedBeforeNextDay);
        Convert.ToInt64(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedPendingDeliveries);
        Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedUserCreatedBeforeNow);
    }

    /// <summary>
    /// EN: Counts the rows returned by a select over the configured users table.
    /// PT: Conta as linhas retornadas por um select na tabela de usuarios configurada.
    /// </summary>
    public int RunRowCountAfterSelect(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);
        var count = CountReaderRows($"SELECT * FROM {usersTable}");
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CteSimple(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowRowNumber(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowLag(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.WindowLead(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectExistsPredicate(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectNotExistsPredicate(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectCorrelatedCount(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);

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

        reader.Read().Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 1, "Alice", 3, 0, 1);

        reader.Read().Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 2, "Bob", 1, 0, 0);

        reader.Read().Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 3, "Carla", 0, 1, 0);

        reader.Read().Should().BeFalse();
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.GroupByHaving(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.UnionAllProjection(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.UnionDistinctProjection(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.DistinctProjection(usersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.MultiJoinAggregate(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = ExecuteScalar(Dialect.SelectScalarSubquery(usersTable, ordersTable));
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectInSubquery(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.SelectNotInSubquery(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CrossApplyProjection(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var ordersTable = ResolveScenarioTableName(orders);
        var value = Convert.ToInt32(ExecuteScalar(Dialect.OuterApplyProjection(usersTable, ordersTable)), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var value = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(*) FROM {usersTable} WHERE Id BETWEEN 5 AND 10"), CultureInfo.InvariantCulture);
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
        var usersTable = ResolveScenarioTableName(users);
        var sql = $"SELECT COUNT(*) FROM {usersTable} WHERE Name LIKE 'A%' OR Name LIKE 'B%'";
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
