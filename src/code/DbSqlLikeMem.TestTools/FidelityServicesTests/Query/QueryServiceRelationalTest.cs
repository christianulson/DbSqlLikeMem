namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest
{
    private string BuildFirstNoteSubquery(string orderByDirection) => Repo.Dialect.Provider switch
    {
        ProviderId.SqlServer
        or ProviderId.SqlAzure => $"""
    SELECT TOP 1 o2.Note
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
    ORDER BY o2.Note {orderByDirection}
""",
        ProviderId.Oracle => orderByDirection.Equals("ASC", StringComparison.OrdinalIgnoreCase)
                ? $"""
    SELECT MIN(o2.Note)
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
"""
                : $"""
    SELECT MAX(o2.Note)
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
""",
        ProviderId.Firebird => orderByDirection.Equals("ASC", StringComparison.OrdinalIgnoreCase)
                ? $"""
    SELECT MIN(o2.Note)
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
"""
                : $"""
    SELECT MAX(o2.Note)
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
""",
        _ => $"""
    SELECT o2.Note
    FROM {Context.TbOrdersFullName} o2
    WHERE o2.{Context.TbUsers}Id = u.Id
    ORDER BY o2.Note {orderByDirection}
    LIMIT 1
"""
    };

    private string BuildOrderCountExpression() => Repo.Dialect.Provider switch
    {
        ProviderId.SqlServer
        or ProviderId.SqlAzure => $"COUNT(o.{Context.TbUsers}Id)",
        ProviderId.Oracle => $"""
(SELECT COUNT(*)
 FROM {Context.TbOrdersFullName} o2
 WHERE o2.{Context.TbUsers}Id = u.Id)
""",
        ProviderId.Db2 => $"COUNT(o.{Context.TbUsers}Id)",
        _ => "COUNT(o.Id)"
    };

    private string TemporalCurrentTimestampForProvider() => Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure
        ? "SYSUTCDATETIME()"
        : Repo.Dialect.TemporalCurrentTimestampExpression();

    private string TemporalDateAddForProvider() => Repo.Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure
        ? "DATEADD(day, 1, SYSUTCDATETIME())"
        : Repo.Dialect.TemporalDateAddExpression();

    /// <summary>
    /// EN: Executes a large grouped join query over users and orders and validates the projected rows.
    /// PT: Executa uma consulta grande com junção agrupada entre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public async Task<object?> RunJoinTypedExpressionMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(2);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    UPPER(u.Name) AS UserNameUpper,
    LOWER(u.Name) AS UserNameLower,
    {orderCountExpr} AS OrderCount,
    SUM(o.Quantity) AS TotalQuantity,
    ROUND(SUM(o.Amount), 2) AS TotalAmount,
    ROUND(AVG(o.Amount), 2) AS AvgAmount,
    COALESCE(({BuildFirstNoteSubquery("ASC")}), 'none') AS FirstNote,
    ({BuildFirstNoteSubquery("DESC")}) AS LastNote,
    CASE WHEN {orderCountExpr} > 1 THEN 1 ELSE 0 END AS HasMultipleOrders,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS AmountAtLeastThree
FROM {Context.TbUsersFullName} u
INNER JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTypedRow(reader, 1, "ALICE", "alice", 2, 3, 4.00m, 2.00m, "A", "B", 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTypedRow(reader, 2, "BOB", "bob", 1, 4, 5.50m, 5.50m, "C", "C", 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserNameUpper", "UserNameLower", "OrderCount", "TotalQuantity", "TotalAmount", "AvgAmount", "FirstNote", "LastNote", "HasMultipleOrders", "AmountAtLeastThree"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a left join aggregate query that keeps users without orders and validates null-handling behavior.
    /// PT: Executa uma consulta agregada com left join que preserva usuarios sem pedidos e valida o tratamento de null.
    /// </summary>
    public async Task<object?> RunJoinNullAggregateMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    {orderCountExpr} AS OrderCount,
    COALESCE(SUM(o.Quantity), 0) AS TotalQuantity,
    COALESCE(ROUND(SUM(o.Amount), 2), 0) AS TotalAmount,
    COALESCE(({BuildFirstNoteSubquery("ASC")}), 'none') AS FirstNote,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN SUM(o.Amount) IS NULL THEN 1 ELSE 0 END AS AmountIsNull,
    CASE WHEN MAX(o.Quantity) > 1 THEN 1 ELSE 0 END AS HasLargeQuantity
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 1, "Alice", 2, 3, 4.00m, "A", 0, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 2, "Bob", 1, 4, 5.50m, "C", 0, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinNullAggregateRow(reader, 3, "Carla", 0, 0, 0.00m, "none", 1, 1, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "OrderCount", "TotalQuantity", "TotalAmount", "FirstNote", "HasNoOrders", "AmountIsNull", "HasLargeQuantity"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped left join query that combines casts, null handling, and aggregate formatting.
    /// PT: Executa uma consulta agrupada com left join que combina casts, tratamento de null e formatacao de agregados.
    /// </summary>
    public async Task<object?> RunJoinCastNullMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Repo.Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Repo.Dialect.DecimalTextExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 2)}) AS TotalAmountText,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS NotesAreNull,
    CASE WHEN {orderCountExpr} > 0 THEN 1 ELSE 0 END AS HasNote,
    CASE WHEN SUM(o.Amount) >= 3 THEN 1 ELSE 0 END AS MeetsAmountThreshold
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastNullRow(reader, 1, "Alice", "2", "3", "4.00", 0, 0, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastNullRow(reader, 2, "Bob", "1", "4", "5.50", 0, 0, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastNullRow(reader, 3, "Carla", "0", "0", "0.00", 1, 1, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "HasNoOrders", "NotesAreNull", "HasNote", "MeetsAmountThreshold"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped left join query that casts numeric aggregates to text and combines them with comparisons.
    /// PT: Executa uma consulta agrupada com left join que converte agregados numericos para texto e os combina com comparacoes.
    /// </summary>
    public async Task<object?> RunJoinCastTextComparisonMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Repo.Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Repo.Dialect.DecimalTextExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 2)}) AS TotalAmountText,
    CASE WHEN TRIM({Repo.Dialect.StringCastExpression(orderCountExpr, 10)}) = '0' THEN 1 ELSE 0 END AS CountTextIsZero,
    CASE WHEN TRIM({Repo.Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) <> '0' THEN 1 ELSE 0 END AS QuantityTextNonZero,
    CASE WHEN COALESCE(MIN(o.Note), 'none') = 'none' THEN 1 ELSE 0 END AS NotesAreMissing,
    CASE WHEN MAX(o.Note) IS NOT NULL THEN 1 ELSE 0 END AS HasAnyNote
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastTextRow(reader, 1, "Alice", "2", "3", "4.00", 0, 1, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastTextRow(reader, 2, "Bob", "1", "4", "5.50", 0, 1, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinCastTextRow(reader, 3, "Carla", "0", "0", "0.00", 1, 0, 1, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "CountTextIsZero", "QuantityTextNonZero", "NotesAreMissing", "HasAnyNote"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped join query with HAVING filters and validates casted aggregate outputs.
    /// PT: Executa uma consulta agrupada com filtros HAVING e valida saidas agregadas convertidas.
    /// </summary>
    public async Task<object?> RunJoinHavingCastMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(1);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    TRIM({Repo.Dialect.StringCastExpression(orderCountExpr, 10)}) AS OrderCountText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Repo.Dialect.DecimalTextExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 2)}) AS TotalAmountText,
    CASE WHEN {orderCountExpr} >= 2 THEN 1 ELSE 0 END AS HasTwoOrMoreOrders,
    CASE WHEN SUM(o.Amount) >= 4 THEN 1 ELSE 0 END AS AmountAtLeastFour,
    CASE WHEN MIN(o.Note) = 'A' THEN 1 ELSE 0 END AS StartsAtA
FROM {Context.TbUsersFullName} u
INNER JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
HAVING {orderCountExpr} >= 2
   AND SUM(o.Amount) >= 4
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinHavingCastRow(reader, 1, "Alice", "2", "3", "4.00", 1, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "HasTwoOrMoreOrders", "AmountAtLeastFour", "StartsAtA"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped join query that mixes string-length expressions with numeric conversions and aggregates.
    /// PT: Executa uma consulta agrupada que mistura expressoes de comprimento de texto com conversoes numericas e agregados.
    /// </summary>
    public async Task<object?> RunJoinLengthNumericMatrixAsync(params object[] pars)
    {
        var nameLenExpr = Repo.Dialect.Provider is ProviderId.Db2 or ProviderId.Oracle
            ? "MAX(LENGTH(u.Name))"
            : $"MAX({Repo.Dialect.StringLengthExpression("u.Name")})";
        var noteLenSource = Repo.Dialect.Provider == ProviderId.Db2 ? "Note" : "o.Note";
        var noteLenExpr = Repo.Dialect.StringLengthExpression(noteLenSource);
        var nameLenTextExpr = $"TRIM({Repo.Dialect.StringCastExpression(nameLenExpr, 10)})";
        var noteLenTextExpr = $"TRIM({Repo.Dialect.StringCastExpression($"COALESCE(MAX({noteLenExpr}), 0)", 10)})";
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    {nameLenTextExpr} AS NameLenText,
    TRIM({Repo.Dialect.StringCastExpression("COALESCE(SUM(o.Quantity), 0)", 10)}) AS TotalQuantityText,
    TRIM({Repo.Dialect.DecimalTextExpression("COALESCE(ROUND(SUM(o.Amount), 2), 0)", 2)}) AS TotalAmountText,
    {noteLenTextExpr} AS MaxNoteLenText,
    CASE WHEN {nameLenExpr} >= 4 THEN 1 ELSE 0 END AS NameLenGe4,
    CASE WHEN COALESCE(SUM(o.Quantity), 0) >= 3 THEN 1 ELSE 0 END AS QuantityGe3,
    CASE WHEN COALESCE(ROUND(SUM(o.Amount), 2), 0) >= 4 THEN 1 ELSE 0 END AS AmountGe4,
    CASE WHEN COALESCE(MAX({noteLenExpr}), 0) >= 1 THEN 1 ELSE 0 END AS HasNotes
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 1, "Alice", "5", "3", "4.00", "1", 1, 1, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 2, "Bob", "3", "4", "5.50", "1", 0, 1, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinLengthNumericRow(reader, 3, "Carla", "5", "0", "0.00", "0", 1, 0, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "NameLenText", "TotalQuantityText", "TotalAmountText", "MaxNoteLenText", "NameLenGe4", "QuantityGe3", "AmountGe4", "HasNotes"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped join query that blends string case, string length, and aggregate comparisons.
    /// PT: Executa uma consulta agrupada que mistura caixa de texto, comprimento de texto e comparacoes agregadas.
    /// </summary>
    public async Task<object?> RunJoinTextCaseLengthMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();

        var nameLenExpr = Repo.Dialect.Provider is ProviderId.Db2 or ProviderId.Oracle
            ? "MAX(LENGTH(u.Name))"
            : $"MAX({Repo.Dialect.StringLengthExpression("u.Name")})";
        var noteLenSource = Repo.Dialect.Provider == ProviderId.Db2 ? "Note" : "o.Note";
        var noteLenExpr = Repo.Dialect.StringLengthExpression(noteLenSource);
        var nameLenTextExpr = $"TRIM({Repo.Dialect.StringCastExpression(nameLenExpr, 10)})";
        var noteLenTextExpr = $"TRIM({Repo.Dialect.StringCastExpression($"COALESCE(MAX({noteLenExpr}), 0)", 10)})";
        var textMatchAlready = Repo.Dialect.Provider is ProviderId.Sqlite or ProviderId.Oracle or ProviderId.Npgsql or ProviderId.Db2 or ProviderId.Firebird ? 0 : 1;
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
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
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.Name
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 1, "ALICE", "alice", "Alice", "5", "1", 1, textMatchAlready, textMatchAlready, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 2, "BOB", "bob", "Bob", "3", "1", 0, textMatchAlready, textMatchAlready, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTextCaseLengthRow(reader, 3, "CARLA", "carla", "Carla", "5", "0", 1, textMatchAlready, textMatchAlready, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "NameUpper", "NameLower", "NameTrimmed", "NameLenText", "MaxNoteLenText", "NameLenGe4", "IsUpperAlready", "IsLowerAlready", "TwoOrMoreOrders", "QuantityGe3"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped left join report that combines distinct counts, CASE expressions, and repeated values.
    /// PT: Executa um relatorio agrupado com left join que combina contagens distintas, expressoes CASE e valores repetidos.
    /// </summary>
    public async Task<object?> RunJoinDistinctCaseMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
    COUNT(DISTINCT o.Note) AS DistinctNoteCount,
    SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) AS NoteACount,
    CASE WHEN COUNT(DISTINCT o.Note) >= 2 THEN 1 ELSE 0 END AS HasMultipleDistinctNotes,
    CASE WHEN SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END AS HasRepeatedNoteA
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 2, 1, 1, 0, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderCount", "DistinctNoteCount", "NoteACount", "HasMultipleDistinctNotes", "HasRepeatedNoteA"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped left join report with HAVING and distinct note counts.
    /// PT: Executa um relatorio agrupado com left join, HAVING e contagens distintas de notas.
    /// </summary>
    public async Task<object?> RunJoinDistinctHavingMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var rows = new List<QueryResultRowSnapshot>(2);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
    COUNT(DISTINCT o.Note) AS DistinctNoteCount,
    SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) AS NoteACount,
    CASE WHEN COUNT(DISTINCT o.Note) >= 2 THEN 1 ELSE 0 END AS HasMultipleDistinctNotes,
    CASE WHEN SUM(CASE WHEN o.Note = 'A' THEN 1 ELSE 0 END) >= 2 THEN 1 ELSE 0 END AS HasRepeatedNoteA
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id
    HAVING COUNT(DISTINCT o.Note) >= 2 OR {orderCountExpr} = 0
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 1, 3, 2, 2, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinDistinctCaseRow(reader, 3, 0, 0, 0, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderCount", "DistinctNoteCount", "NoteACount", "HasMultipleDistinctNotes", "HasRepeatedNoteA"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped left join query that blends temporal comparisons with aggregate counts.
    /// PT: Executa uma consulta agrupada com left join que mistura comparacoes temporais com contagens agregadas.
    /// </summary>
    public async Task<object?> RunJoinTemporalMatrixAsync(params object[] pars)
    {
        var orderCountExpr = BuildOrderCountExpression();
        var expectedNextDayAfterOrder = 1;
        var rows = new List<QueryResultRowSnapshot>(3);

        var nowExpr = TemporalCurrentTimestampForProvider();
        var nextDayExpr = TemporalDateAddForProvider();

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    {orderCountExpr} AS OrderCount,
    CASE WHEN {orderCountExpr} = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN COALESCE(MIN(o.OrderedAt), u.CreatedAt) <= {nowExpr} THEN 1 ELSE 0 END AS MinOrderedBeforeNow,
    CASE WHEN COALESCE(MAX(o.OrderedAt), u.CreatedAt) < {nextDayExpr} THEN 1 ELSE 0 END AS MaxOrderedBeforeNextDay,
    SUM(CASE WHEN o.DeliveredAt IS NULL THEN 1 ELSE 0 END) AS PendingDeliveries,
    CASE WHEN u.CreatedAt <= {nowExpr} THEN 1 ELSE 0 END AS UserCreatedBeforeNow
FROM {Context.TbUsersFullName} u
LEFT JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
GROUP BY u.Id, u.CreatedAt
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTemporalRow(reader, 1, 2, 0, 1, expectedNextDayAfterOrder, 2, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTemporalRow(reader, 2, 1, 0, 1, expectedNextDayAfterOrder, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinTemporalRow(reader, 3, 0, 1, 1, expectedNextDayAfterOrder, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderCount", "HasNoOrders", "MinOrderedBeforeNow", "MaxOrderedBeforeNextDay", "PendingDeliveries", "UserCreatedBeforeNow"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a joined window-function query over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public async Task<object?> RunJoinWindowMatrixAsync(params object[] pars)
    {
        var orderCountExpr = "COUNT(*)";
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    o.Id AS OrderId,
    ROW_NUMBER() OVER (PARTITION BY u.Id ORDER BY o.Id) AS RowNumberInUser,
    {orderCountExpr} OVER (PARTITION BY u.Id) AS OrdersPerUser,
    LAG(o.Note) OVER (PARTITION BY u.Id ORDER BY o.Id) AS PreviousNote
FROM {Context.TbUsersFullName} u
JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowRow(reader, 1, 10, 1, 2, null);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowRow(reader, 1, 11, 2, 2, "A");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowRow(reader, 2, 12, 1, 1, null);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a joined window-function query with temporal comparisons over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela e comparacoes temporais em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public async Task<object?> RunJoinWindowTemporalMatrixAsync(params object[] pars)
    {
        var orderCountExpr = "COUNT(*)";
        var expectedNextDayAfterOrder = 1;
        var rows = new List<QueryResultRowSnapshot>(3);

        var nowExpr = TemporalCurrentTimestampForProvider();
        var nextDayExpr = TemporalDateAddForProvider();

        using var command = Repo.Cnn.CreateCommand();
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
FROM {Context.TbUsersFullName} u
JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 1, 10, 1, 2, null, 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 1, 11, 2, 2, "A", 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowTemporalRow(reader, 2, 12, 1, 1, null, 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote", "OrderedBeforeNow", "NextDayAfterOrder", "UserCreatedBeforeNow"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a joined window-function query with temporal and aggregate comparisons over users and orders and validates the projected rows.
    /// PT: Executa uma consulta com funcoes de janela, comparacoes temporais e agregadas em join sobre usuarios e pedidos e valida as linhas projetadas.
    /// </summary>
    public async Task<object?> RunJoinWindowAggregateTemporalMatrixAsync(params object[] pars)
    {
        var orderCountExpr = "COUNT(*)";
        var expectedNextDayAfterOrder = 1;
        var rows = new List<QueryResultRowSnapshot>(3);

        var nowExpr = TemporalCurrentTimestampForProvider();
        var nextDayExpr = TemporalDateAddForProvider();

        using var command = Repo.Cnn.CreateCommand();
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
FROM {Context.TbUsersFullName} u
JOIN {Context.TbOrdersFullName} o ON o.{Context.TbUsers}Id = u.Id
ORDER BY u.Id, o.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 10, 1, 2, 2, 0.00m, null, 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 1, 11, 2, 2, 2, 0.00m, "A", 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateJoinWindowAggregateTemporalRow(reader, 2, 12, 1, 1, 1, 0.00m, null, 1, expectedNextDayAfterOrder, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "QuantityPerUser", "AmountPerUser", "PreviousNote", "OrderedBeforeNow", "NextDayAfterOrder", "UserCreatedBeforeNow"],
            Rows = rows,
        };
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
        var integerPart = string.IsNullOrEmpty(parts[0]) ? "0" : parts[0];

        if (parts.Length == 1)
        {
            return $"{sign}{integerPart}.00";
        }

        if (parts.Length == 2)
        {
            var fractional = parts[1];
            if (fractional.Length == 0)
                return $"{sign}{integerPart}.00";

            if (fractional.Length == 1)
                return $"{sign}{integerPart}.{fractional}0";

            if (fractional.Length == 2)
                return $"{sign}{integerPart}.{fractional}";
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
    public async Task<object?> RunRowCountAfterSelectAsync(params object[] pars)
    {
        var count = await CountReaderRowsAsync($"SELECT * FROM {Context.TbUsersFullName}");
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected select rowcount for {Repo.Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Executes a select over all configured users and returns the ordered Name snapshot.
    /// PT: Executa um select sobre todos os usuarios configurados e retorna o snapshot ordenado de Name.
    /// </summary>
    public async Task<object?> RunAllRowsSnapshotAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"SELECT Name FROM {Context.TbUsersFullName} ORDER BY Id");
    }

    /// <summary>
    /// EN: Executes a simple CTE query against the configured users table.
    /// PT: Executa uma consulta CTE simples na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunCteSimpleAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.CteSimple(Context));
    }

    /// <summary>
    /// EN: Executes a CTE query with a MATERIALIZED hint against the configured users table.
    /// PT: Executa uma consulta CTE com hint MATERIALIZED na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunCteMaterializedHintAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
WITH x AS MATERIALIZED (
    SELECT Id, Name
    FROM {Context.TbUsersFullName}
    ORDER BY Id
)
SELECT Name
FROM x
""");
    }

    /// <summary>
    /// EN: Executes a ROW_NUMBER window query against the configured users table and returns the full rowset snapshot.
    /// PT: Executa uma consulta de janela ROW_NUMBER na tabela de usuarios configurada e retorna o snapshot completo do conjunto de linhas.
    /// </summary>
    public async Task<object?> RunWindowRowNumberAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.WindowRowNumber(Context));
    }

    /// <summary>
    /// EN: Executes a LAG window query against the configured users table and returns the full rowset snapshot.
    /// PT: Executa uma consulta de janela LAG na tabela de usuarios configurada e retorna o snapshot completo do conjunto de linhas.
    /// </summary>
    public async Task<object?> RunWindowLagAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.WindowLag(Context));
    }

    /// <summary>
    /// EN: Executes a LEAD window query against the configured users table and returns the full rowset snapshot.
    /// PT: Executa uma consulta de janela LEAD na tabela de usuarios configurada e retorna o snapshot completo do conjunto de linhas.
    /// </summary>
    public async Task<object?> RunWindowLeadAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.WindowLead(Context));
    }

    /// <summary>
    /// EN: Executes an EXISTS predicate query against the configured users and orders tables.
    /// PT: Executa uma consulta com predicado EXISTS nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectExistsPredicateAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectExistsPredicate(Context));
    }

    /// <summary>
    /// EN: Executes a NOT EXISTS predicate query against the configured users and orders tables.
    /// PT: Executa uma consulta com predicado NOT EXISTS nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectNotExistsPredicateAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectNotExistsPredicate(Context));
    }

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join query against the configured users and orders tables.
    /// PT: Executa uma consulta anti-join com LEFT JOIN nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectLeftJoinAntiJoinAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectLeftJoinAntiJoin(Context));
    }

    /// <summary>
    /// EN: Executes a correlated count query against the configured users and orders tables.
    /// PT: Executa uma consulta de contagem correlacionada nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectCorrelatedCountAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectCorrelatedCount(Context));
    }

    /// <summary>
    /// EN: Executes a per-row scalar subquery report with CASE expressions against the configured users and orders tables.
    /// PT: Executa um relatorio com subconsulta escalar por linha e expressoes CASE nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectScalarCaseMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    u.Id AS UserId,
    u.Name AS UserName,
    (SELECT COUNT(*) FROM {Context.TbOrdersFullName} o WHERE o.{Context.TbUsers}Id = u.Id) AS OrderCount,
    CASE WHEN (SELECT COUNT(*) FROM {Context.TbOrdersFullName} o WHERE o.{Context.TbUsers}Id = u.Id) = 0 THEN 1 ELSE 0 END AS HasNoOrders,
    CASE WHEN (SELECT COUNT(*) FROM {Context.TbOrdersFullName} o WHERE o.{Context.TbUsers}Id = u.Id) >= 2 THEN 1 ELSE 0 END AS HasManyOrders
FROM {Context.TbUsersFullName} u
ORDER BY u.Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 1, "Alice", 3, 0, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 2, "Bob", 1, 0, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateSelectScalarCaseRow(reader, 3, "Carla", 0, 1, 0);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = ["UserId", "UserName", "OrderCount", "HasNoOrders", "HasManyOrders"],
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes the scalar subquery case matrix against the configured users and orders tables.
    /// PT: Executa a matriz de subconsulta escalar com CASE nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public Task<object?> RunSelectScalarSubqueryCaseMatrixAsync(params object[] pars)
        => RunSelectScalarCaseMatrixAsync(pars);

    /// <summary>
    /// EN: Executes a GROUP BY HAVING query against the configured users and orders tables and returns the matching rowset.
    /// PT: Executa uma consulta GROUP BY HAVING nas tabelas de usuarios e pedidos configuradas e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunGroupByHavingAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.GroupByHaving(Context));
    }

    /// <summary>
    /// EN: Executes a UNION ALL projection query against the configured users table.
    /// PT: Executa uma consulta de projeção UNION ALL na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunUnionAllProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.UnionAllProjection(Context));
    }

    /// <summary>
    /// EN: Executes a UNION projection query against the configured users table.
    /// PT: Executa uma consulta de projeção UNION na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunUnionDistinctProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.UnionDistinctProjection(Context));
    }

    /// <summary>
    /// EN: Executes a DISTINCT projection query against the configured users table.
    /// PT: Executa uma consulta de projeção DISTINCT na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunDistinctProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.DistinctProjection(Context));
    }

    /// <summary>
    /// EN: Executes a DISTINCT ON projection query against the configured users and orders tables.
    /// PT: Executa uma consulta de projeção DISTINCT ON nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunDistinctOnProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.DistinctOnProjection(Context));
    }

    /// <summary>
    /// EN: Executes a multi-join aggregate query against the configured users and orders tables.
    /// PT: Executa uma consulta agregada com multiplos joins nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunMultiJoinAggregateAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.MultiJoinAggregate(Context));
    }

    /// <summary>
    /// EN: Executes a scalar subquery projection against the configured users and orders tables.
    /// PT: Executa uma projeção com subconsulta escalar nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectScalarSubqueryAsync(params object[] pars)
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.SelectScalarSubquery(Context));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes an IN subquery predicate against the configured users and orders tables.
    /// PT: Executa um predicado IN com subconsulta nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectInSubqueryAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectInSubquery(Context));
    }

    /// <summary>
    /// EN: Executes a NOT IN subquery predicate against the configured users and orders tables.
    /// PT: Executa um predicado NOT IN com subconsulta nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunSelectNotInSubqueryAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.SelectNotInSubquery(Context));
    }

    /// <summary>
    /// EN: Executes a CROSS APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo CROSS APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunCrossApplyProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.CrossApplyProjection(Context));
    }

    /// <summary>
    /// EN: Executes an OUTER APPLY style projection against the configured users and orders tables.
    /// PT: Executa uma projeção no estilo OUTER APPLY nas tabelas de usuarios e pedidos configuradas.
    /// </summary>
    public async Task<object?> RunOuterApplyProjectionAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync(Repo.Dialect.OuterApplyProjection(Context));
    }

    /// <summary>
    /// EN: Executes a STRING_SPLIT projection against the configured users table and returns the full rowset snapshot.
    /// PT: Executa uma projeção STRING_SPLIT na tabela de usuarios configurada e retorna o snapshot completo do conjunto de linhas.
    /// </summary>
    public async Task<object?> RunStringSplitProjectionAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsApplyClause || !Repo.Dialect.SupportsStringSplitFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the STRING_SPLIT benchmark.");
        }

        return await CaptureSnapshotAsync($"""
SELECT part.value
FROM {Context.TbUsersFullName} u
CROSS APPLY STRING_SPLIT(u.Email, ',') part
WHERE u.Id = 3
""");
    }

    /// <summary>
    /// EN: Executes a FOR JSON PATH projection against the configured users table and returns the serialized payload.
    /// PT: Executa uma projeção FOR JSON PATH na tabela de usuarios configurada e retorna o payload serializado.
    /// </summary>
    public async Task<object?> RunForJsonPathProjectionAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsForJsonClause)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the FOR JSON benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync($"""
SELECT
    u.Id AS [User.Id],
    u.Name AS [User.Name]
FROM {Context.TbUsersFullName} u
WHERE u.Id IN (1, 2)
ORDER BY u.Id
FOR JSON PATH, ROOT('users')
""");

        GC.KeepAlive(value);
        return Convert.ToString(value, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// EN: Executes a partition-pruning style select against the configured users table.
    /// PT: Executa um select no estilo partition pruning na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunPartitionPruningSelectAsync(params object[] pars)
    {
        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE Id BETWEEN 5 AND 10"), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a pivot-style count query against the configured users table.
    /// PT: Executa uma consulta de contagem no estilo pivot na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunPivotCountAsync(params object[] pars)
    {
        var sql = $"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE Name LIKE 'A%' OR Name LIKE 'B%'";
        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync(sql), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    private async Task<int> CountReaderRowsAsync(string sql, DbTransaction? transaction = null)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        using var reader = await command.ExecuteReaderAsync();
        var count = 0;
        while (await reader.ReadAsync())
        {
            count++;
        }

        return count;
    }

    private async Task<QueryResultSnapshot> CaptureSnapshotAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;

        using var reader = await command.ExecuteReaderAsync();
        var snapshot = QueryResultSnapshotReader.Capture(reader);
        if (Repo.Dialect.Provider is ProviderId.Oracle or ProviderId.Db2 or ProviderId.Firebird)
        {
            var columnNames = new string[snapshot.ColumnNames.Count];
            for (var i = 0; i < snapshot.ColumnNames.Count; i++)
                columnNames[i] = snapshot.ColumnNames[i].ToUpperInvariant();

            return snapshot with { ColumnNames = columnNames };
        }

        return snapshot;
    }
}
