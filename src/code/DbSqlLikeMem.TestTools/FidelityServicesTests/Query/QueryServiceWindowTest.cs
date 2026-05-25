namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes shared window-function query workflows against the active provider.
/// PT-br: Executa fluxos compartilhados de consultas com funcoes de janela no provedor ativo.
/// </summary>
public partial class QueryServiceTest
{
    /// <summary>
    /// EN: Executes a ranking window query with duplicate names and validates rank, dense-rank, and row-number behavior.
    /// PT-br: Executa uma consulta de janela com ranking e nomes duplicados e valida o comportamento de rank, dense-rank e row-number.
    /// </summary>
    public async Task<QueryResultSnapshot> RunWindowRankDenseRank(params object[] pars)
    {
        var expectedFirstName = pars.Length > 1 ? (string)pars[1] : "Alice";
        var rows = new List<QueryResultRowSnapshot>(4);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    RANK() OVER (ORDER BY Name) AS RankValue,
    DENSE_RANK() OVER (ORDER BY Name) AS DenseRankValue,
    ROW_NUMBER() OVER (ORDER BY Name, Id) AS RowNumberValue
FROM {Context.TbUsersFullName}
ORDER BY Name, Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowRankDenseRow(reader, expectedFirstName, 1, 1, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Charlie", 4, 3, 4);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "RankValue", "DenseRankValue", "RowNumberValue"]),
            Rows = rows,
        };
    }

    private static void ValidateWindowRankDenseRow(
        DbDataReader reader,
        string expectedName,
        int expectedRankValue,
        int expectedDenseRankValue,
        int expectedRowNumberValue)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedRankValue);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDenseRankValue);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedRowNumberValue);
    }

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window query and validates the projected rows.
    /// PT-br: Executa uma consulta de janela com FIRST_VALUE e LAST_VALUE e valida as linhas projetadas.
    /// </summary>
    public async Task<QueryResultSnapshot> RunWindowFirstLastValue(params object[] pars)
    {
        var expectedFirstName = pars.Length > 0 ? (string)pars[0] : "Alice";
        var rows = new List<QueryResultRowSnapshot>(4);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    FIRST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS FirstName,
    LAST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastName
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowFirstLastRow(reader, expectedFirstName, expectedFirstName, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Bravo", expectedFirstName, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Bravo", expectedFirstName, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Charlie", expectedFirstName, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "FirstName", "LastName"]),
            Rows = rows,
        };
    }

    private static void ValidateWindowFirstLastRow(
        DbDataReader reader,
        string expectedName,
        string expectedFirstName,
        string expectedLastName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedFirstName);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedLastName);
    }

    /// <summary>
    /// EN: Executes an NTILE window query and validates the projected bucket values.
    /// PT-br: Executa uma consulta de janela NTILE e valida os valores de bucket projetados.
    /// </summary>
    public async Task<QueryResultSnapshot> RunWindowNtile(params object[] pars)
    {
        var expectedFirstName = pars.Length > 0 ? (string)pars[0] : "Alice";
        var rows = new List<QueryResultRowSnapshot>(4);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTILE(2) OVER (ORDER BY Name, Id) AS BucketValue
FROM {Context.TbUsersFullName}
ORDER BY Name, Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNtileRow(reader, expectedFirstName, 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNtileRow(reader, "Bravo", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNtileRow(reader, "Bravo", 2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNtileRow(reader, "Charlie", 2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "BucketValue"]),
            Rows = rows,
        };
    }

    private static void ValidateWindowNtileRow(
        DbDataReader reader,
        string expectedName,
        int expectedBucketValue)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedBucketValue);
    }

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window query and validates the projected rows.
    /// PT-br: Executa uma consulta de janela com PERCENT_RANK e CUME_DIST e valida as linhas projetadas.
    /// </summary>
    public async Task<QueryResultSnapshot> RunWindowPercentRankCumeDist(params object[] pars)
    {
        var expectedFirstName = pars.Length > 0 ? (string)pars[0] : "Alice";
        var rows = new List<QueryResultRowSnapshot>(4);
        var percentRankExpr = Repo.Dialect.Provider == ProviderId.Firebird
            ? "ROUND(CASE WHEN COUNT(*) OVER () <= 1 THEN 0 ELSE CAST(RANK() OVER (ORDER BY Name, Id) - 1 AS DOUBLE PRECISION) / CAST(COUNT(*) OVER () - 1 AS DOUBLE PRECISION) END, 6)"
            : Repo.Dialect.Provider == ProviderId.Npgsql
            ? "ROUND((PERCENT_RANK() OVER (ORDER BY Name, Id))::numeric, 6)"
            : "ROUND(PERCENT_RANK() OVER (ORDER BY Name, Id), 6)";
        var cumeDistExpr = Repo.Dialect.Provider == ProviderId.Firebird
            ? "ROUND(CASE WHEN COUNT(*) OVER () = 0 THEN 0 ELSE CAST(COUNT(*) OVER (ORDER BY Name, Id) AS DOUBLE PRECISION) / CAST(COUNT(*) OVER () AS DOUBLE PRECISION) END, 6)"
            : Repo.Dialect.Provider == ProviderId.Npgsql
            ? "ROUND((CUME_DIST() OVER (ORDER BY Name, Id))::numeric, 6)"
            : "ROUND(CUME_DIST() OVER (ORDER BY Name, Id), 6)";

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    {percentRankExpr} AS PercentRankValue,
    {cumeDistExpr} AS CumeDistValue
FROM {Context.TbUsersFullName}
ORDER BY Name, Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(
            reader,
            expectedFirstName,
            0.0m,
            0.25m);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(
            reader,
            "Bravo",
            0.333333m,
            0.5m);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(
            reader,
            "Bravo",
            0.666667m,
            0.75m);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(
            reader,
            "Charlie",
            1.0m,
            1.0m);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "PercentRankValue", "CumeDistValue"]),
            Rows = rows,
        };
    }

    private static void ValidateWindowPercentRankCumeDistRow(
        DbDataReader reader,
        string expectedName,
        decimal expectedPercentRankValue,
        decimal expectedCumeDistValue)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedPercentRankValue);
        Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedCumeDistValue);
    }

    /// <summary>
    /// EN: Executes an NTH_VALUE window query and validates the projected rows.
    /// PT-br: Executa uma consulta de janela NTH_VALUE e valida as linhas projetadas.
    /// </summary>
    public async Task<QueryResultSnapshot> RunWindowNthValue(params object[] pars)
    {
        if (!Repo.Dialect.SupportsNthValueWindowFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the NTH_VALUE window benchmark.");
        }

        var expectedFirstName = pars.Length > 0 ? (string)pars[0] : "Alice";
        var rows = new List<QueryResultRowSnapshot>(4);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTH_VALUE(Name, 2) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SecondName
FROM {Context.TbUsersFullName}
ORDER BY Id
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNthValueRow(reader, expectedFirstName, "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Charlie", "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "SecondName"]),
            Rows = rows,
        };
    }

    private static void ValidateWindowNthValueRow(
        DbDataReader reader,
        string expectedName,
        string expectedSecondName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedSecondName);
    }
}
