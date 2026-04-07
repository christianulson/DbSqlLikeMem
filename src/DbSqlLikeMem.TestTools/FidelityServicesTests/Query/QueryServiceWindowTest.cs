namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes shared window-function query workflows against the active provider.
/// PT: Executa fluxos compartilhados de consultas com funcoes de janela no provedor ativo.
/// </summary>
public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Executes a ranking window query with duplicate names and validates rank, dense-rank, and row-number behavior.
    /// PT: Executa uma consulta de janela com ranking e nomes duplicados e valida o comportamento de rank, dense-rank e row-number.
    /// </summary>
    public int RunWindowRankDenseRank(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    RANK() OVER (ORDER BY Name) AS RankValue,
    DENSE_RANK() OVER (ORDER BY Name) AS DenseRankValue,
    ROW_NUMBER() OVER (ORDER BY Name, Id) AS RowNumberValue
FROM {usersTable}
ORDER BY Name, Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Alice", 1, 1, 1);

        reader.Read().Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 2);

        reader.Read().Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 3);

        reader.Read().Should().BeTrue();
        ValidateWindowRankDenseRow(reader, "Charlie", 4, 3, 4);

        reader.Read().Should().BeFalse();
        GC.KeepAlive(usersTable);
        return 4;
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
    /// PT: Executa uma consulta de janela com FIRST_VALUE e LAST_VALUE e valida as linhas projetadas.
    /// </summary>
    public int RunWindowFirstLastValue(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    FIRST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS FirstName,
    LAST_VALUE(Name) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastName
FROM {usersTable}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Alice", "Alice", "Charlie");

        reader.Read().Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Bravo", "Alice", "Charlie");

        reader.Read().Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Bravo", "Alice", "Charlie");

        reader.Read().Should().BeTrue();
        ValidateWindowFirstLastRow(reader, "Charlie", "Alice", "Charlie");

        reader.Read().Should().BeFalse();
        GC.KeepAlive(usersTable);
        return 4;
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
    /// PT: Executa uma consulta de janela NTILE e valida os valores de bucket projetados.
    /// </summary>
    public int RunWindowNtile(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTILE(2) OVER (ORDER BY Name, Id) AS BucketValue
FROM {usersTable}
ORDER BY Name, Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateWindowNtileRow(reader, "Alice", 1);

        reader.Read().Should().BeTrue();
        ValidateWindowNtileRow(reader, "Bravo", 1);

        reader.Read().Should().BeTrue();
        ValidateWindowNtileRow(reader, "Bravo", 2);

        reader.Read().Should().BeTrue();
        ValidateWindowNtileRow(reader, "Charlie", 2);

        reader.Read().Should().BeFalse();
        GC.KeepAlive(usersTable);
        return 4;
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
    /// PT: Executa uma consulta de janela com PERCENT_RANK e CUME_DIST e valida as linhas projetadas.
    /// </summary>
    public int RunWindowPercentRankCumeDist(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);
        var percentRankExpr = Dialect.Provider == ProviderId.Firebird
            ? "ROUND(CASE WHEN COUNT(*) OVER () <= 1 THEN 0 ELSE 1.0 * (RANK() OVER (ORDER BY Name, Id) - 1) / (COUNT(*) OVER () - 1) END, 6)"
            : Dialect.Provider == ProviderId.Npgsql
            ? "ROUND((PERCENT_RANK() OVER (ORDER BY Name, Id))::numeric, 6)"
            : "ROUND(PERCENT_RANK() OVER (ORDER BY Name, Id), 6)";
        var cumeDistExpr = Dialect.Provider == ProviderId.Firebird
            ? "ROUND(CASE WHEN COUNT(*) OVER () = 0 THEN 0 ELSE 1.0 * COUNT(*) OVER (ORDER BY Name, Id) / COUNT(*) OVER () END, 6)"
            : Dialect.Provider == ProviderId.Npgsql
            ? "ROUND((CUME_DIST() OVER (ORDER BY Name, Id))::numeric, 6)"
            : "ROUND(CUME_DIST() OVER (ORDER BY Name, Id), 6)";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    {percentRankExpr} AS PercentRankValue,
    {cumeDistExpr} AS CumeDistValue
FROM {usersTable}
ORDER BY Name, Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(reader, "Alice", 0.0m, 0.25m);

        reader.Read().Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(reader, "Bravo", 0.333333m, 0.5m);

        reader.Read().Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(reader, "Bravo", 0.666667m, 0.75m);

        reader.Read().Should().BeTrue();
        ValidateWindowPercentRankCumeDistRow(reader, "Charlie", 1.0m, 1.0m);

        reader.Read().Should().BeFalse();
        GC.KeepAlive(usersTable);
        return 4;
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
    /// PT: Executa uma consulta de janela NTH_VALUE e valida as linhas projetadas.
    /// </summary>
    public int RunWindowNthValue(params object[] pars)
    {
        if (Dialect.Provider is ProviderId.SqlServer or ProviderId.SqlAzure)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the NTH_VALUE window benchmark.");
        }

        var users = (string)pars[0];
        var usersTable = ResolveScenarioTableName(users);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTH_VALUE(Name, 2) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SecondName
FROM {usersTable}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        reader.Read().Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Alice", "Bravo");

        reader.Read().Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");

        reader.Read().Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");

        reader.Read().Should().BeTrue();
        ValidateWindowNthValueRow(reader, "Charlie", "Bravo");

        reader.Read().Should().BeFalse();
        GC.KeepAlive(usersTable);
        return 4;
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
