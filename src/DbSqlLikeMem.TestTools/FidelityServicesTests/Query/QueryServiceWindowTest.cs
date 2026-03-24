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
        var usersTable = $"{users}";

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

        Assert.True(reader.Read());
        ValidateWindowRankDenseRow(reader, "Aaron", 1, 1, 1);

        Assert.True(reader.Read());
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 2);

        Assert.True(reader.Read());
        ValidateWindowRankDenseRow(reader, "Bravo", 2, 2, 3);

        Assert.True(reader.Read());
        ValidateWindowRankDenseRow(reader, "Charlie", 4, 3, 4);

        Assert.False(reader.Read());
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
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedRankValue, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedDenseRankValue, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedRowNumberValue, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Executes a FIRST_VALUE and LAST_VALUE window query and validates the projected rows.
    /// PT: Executa uma consulta de janela com FIRST_VALUE e LAST_VALUE e valida as linhas projetadas.
    /// </summary>
    public int RunWindowFirstLastValue(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = $"{users}";

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

        Assert.True(reader.Read());
        ValidateWindowFirstLastRow(reader, "Aaron", "Aaron", "Charlie");

        Assert.True(reader.Read());
        ValidateWindowFirstLastRow(reader, "Bravo", "Aaron", "Charlie");

        Assert.True(reader.Read());
        ValidateWindowFirstLastRow(reader, "Bravo", "Aaron", "Charlie");

        Assert.True(reader.Read());
        ValidateWindowFirstLastRow(reader, "Charlie", "Aaron", "Charlie");

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        return 4;
    }

    private static void ValidateWindowFirstLastRow(
        DbDataReader reader,
        string expectedName,
        string expectedFirstName,
        string expectedLastName)
    {
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFirstName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedLastName, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Executes an NTILE window query and validates the projected bucket values.
    /// PT: Executa uma consulta de janela NTILE e valida os valores de bucket projetados.
    /// </summary>
    public int RunWindowNtile(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = $"{users}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTILE(2) OVER (ORDER BY Name, Id) AS BucketValue
FROM {usersTable}
ORDER BY Name, Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateWindowNtileRow(reader, "Aaron", 1);

        Assert.True(reader.Read());
        ValidateWindowNtileRow(reader, "Bravo", 1);

        Assert.True(reader.Read());
        ValidateWindowNtileRow(reader, "Bravo", 2);

        Assert.True(reader.Read());
        ValidateWindowNtileRow(reader, "Charlie", 2);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        return 4;
    }

    private static void ValidateWindowNtileRow(
        DbDataReader reader,
        string expectedName,
        int expectedBucketValue)
    {
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBucketValue, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Executes a PERCENT_RANK and CUME_DIST window query and validates the projected rows.
    /// PT: Executa uma consulta de janela com PERCENT_RANK e CUME_DIST e valida as linhas projetadas.
    /// </summary>
    public int RunWindowPercentRankCumeDist(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = $"{users}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    ROUND(PERCENT_RANK() OVER (ORDER BY Name, Id), 6) AS PercentRankValue,
    ROUND(CUME_DIST() OVER (ORDER BY Name, Id), 6) AS CumeDistValue
FROM {usersTable}
ORDER BY Name, Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateWindowPercentRankCumeDistRow(reader, "Aaron", 0.0m, 0.25m);

        Assert.True(reader.Read());
        ValidateWindowPercentRankCumeDistRow(reader, "Bravo", 0.333333m, 0.75m);

        Assert.True(reader.Read());
        ValidateWindowPercentRankCumeDistRow(reader, "Bravo", 0.333333m, 0.75m);

        Assert.True(reader.Read());
        ValidateWindowPercentRankCumeDistRow(reader, "Charlie", 1.0m, 1.0m);

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        return 4;
    }

    private static void ValidateWindowPercentRankCumeDistRow(
        DbDataReader reader,
        string expectedName,
        decimal expectedPercentRankValue,
        decimal expectedCumeDistValue)
    {
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedPercentRankValue, Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedCumeDistValue, Convert.ToDecimal(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    /// <summary>
    /// EN: Executes an NTH_VALUE window query and validates the projected rows.
    /// PT: Executa uma consulta de janela NTH_VALUE e valida as linhas projetadas.
    /// </summary>
    public int RunWindowNthValue(params object[] pars)
    {
        var users = (string)pars[0];
        var usersTable = $"{users}";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    NTH_VALUE(Name, 2) OVER (ORDER BY Id ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS SecondName
FROM {usersTable}
ORDER BY Id
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateWindowNthValueRow(reader, "Aaron", "Bravo");

        Assert.True(reader.Read());
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");

        Assert.True(reader.Read());
        ValidateWindowNthValueRow(reader, "Bravo", "Bravo");

        Assert.True(reader.Read());
        ValidateWindowNthValueRow(reader, "Charlie", "Bravo");

        Assert.False(reader.Read());
        GC.KeepAlive(usersTable);
        return 4;
    }

    private static void ValidateWindowNthValueRow(
        DbDataReader reader,
        string expectedName,
        string expectedSecondName)
    {
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedSecondName, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
    }
}
