using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared sequence fidelity tests for create and advance workflows across mock and container runs.
/// PT: Fornece testes de fidelidade de sequence compartilhados para fluxos de criacao e avancar entre mock e container.
/// </summary>
public abstract class SequenceTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a created sequence returns the expected first and second values for the current provider.
    /// PT: Verifica se uma sequence criada retorna os valores esperado primeiro e segundo para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceNextValuesTest()
        => RunSequenceNextValuesTest();

    /// <summary>
    /// EN: Verifies that sequence values can be consumed by inserts and keep the expected row range for the current provider.
    /// PT: Verifica se valores de sequence podem ser consumidos por inserts e mantem a faixa esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceInsertRoundTripTest()
        => RunSequenceInsertRoundTripTest();

    /// <summary>
    /// EN: Verifies that sequence expressions can be used directly inside inserts for the current provider.
    /// PT: Verifica se expressoes de sequence podem ser usadas diretamente dentro de inserts para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceInsertExpressionTest()
        => RunSequenceInsertExpressionTest();

    /// <summary>
    /// EN: Verifies that the current sequence value follows the last consumed value for the current provider.
    /// PT: Verifica se o valor corrente da sequence acompanha o ultimo valor consumido para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceCurrentValueTest()
        => RunSequenceCurrentValueTest();

    /// <summary>
    /// EN: Verifies that a sequence value can be projected inside a SELECT for the current provider.
    /// PT: Verifica se um valor de sequence pode ser projetado dentro de um SELECT para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceSelectProjectionTest()
        => RunSequenceSelectProjectionTest();

    /// <summary>
    /// EN: Verifies that sequence values can participate in CASE and WHERE logic inside a single query for the current provider.
    /// PT: Verifica se valores de sequence podem participar de logica CASE e WHERE dentro de uma unica consulta para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceCaseWhereMatrixTest()
        => RunSequenceCaseWhereMatrixTest();

    /// <summary>
    /// EN: Verifies that sequence values can be combined with temporal expressions inside a single query for the current provider.
    /// PT: Verifica se valores de sequence podem ser combinados com expressoes temporais dentro de uma unica consulta para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceTemporalMatrixTest()
        => RunSequenceTemporalMatrixTest();

    /// <summary>
    /// EN: Verifies that sequence-generated keys can participate in a join aggregate workflow for the current provider.
    /// PT: Verifica se chaves geradas por sequence podem participar de um fluxo agregado com join para o provedor atual.
    /// </summary>
    [Fact]
    public void SequenceJoinAggregateTest()
        => RunSequenceJoinAggregateTest();

    private void RunSequenceNextValuesTest()
    {
        var sequence = $"seq_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceNextValuesScenario(connMock, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceNextValuesScenario(connContainer, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceInsertRoundTripTest()
    {
        var sequence = $"seq_{NewToken()}";
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceInsertRoundTripScenario(connMock, users, uId, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceInsertRoundTripScenario(connContainer, users, uId, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceInsertExpressionTest()
    {
        var sequence = $"seq_{NewToken()}";
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceInsertExpressionScenario(connMock, users, uId, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceInsertExpressionScenario(connContainer, users, uId, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceCurrentValueTest()
    {
        var sequence = $"seq_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceCurrentValueScenario(connMock, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceCurrentValueScenario(connContainer, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceSelectProjectionTest()
    {
        var sequence = $"seq_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceSelectProjectionScenario(connMock, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceSelectProjectionScenario(connContainer, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceCaseWhereMatrixTest()
    {
        var sequence = $"seq_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceCaseWhereMatrixScenario(connMock, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceCaseWhereMatrixScenario(connContainer, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceTemporalMatrixTest()
    {
        var sequence = $"seq_{NewToken()}";

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceTemporalMatrixScenario(connMock, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceTemporalMatrixScenario(connContainer, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private void RunSequenceJoinAggregateTest()
    {
        var sequence = $"seq_{NewToken()}";
        var users = "Users";
        var orders = "Orders";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunSequenceJoinAggregateScenario(connMock, users, orders, uId, sequence);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunSequenceJoinAggregateScenario(connContainer, users, orders, uId, sequence);
            Assert.Equal(resultMock, resultContainer);
        }
    }

    private (long First, long Second) RunSequenceNextValuesScenario<TConnection>(
        TConnection connection,
        string sequence)
        where TConnection : DbConnection
    {
        var testScenario = new SequenceScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(sequence);

        try
        {
            var first = Convert.ToInt64(serviceTest.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            var second = Convert.ToInt64(serviceTest.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);

            Assert.Equal(10L, first);
            Assert.Equal(11L, second);
            return (first, second);
        }
        finally
        {
            serviceTest.DropScenario(sequence);
        }
    }

    private (long MinId, long MaxId, long RowCount) RunSequenceInsertRoundTripScenario<TConnection>(
        TConnection connection,
        string users,
        string uId,
        string sequence)
        where TConnection : DbConnection
    {
        var sequenceScenario = new SequenceScenario<TConnection>(dialect);
        var sequenceService = new DmlMutationServiceTest<TConnection>(connection, sequenceScenario, dialect);
        sequenceService.CreateScenario(sequence);

        var usersScenario = new UsersScenario<TConnection>(dialect);
        var usersService = new DmlMutationServiceTest<TConnection>(connection, usersScenario, dialect);
        usersService.CreateScenario(users, uId);

        try
        {
            var tableName = $"{users}_{uId}";
            var first = Convert.ToInt64(sequenceService.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            ExecuteNonQueryOnConnection(connection, dialect.InsertUser(tableName, (int)first, $"Seq-{first}"));
            var second = Convert.ToInt64(sequenceService.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            ExecuteNonQueryOnConnection(connection, dialect.InsertUser(tableName, (int)second, $"Seq-{second}"));

            var result = ExecuteAggregateReadback(connection, tableName);
            Assert.Equal(10L, result.MinId);
            Assert.Equal(11L, result.MaxId);
            Assert.Equal(2L, result.RowCount);
            return result;
        }
        finally
        {
            usersService.DropScenario(users, uId);
            sequenceService.DropScenario(sequence);
        }
    }

    private (long MinId, long MaxId, long RowCount) RunSequenceInsertExpressionScenario<TConnection>(
        TConnection connection,
        string users,
        string uId,
        string sequence)
        where TConnection : DbConnection
    {
        var sequenceScenario = new SequenceScenario<TConnection>(dialect);
        var sequenceService = new DmlMutationServiceTest<TConnection>(connection, sequenceScenario, dialect);
        sequenceService.CreateScenario(sequence);

        var usersScenario = new UsersScenario<TConnection>(dialect);
        var usersService = new DmlMutationServiceTest<TConnection>(connection, usersScenario, dialect);
        usersService.CreateScenario(users, uId);

        try
        {
            var tableName = $"{users}_{uId}";
            ExecuteNonQueryOnConnection(connection, $"INSERT INTO {tableName} (Id, Name) VALUES ({dialect.NextSequenceValueExpression(sequence)}, 'Seq-A')");
            ExecuteNonQueryOnConnection(connection, $"INSERT INTO {tableName} (Id, Name) VALUES ({dialect.NextSequenceValueExpression(sequence)}, 'Seq-B')");

            var result = ExecuteAggregateReadback(connection, tableName);
            Assert.Equal(10L, result.MinId);
            Assert.Equal(11L, result.MaxId);
            Assert.Equal(2L, result.RowCount);
            return result;
        }
        finally
        {
            usersService.DropScenario(users, uId);
            sequenceService.DropScenario(sequence);
        }
    }

    private (long First, long Current, long Second, long CurrentAfterSecond) RunSequenceCurrentValueScenario<TConnection>(
        TConnection connection,
        string sequence)
        where TConnection : DbConnection
    {
        var testScenario = new SequenceScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(sequence);

        try
        {
            var first = Convert.ToInt64(serviceTest.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            var current = Convert.ToInt64(ExecuteScalarOnConnection(connection, dialect.CurrentSequenceValue(sequence))!, CultureInfo.InvariantCulture);
            var second = Convert.ToInt64(serviceTest.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            var currentAfterSecond = Convert.ToInt64(ExecuteScalarOnConnection(connection, dialect.CurrentSequenceValue(sequence))!, CultureInfo.InvariantCulture);

            Assert.Equal(10L, first);
            Assert.Equal(10L, current);
            Assert.Equal(11L, second);
            Assert.Equal(11L, currentAfterSecond);

            return (first, current, second, currentAfterSecond);
        }
        finally
        {
            serviceTest.DropScenario(sequence);
        }
    }

    private long RunSequenceSelectProjectionScenario<TConnection>(
        TConnection connection,
        string sequence)
        where TConnection : DbConnection
    {
        var testScenario = new SequenceScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(sequence);

        try
        {
            var value = Convert.ToInt64(ExecuteScalarOnConnection(connection, dialect.SelectNextSequenceValue(sequence))!, CultureInfo.InvariantCulture);
            Assert.Equal(10L, value);
            return value;
        }
        finally
        {
            serviceTest.DropScenario(sequence);
        }
    }

    private int RunSequenceCaseWhereMatrixScenario<TConnection>(
        TConnection connection,
        string sequence)
        where TConnection : DbConnection
    {
        var testScenario = new SequenceScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(sequence);

        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"""
WITH seq_first AS (
    SELECT {dialect.NextSequenceValueExpression(sequence)} AS SeqValue
),
seq_second AS (
    SELECT {dialect.NextSequenceValueExpression(sequence)} AS SeqValue
)
SELECT
    s1.SeqValue,
    s2.SeqValue,
    CASE WHEN s1.SeqValue BETWEEN 10 AND 11 THEN 1 ELSE 0 END AS FirstInRange,
    CASE WHEN s2.SeqValue BETWEEN 10 AND 11 THEN 1 ELSE 0 END AS SecondInRange,
    CASE WHEN s1.SeqValue < s2.SeqValue THEN 1 ELSE 0 END AS IsAscending,
    CASE WHEN s1.SeqValue = 10 THEN 1 ELSE 0 END AS FirstIsTen,
    CASE WHEN s2.SeqValue = 11 THEN 1 ELSE 0 END AS SecondIsEleven
FROM seq_first s1
CROSS JOIN seq_second s2
WHERE s1.SeqValue >= 10
  AND s2.SeqValue <= 11
""";

            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(10L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
            Assert.Equal(11L, Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
            Assert.False(reader.Read());
            return 1;
        }
        finally
        {
            serviceTest.DropScenario(sequence);
        }
    }

    private int RunSequenceTemporalMatrixScenario<TConnection>(
        TConnection connection,
        string sequence)
        where TConnection : DbConnection
    {
        var testScenario = new SequenceScenario<TConnection>(dialect);
        var serviceTest = new DmlMutationServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(sequence);

        try
        {
            var nowExpr = dialect.TemporalCurrentTimestampExpression();
            var nextDayExpr = dialect.TemporalDateAddExpression();

            using var command = connection.CreateCommand();
            command.CommandText = $"""
WITH seq_first AS (
    SELECT {dialect.NextSequenceValueExpression(sequence)} AS SeqValue
),
seq_second AS (
    SELECT {dialect.NextSequenceValueExpression(sequence)} AS SeqValue
)
SELECT
    s1.SeqValue,
    s2.SeqValue,
    CASE WHEN s1.SeqValue = 10 THEN 1 ELSE 0 END AS FirstIsTen,
    CASE WHEN s2.SeqValue = 11 THEN 1 ELSE 0 END AS SecondIsEleven,
    CASE WHEN {nowExpr} IS NOT NULL THEN 1 ELSE 0 END AS NowPresent,
    CASE WHEN {nextDayExpr} > {nowExpr} THEN 1 ELSE 0 END AS NextDayAfterNow,
    CASE WHEN s1.SeqValue < s2.SeqValue THEN 1 ELSE 0 END AS IsAscending
FROM seq_first s1
CROSS JOIN seq_second s2
WHERE s1.SeqValue BETWEEN 10 AND 10
  AND s2.SeqValue BETWEEN 11 AND 11
""";

            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal(10L, Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture));
            Assert.Equal(11L, Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(6), CultureInfo.InvariantCulture));
            Assert.False(reader.Read());
            return 1;
        }
        finally
        {
            serviceTest.DropScenario(sequence);
        }
    }

    private (long FirstUserId, long SecondUserId, int JoinedRows) RunSequenceJoinAggregateScenario<TConnection>(
        TConnection connection,
        string users,
        string orders,
        string uId,
        string sequence)
        where TConnection : DbConnection
    {
        var sequenceScenario = new SequenceScenario<TConnection>(dialect);
        var sequenceService = new DmlMutationServiceTest<TConnection>(connection, sequenceScenario, dialect);
        sequenceService.CreateScenario(sequence);

        var joinScenario = new UsersOrdersScenario<TConnection>(dialect, Array.Empty<(int id, string name)>(), Array.Empty<(int id, int userId, string note)>());
        var joinService = new DmlMutationServiceTest<TConnection>(connection, joinScenario, dialect);
        joinService.CreateScenario(users, orders, uId);

        try
        {
            var usersTable = $"{users}_{uId}";
            var ordersTable = $"{orders}_{uId}";
            var firstUserId = Convert.ToInt64(sequenceService.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);
            var secondUserId = Convert.ToInt64(sequenceService.RunSequenceNextValue(sequence), CultureInfo.InvariantCulture);

            ExecuteNonQueryOnConnection(connection, dialect.InsertUser(usersTable, (int)firstUserId, $"Seq-{firstUserId}"));
            ExecuteNonQueryOnConnection(connection, dialect.InsertUser(usersTable, (int)secondUserId, $"Seq-{secondUserId}"));
            ExecuteNonQueryOnConnection(connection, dialect.InsertOrder(ordersTable, usersTable, 100, (int)firstUserId, "A", "o-100", 1.25m, 1, false, dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP"));
            ExecuteNonQueryOnConnection(connection, dialect.InsertOrder(ordersTable, usersTable, 101, (int)firstUserId, "B", "o-101", 2.75m, 2, true, dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP"));
            ExecuteNonQueryOnConnection(connection, dialect.InsertOrder(ordersTable, usersTable, 102, (int)secondUserId, "C", "o-102", 5.50m, 4, false, dialect.Provider == ProviderId.Db2 ? "CURRENT TIMESTAMP" : "CURRENT_TIMESTAMP"));

            using var command = connection.CreateCommand();
            command.CommandText = $"""
SELECT
    u.Id,
    COUNT(o.Id) AS OrderCount,
    SUM(o.Quantity) AS TotalQuantity,
    ROUND(SUM(o.Amount), 2) AS TotalAmount
FROM {usersTable} u
INNER JOIN {ordersTable} o ON o.{usersTable}Id = u.Id
GROUP BY u.Id
ORDER BY u.Id
""";

            using var reader = command.ExecuteReader();
            Assert.True(reader.Read());
            Assert.Equal((int)firstUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
            Assert.Equal(2, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
            Assert.Equal(3, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
            Assert.Equal(4.00m, Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture));

            Assert.True(reader.Read());
            Assert.Equal((int)secondUserId, Convert.ToInt32(reader.GetValue(0), CultureInfo.InvariantCulture));
            Assert.Equal(1, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
            Assert.Equal(4, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
            Assert.Equal(5.50m, Convert.ToDecimal(reader.GetValue(3), CultureInfo.InvariantCulture));

            Assert.False(reader.Read());
            return (firstUserId, secondUserId, 2);
        }
        finally
        {
            joinService.DropScenario(users, orders, uId);
            sequenceService.DropScenario(sequence);
        }
    }

    private static (long MinId, long MaxId, long RowCount) ExecuteAggregateReadback(DbConnection connection, string tableName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT MIN(Id), MAX(Id), COUNT(*) FROM {tableName}";
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException("Sequence insert round-trip readback returned no rows.");
        }

        return (
            Convert.ToInt64(reader.GetValue(0), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(1), CultureInfo.InvariantCulture),
            Convert.ToInt64(reader.GetValue(2), CultureInfo.InvariantCulture));
    }

    private static void ExecuteNonQueryOnConnection(
        DbConnection connection,
        string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static object? ExecuteScalarOnConnection(
        DbConnection connection,
        string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        return command.ExecuteScalar();
    }

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}


