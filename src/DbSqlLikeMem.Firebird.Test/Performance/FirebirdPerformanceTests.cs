using System.Diagnostics;

namespace DbSqlLikeMem.Firebird.Test.Performance;

/// <summary>
/// EN: Measures CRUD throughput against the Firebird mock provider.
/// PT: Mede o throughput CRUD contra o provedor mock do Firebird.
/// </summary>
public sealed class FirebirdPerformanceTests : XUnitTestBase
{
    private readonly FirebirdConnectionMock _connection;

    /// <summary>
    /// EN: Creates the Firebird performance fixture with a seeded Users table.
    /// PT: Cria a fixture de performance do Firebird com a tabela Users semeada.
    /// </summary>
    /// <param name="helper">EN: The xUnit output helper. PT: O helper de saida do xUnit.</param>
    public FirebirdPerformanceTests(ITestOutputHelper helper) : base(helper)
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);
        var numbers = db.AddTable("Numbers", [
            new("Id", DbType.Int32, false)
        ]);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });
        var numbersParam = db.AddTable("NumbersParam", [
            new("Id", DbType.Int32, false)
        ]);
        numbersParam.Add(new Dictionary<int, object?> { [0] = 1 });
        numbersParam.Add(new Dictionary<int, object?> { [0] = 2 });
        numbersParam.Add(new Dictionary<int, object?> { [0] = 3 });

        _connection = new FirebirdConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Verifies baseline CRUD performance metrics for the Users table.
    /// PT: Verifica as metricas base de performance CRUD para a tabela Users.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_crud_baseline_metrics()
    {
        const int totalRows = 2000;
        const int sampledReads = 1000;

        var insertStatements = new string[totalRows];
        var updateStatements = new string[totalRows];
        var deleteStatements = new string[totalRows];
        var readStatements = new string[sampledReads];

        for (var i = 1; i <= totalRows; i++)
        {
            var rowIndex = i - 1;
            insertStatements[rowIndex] = $"INSERT INTO Users (Id, Name, Email) VALUES ({i}, 'User {i}', 'user{i}@mail.com')";
            updateStatements[rowIndex] = $"UPDATE Users SET Name = 'Updated {i}' WHERE Id = {i}";
            deleteStatements[rowIndex] = $"DELETE FROM Users WHERE Id = {i}";
        }

        for (var i = 1; i <= sampledReads; i++)
        {
            var userId = (i % totalRows) + 1;
            readStatements[i - 1] = $"SELECT Id, Name, Email FROM Users WHERE Id = {userId}";
        }

        using var command = new FirebirdCommandMock(_connection);

        var insertedRows = 0;
        var insertElapsedMs = Measure(() =>
        {
            for (var i = 0; i < insertStatements.Length; i++)
            {
                command.CommandText = insertStatements[i];
                insertedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, insertedRows);

        var successfulReads = 0;
        var readElapsedMs = Measure(() =>
        {
            for (var i = 0; i < readStatements.Length; i++)
            {
                command.CommandText = readStatements[i];
                using var reader = command.ExecuteReader();
                if (reader.Read())
                    successfulReads++;
            }
        });
        Assert.Equal(sampledReads, successfulReads);

        var updatedRows = 0;
        var updateElapsedMs = Measure(() =>
        {
            for (var i = 0; i < updateStatements.Length; i++)
            {
                command.CommandText = updateStatements[i];
                updatedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, updatedRows);

        var deletedRows = 0;
        var deleteElapsedMs = Measure(() =>
        {
            for (var i = 0; i < deleteStatements.Length; i++)
            {
                command.CommandText = deleteStatements[i];
                deletedRows += command.ExecuteNonQuery();
            }
        });
        Assert.Equal(totalRows, deletedRows);

        Console.WriteLine($"[Firebird][Performance] Inserts: {totalRows} in {insertElapsedMs}ms ({OpsPerSecond(totalRows, insertElapsedMs):F2} ops/s, {OpsAVG(totalRows, insertElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[Firebird][Performance] Reads: {sampledReads} in {readElapsedMs}ms ({OpsPerSecond(sampledReads, readElapsedMs):F2} ops/s, {OpsAVG(sampledReads, readElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[Firebird][Performance] Updates: {totalRows} in {updateElapsedMs}ms ({OpsPerSecond(totalRows, updateElapsedMs):F2} ops/s, {OpsAVG(totalRows, updateElapsedMs):F2} ms/avg)");
        Console.WriteLine($"[Firebird][Performance] Deletes: {totalRows} in {deleteElapsedMs}ms ({OpsPerSecond(totalRows, deleteElapsedMs):F2} ops/s, {OpsAVG(totalRows, deleteElapsedMs):F2} ms/avg)");

        Assert.Empty(_connection.GetTable("Users"));
    }

    /// <summary>
    /// EN: Measures repeated scalar function execution for the Firebird mock function surface.
    /// PT: Mede a execucao repetida de funcoes escalares na surface de funcoes do mock Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_scalar_function_baseline_metrics()
    {
        const int totalCalls = 2000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = "CREATE FUNCTION fn_bench(baseValue INT, incrementValue INT DEFAULT 2) RETURNS INT AS BEGIN RETURN baseValue + incrementValue; END";
        command.ExecuteNonQuery();

        var lastResult = 0;
        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
            {
                command.CommandText = "SELECT fn_bench(40) FROM RDB$DATABASE";
                lastResult = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
            }
        });

        Assert.Equal(42, lastResult);
        Console.WriteLine($"[Firebird][Performance] Scalar function calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with scoped input parameters for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com parametros de entrada no escopo da surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_parameter_binding_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 1)
AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (:tenantId, 'Bench');
    DELETE FROM Users WHERE Id = :tenantId;
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with nested BEGIN ... END compound statements for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com blocos compostos BEGIN ... END aninhados na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_nested_compound_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK AS
BEGIN
    BEGIN
        INSERT INTO Users (Id, Name, Email) VALUES (1, 'Bench A', 'bench.a@mail.com');
        INSERT INTO Users (Id, Name, Email) VALUES (2, 'Bench B', 'bench.b@mail.com');
    END;
    DELETE FROM Users WHERE Id = 1;
    DELETE FROM Users WHERE Id = 2;
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK nested compound calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with IF ... THEN ... ELSE branching for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com ramificacao IF ... THEN ... ELSE na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_if_branching_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 1)
RETURNS (outValue INT)
AS
BEGIN
    IF (tenantId = 1) THEN
    BEGIN
        outValue = tenantId + 1;
    END
    ELSE
    BEGIN
        outValue = 0;
    END;

    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Bench');
    DELETE FROM Users WHERE Id = :outValue;
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK IF branch calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with WHILE loop branching for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com loop WHILE na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_while_loop_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    counter = 0;
    WHILE (counter < 3) DO
    BEGIN
        counter = counter + 1;
        INSERT INTO Users (Id, Name, Email) VALUES (:counter, 'Bench Loop', 'bench.loop@mail.com');
        DELETE FROM Users WHERE Id = :counter;
    END
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK WHILE loop calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with FOR SELECT iteration for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com iteracao FOR SELECT na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_for_select_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR SELECT Id FROM Numbers ORDER BY Id INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name, Email) VALUES (:counter, 'Bench For', 'bench.for@mail.com');
        DELETE FROM Users WHERE Id = :counter;
    END
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK FOR SELECT calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with FOR EXECUTE STATEMENT iteration for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com iteracao FOR EXECUTE STATEMENT na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_for_execute_statement_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT 'SELECT Id FROM Numbers ORDER BY Id' INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name, Email) VALUES (:counter, 'Bench For Exec', 'bench.forexec@mail.com');
        DELETE FROM Users WHERE Id = :counter;
    END
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK FOR EXECUTE STATEMENT calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with parameterized FOR EXECUTE STATEMENT iteration for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com iteracao FOR EXECUTE STATEMENT parametrizada na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_for_execute_statement_parameterized_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT ('SELECT Id FROM NumbersParam WHERE Id >= :minId ORDER BY Id') (minId := 1) INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name, Email) VALUES (:counter, 'Bench For Exec Param', 'bench.forexecparam@mail.com');
        DELETE FROM Users WHERE Id = :counter;
    END
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK FOR EXECUTE STATEMENT parameterized calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    /// <summary>
    /// EN: Measures repeated EXECUTE BLOCK execution with parameterized EXECUTE STATEMENT payloads for the Firebird mock surface.
    /// PT: Mede a execucao repetida de EXECUTE BLOCK com cargas parametrizadas de EXECUTE STATEMENT na surface mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "Performance")]
    public void Should_report_execute_block_parameterized_execute_statement_metrics()
    {
        const int totalCalls = 1000;

        using var command = new FirebirdCommandMock(_connection);
        command.CommandText = """
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT ('INSERT INTO Users (Id, Name, Email) VALUES (:userId, :userName, :userEmail)') (userId := 1, userName := 'Bench Exec', userEmail := 'bench.exec@mail.com') WITH CALLER PRIVILEGES;
    DELETE FROM Users WHERE Id = 1;
END
""";

        var elapsed = Measure(() =>
        {
            for (var i = 0; i < totalCalls; i++)
                command.ExecuteNonQuery();
        });

        Assert.Empty(_connection.GetTable("Users"));
        Console.WriteLine($"[Firebird][Performance] EXECUTE BLOCK parameterized EXECUTE STATEMENT calls: {totalCalls} in {elapsed}ms ({OpsPerSecond(totalCalls, elapsed):F2} ops/s, {OpsAVG(totalCalls, elapsed):F2} ms/avg)");
    }

    private static long Measure(Action action)
    {
        var watch = Stopwatch.StartNew();
        action();
        watch.Stop();
        return watch.ElapsedMilliseconds;
    }

    private static double OpsPerSecond(int operationCount, long elapsedMs)
    {
        if (elapsedMs <= 0)
            return operationCount;

        return operationCount / (elapsedMs / 1000d);
    }

    private static double OpsAVG(int operationCount, long elapsedMs)
    {
        if (operationCount <= 0)
            return elapsedMs;

        return (double)elapsedMs / operationCount;
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}
