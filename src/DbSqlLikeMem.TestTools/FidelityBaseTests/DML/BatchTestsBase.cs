using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared batch fidelity tests across mock and container runs.
/// PT: Fornece testes de fidelidade de batch compartilhados entre execucoes mock e container.
/// </summary>
public abstract class BatchTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a batch insert of ten rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de dez linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceInsert10Test()
        => Assert.Equal(
            10,
            RunBatchComparison(
                (service, tableName) => service.RunBatchInsert10(tableName),
                (service, tableName) => service.RunBatchInsert10(tableName)));

    /// <summary>
    /// EN: Verifies that a batch insert of one hundred rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de cem linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceInsert100Test()
        => Assert.Equal(
            100,
            RunBatchComparison(
                (service, tableName) => service.RunBatchInsert100(tableName),
                (service, tableName) => service.RunBatchInsert100(tableName)));

    /// <summary>
    /// EN: Verifies that batched single-row inserts persist the expected row count for the current provider.
    /// PT: Verifica se inserts unitarios em lote persistem a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceRowCountInBatchTest()
        => Assert.Equal(
            2,
            RunBatchComparison(
                (service, tableName) => service.RunRowCountInBatch(tableName),
                (service, tableName) => service.RunRowCountInBatch(tableName)));

    /// <summary>
    /// EN: Verifies that a mixed batch keeps reads and writes consistent for the current provider.
    /// PT: Verifica se um lote misto mantem leituras e escritas consistentes para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceMixedReadWriteTest()
        => Assert.Equal(
            "Alice",
            RunBatchComparison(
                (service, tableName) => service.RunBatchMixedReadWrite(tableName),
                (service, tableName) => service.RunBatchMixedReadWrite(tableName)));

    /// <summary>
    /// EN: Verifies that a scalar batch keeps the expected row count and second value for the current provider.
    /// PT: Verifica se um lote escalar mantem a contagem esperada de linhas e o segundo valor para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceScalarTest()
        => Assert.Equal(
            "Bob",
            RunBatchComparison(
                (service, tableName) => service.RunBatchScalar(tableName),
                (service, tableName) => service.RunBatchScalar(tableName)));

    /// <summary>
    /// EN: Verifies that a non-query batch keeps the expected final row count for the current provider.
    /// PT: Verifica se um lote sem consulta mantem a contagem final esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceNonQueryTest()
        => Assert.Equal(
            1,
            RunBatchComparison(
                (service, tableName) => service.RunBatchNonQuery(tableName),
                (service, tableName) => service.RunBatchNonQuery(tableName)));

    /// <summary>
    /// EN: Verifies that a batch transaction control flow keeps the committed table name visible for the current provider.
    /// PT: Verifica se um fluxo de controle transacional em batch mantém o nome da tabela confirmado visivel para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceTransactionControlTest()
    {
        var result = RunBatchComparison(
            (service, tableName) => service.RunBatchTransactionControl(tableName),
            (service, tableName) => service.RunBatchTransactionControl(tableName));

        Assert.StartsWith("Users", result, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// EN: Verifies that a batch reader can iterate through multiple result sets for the current provider.
    /// PT: Verifica se um leitor em lote pode iterar por multiplos conjuntos de resultados para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceReaderMultiResultTest()
        => Assert.Equal(
            "Alice",
            RunBatchComparison(
                (service, tableName) => service.RunBatchReaderMultiResult(tableName),
                (service, tableName) => service.RunBatchReaderMultiResult(tableName)));

    private TResult RunBatchComparison<TResult>(
        Func<BatchServiceTest<T>, string, TResult> runMock,
        Func<BatchServiceTest<T2>, string, TResult> runContainer)
    {
        var users = "Users";
        var uId = NewToken();

        using var connMock = connectionMock();
        connMock.Open();
        var resultMock = RunBatchScenario(connMock, users, uId, runMock);

        if (IsInsertContainerComparisonEnabled(dialect.Provider)
            && TryResolveContainerConnectionString(dialect.Provider, out var connectionString))
        {
            using var connContainer = connectionContainer(connectionString);
            connContainer.Open();
            var resultContainer = RunBatchScenario(connContainer, users, uId, runContainer);
            Assert.Equal(resultMock, resultContainer);
        }

        return resultMock;
    }

    private TResult RunBatchScenario<TConnection, TResult>(
        TConnection connection,
        string users,
        string uId,
        Func<BatchServiceTest<TConnection>, string, TResult> runner)
        where TConnection : DbConnection
    {
        var testScenario = new InsertUsersScenario<TConnection>(dialect);
        var serviceTest = new BatchServiceTest<TConnection>(connection, testScenario, dialect);
        serviceTest.CreateScenario(users, uId);

        try
        {
            var tableName = BuildTableName(users, uId);
            return runner(serviceTest, tableName);
        }
        finally
        {
            serviceTest.DropScenario(users, uId);
        }
    }

    private string BuildTableName(string users, string uId)
        => dialect.Provider == ProviderId.Oracle
            ? users.ToLowerInvariant()
            : $"{users}_{uId}";

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
