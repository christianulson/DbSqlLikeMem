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
        => RunBatchComparison(
                (service, tableName) => service.RunBatchInsert10(tableName),
                (service, tableName) => service.RunBatchInsert10(tableName))
            .Should()
            .Be(10);

    /// <summary>
    /// EN: Verifies that a batch insert of ten rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de dez linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchInsert10Test()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchInsert10(tableName),
                (service, tableName) => service.RunBatchInsert10(tableName))
            .Should()
            .Be(10);

    /// <summary>
    /// EN: Verifies that a batch insert of one hundred rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de cem linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceInsert100Test()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchInsert100(tableName),
                (service, tableName) => service.RunBatchInsert100(tableName))
            .Should()
            .Be(100);

    /// <summary>
    /// EN: Verifies that a batch insert of one hundred rows persists the expected count for the current provider.
    /// PT: Verifica se um insert em lote de cem linhas persiste a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchInsert100Test()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchInsert100(tableName),
                (service, tableName) => service.RunBatchInsert100(tableName))
            .Should()
            .Be(100);

    /// <summary>
    /// EN: Verifies that batched single-row inserts persist the expected row count for the current provider.
    /// PT: Verifica se inserts unitarios em lote persistem a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceRowCountInBatchTest()
        => RunBatchComparison(
                (service, tableName) => service.RunRowCountInBatch(tableName),
                (service, tableName) => service.RunRowCountInBatch(tableName))
            .Should()
            .Be(2);

    /// <summary>
    /// EN: Verifies that batched single-row inserts persist the expected row count for the current provider.
    /// PT: Verifica se inserts unitarios em lote persistem a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void RowCountInBatchTest()
        => RunBatchComparison(
                (service, tableName) => service.RunRowCountInBatch(tableName),
                (service, tableName) => service.RunRowCountInBatch(tableName))
            .Should()
            .Be(2);

    /// <summary>
    /// EN: Verifies that a mixed batch keeps reads and writes consistent for the current provider.
    /// PT: Verifica se um lote misto mantem leituras e escritas consistentes para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceMixedReadWriteTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchMixedReadWrite(tableName),
                (service, tableName) => service.RunBatchMixedReadWrite(tableName))
            .Should()
            .Be("Alice");

    /// <summary>
    /// EN: Verifies that a mixed batch keeps reads and writes consistent for the current provider.
    /// PT: Verifica se um lote misto mantem leituras e escritas consistentes para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchMixedReadWriteTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchMixedReadWrite(tableName),
                (service, tableName) => service.RunBatchMixedReadWrite(tableName))
            .Should()
            .Be("Alice");

    /// <summary>
    /// EN: Verifies that a scalar batch keeps the expected row count and second value for the current provider.
    /// PT: Verifica se um lote escalar mantem a contagem esperada de linhas e o segundo valor para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceScalarTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchScalar(tableName),
                (service, tableName) => service.RunBatchScalar(tableName))
            .Should()
            .Be("Bob");

    /// <summary>
    /// EN: Verifies that a scalar batch keeps the expected row count and second value for the current provider.
    /// PT: Verifica se um lote escalar mantem a contagem esperada de linhas e o segundo valor para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchScalarTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchScalar(tableName),
                (service, tableName) => service.RunBatchScalar(tableName))
            .Should()
            .Be("Bob");

    /// <summary>
    /// EN: Verifies that a non-query batch keeps the expected final row count for the current provider.
    /// PT: Verifica se um lote sem consulta mantem a contagem final esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceNonQueryTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchNonQuery(tableName),
                (service, tableName) => service.RunBatchNonQuery(tableName))
            .Should()
            .Be(1);

    /// <summary>
    /// EN: Verifies that a non-query batch keeps the expected final row count for the current provider.
    /// PT: Verifica se um lote sem consulta mantem a contagem final esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchNonQueryTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchNonQuery(tableName),
                (service, tableName) => service.RunBatchNonQuery(tableName))
            .Should()
            .Be(1);

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

        result.Should().StartWithEquivalentOf("Users");
    }

    /// <summary>
    /// EN: Verifies that a batch transaction control flow keeps the committed table name visible for the current provider.
    /// PT: Verifica se um fluxo de controle transacional em batch mantém o nome da tabela confirmado visivel para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchTransactionControlTest()
    {
        var result = RunBatchComparison(
            (service, tableName) => service.RunBatchTransactionControl(tableName),
            (service, tableName) => service.RunBatchTransactionControl(tableName));

        result.Should().StartWithEquivalentOf("Users");
    }

    /// <summary>
    /// EN: Verifies that a batch reader can iterate through multiple result sets for the current provider.
    /// PT: Verifica se um leitor em lote pode iterar por multiplos conjuntos de resultados para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchServiceReaderMultiResultTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchReaderMultiResult(tableName),
                (service, tableName) => service.RunBatchReaderMultiResult(tableName))
            .Should()
            .Be("Alice");

    /// <summary>
    /// EN: Verifies that a batch reader can iterate through multiple result sets for the current provider.
    /// PT: Verifica se um leitor em lote pode iterar por multiplos conjuntos de resultados para o provedor atual.
    /// </summary>
    [Fact]
    public void BatchReaderMultiResultTest()
        => RunBatchComparison(
                (service, tableName) => service.RunBatchReaderMultiResult(tableName),
                (service, tableName) => service.RunBatchReaderMultiResult(tableName))
            .Should()
            .Be("Alice");

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
            resultMock.Should().Be(resultContainer);
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
        => $"{users}_{uId}";

    private static string NewToken()
        => Guid.NewGuid().ToString("N")[..8].ToUpperInvariant();
}
