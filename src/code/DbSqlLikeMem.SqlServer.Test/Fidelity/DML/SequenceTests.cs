using DbSqlLikeMem.SqlServer.TestTools;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.SqlServer.Test.Fidelity.DML;

/// <summary>
/// EN: Runs SQL Server fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade SQL Server para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<SqlServerConnectionMock, SqlConnection>(
    helper,
    new SqlServerProviderSqlDialect(),
    () => new SqlServerConnectionMock(),
    s => new SqlConnection(s)
    )
{
    /// <summary>
    /// EN: Verifies NEXT VALUE FOR can be used inside a filtered SQL Server query and advances in execution order.
    /// PT: Verifica se NEXT VALUE FOR pode ser usado dentro de uma consulta filtrada do SQL Server e avanca na ordem de execucao.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task SequenceExpressions_ShouldReturnExpectedValues()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect(),
            [[(1, "Alice")]]);

        var result = await testService.RunTestAsync<SequenceScenario, UsersScenario, SequenceExpressionFilterServiceTest>() as long[];

        _ = new long[] { 10L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies sys.sequences current_value follows the sequence before and after generated values for SQL Server coverage.
    /// PT: Verifica se sys.sequences current_value acompanha a sequence antes e depois de valores gerados na cobertura do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task CurrentValue_ShouldTrackGeneratedValues()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceCurrentValueServiceTest>() as long[];

        _ = new long[] { 10L, 10L, 10L, 11L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies CREATE SEQUENCE INCREMENT BY changes the next generated values for SQL Server coverage.
    /// PT: Verifica se CREATE SEQUENCE INCREMENT BY altera os proximos valores gerados na cobertura do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task CreateSequenceIncrementBy_ShouldChangeNextGeneratedValue()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceIncrementByServiceTest>() as long[];

        _ = new long[] { 10L, 13L, 16L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies a CYCLE sequence wraps back to the minimum value after reaching the maximum value for SQL Server coverage.
    /// PT: Verifica se uma sequence com CYCLE volta ao valor minimo apos atingir o valor maximo na cobertura do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task CreateCycleSequence_ShouldWrapBackToMinimumValue()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceCycleServiceTest>() as long[];

        _ = new long[] { 1L, 2L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies ALTER SEQUENCE RESTART WITH resets the next generated value for SQL Server sequence coverage.
    /// PT: Verifica se ALTER SEQUENCE RESTART WITH reinicia o proximo valor gerado na cobertura de sequence do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task AlterSequenceRestartWith_ShouldResetNextGeneratedValue()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceAlterRestartServiceTest>() as long[];

        _ = new long[] { 40L, 41L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies DROP SEQUENCE IF EXISTS stays idempotent after the sequence is already removed.
    /// PT: Verifica se DROP SEQUENCE IF EXISTS continua idempotente depois que a sequence ja foi removida.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedSqlServerMock")]
    public async Task DropSequenceIfExists_ShouldBeIdempotent()
    {
        using var testService = new FidelityTestService<SqlServerConnectionMock, SqlConnection>(
            () => new SqlServerConnectionMock(),
            s => new SqlConnection(s),
            new SqlServerProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceDropIfExistsServiceTest>() as long[];

        _ = new long[] { 10L, 1L }.Should().Equal(result);
    }
}
