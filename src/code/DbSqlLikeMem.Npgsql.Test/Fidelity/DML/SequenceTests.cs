using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Tests.DML;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared sequence workflows.
/// PT: Executa testes de fidelidade PostgreSQL para os fluxos compartilhados de sequence.
/// </summary>
public class SequenceTests(
    ITestOutputHelper helper
    ) : SequenceTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
    /// <summary>
    /// EN: Verifies ALTER SEQUENCE RESTART WITH resets the next generated value for PostgreSQL sequence coverage.
    /// PT: Verifica se ALTER SEQUENCE RESTART WITH reinicia o proximo valor gerado na cobertura de sequence do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task AlterSequenceRestartWith_ShouldResetNextGeneratedValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceAlterRestartServiceTest>() as long[];

        _ = new long[] { 10L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies ALTER SEQUENCE INCREMENT BY changes the next generated values for PostgreSQL coverage.
    /// PT: Verifica se ALTER SEQUENCE INCREMENT BY altera os proximos valores gerados na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task AlterSequenceIncrementBy_ShouldChangeNextGeneratedValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceIncrementByServiceTest>() as long[];

        _ = new long[] { 10L, 13L, 16L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies ALTER SEQUENCE OWNED BY NONE keeps the sequence available after the table is dropped.
    /// PT: Verifica se ALTER SEQUENCE OWNED BY NONE mantem a sequence disponivel depois que a tabela e removida.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task AlterSequenceOwnedByNone_ShouldPreserveSequenceAfterTableDrop()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceOwnedByNoneServiceTest>() as long[];

        _ = new long[] { 1L, 2L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies ALTER SEQUENCE OWNED BY drops the sequence when the owning table is dropped.
    /// PT: Verifica se ALTER SEQUENCE OWNED BY remove a sequence quando a tabela proprietaria e removida.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task AlterSequenceOwnedByTable_ShouldDropSequenceWithTable()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceOwnedByTableServiceTest>() as long[];

        _ = new long[] { 1L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies a CYCLE sequence wraps back to the minimum value after reaching the maximum value.
    /// PT: Verifica se uma sequence com CYCLE volta ao valor minimo apos atingir o valor maximo.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CreateCycleSequence_ShouldWrapBackToMinimumValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceCycleServiceTest>() as long[];

        _ = new long[] { 1L, 2L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies a bounded sequence stops after reaching its maximum value in PostgreSQL coverage.
    /// PT: Verifica se uma sequence limitada para depois de atingir o valor maximo na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CreateBoundedSequence_ShouldStopAtMaximumValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceMaxValueServiceTest>() as long[];

        _ = new long[] { 5L, 6L, 7L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies a descending sequence follows its negative increment in PostgreSQL coverage.
    /// PT: Verifica se uma sequence descendente segue o incremento negativo na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CreateDescendingSequence_ShouldFollowNegativeIncrement()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceDescendingServiceTest>() as long[];

        _ = new long[] { 5L, 3L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies a lower-bounded sequence stops after reaching its minimum value in PostgreSQL coverage.
    /// PT: Verifica se uma sequence com limite inferior para depois de atingir o valor minimo na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CreateBoundedDescendingSequence_ShouldStopAtMinimumValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceMinValueServiceTest>() as long[];

        _ = new long[] { 5L, 3L, 1L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies DROP SEQUENCE IF EXISTS stays idempotent after the sequence is already removed.
    /// PT: Verifica se DROP SEQUENCE IF EXISTS continua idempotente depois que a sequence ja foi removida.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task DropSequenceIfExists_ShouldBeIdempotent()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceDropIfExistsServiceTest>() as long[];

        _ = new long[] { 10L, 1L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies currval and lastval follow the current session after a sequence restart in PostgreSQL coverage.
    /// PT: Verifica se currval e lastval seguem a sessao atual apos um restart de sequence na cobertura do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CurrVal_And_LastVal_ShouldFollowRestartedSequence()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceAlterRestartServiceTest>(true) as long[];

        _ = new long[] { 10L, 10L, 10L, 11L, 11L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies setval with is_called false keeps lastval unchanged until the next sequence value is consumed.
    /// PT: Verifica se setval com is_called false mantem lastval inalterado ate que o proximo valor da sequence seja consumido.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task SetVal_WithIsCalledFalse_ShouldKeepLastValUntilNextValue()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceSetValServiceTest>() as long[];

        _ = new long[] { 10L, 10L, 40L, 10L, 40L, 40L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies currval stays local to each PostgreSQL session after the first nextval call.
    /// PT: Verifica se currval permanece local a cada sessao PostgreSQL apos a primeira chamada de nextval.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CurrVal_ShouldBeSessionLocal()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceSessionLocalServiceTest>() as object[];
        Assert.NotNull(result);

        _ = new object?[] { 1L, 10L, 10L, 1L, 11L, 11L, 10L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies lastval stays local to each PostgreSQL session after the first nextval call.
    /// PT: Verifica se lastval permanece local a cada sessao PostgreSQL apos a primeira chamada de nextval.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task LastVal_ShouldBeSessionLocal()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceSessionLocalServiceTest>(true) as object[];
        Assert.NotNull(result);

        _ = new object?[] { 1L, 10L, 10L, 1L, 11L, 11L, 10L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies schema-qualified sequence access works for PostgreSQL fidelity coverage.
    /// PT: Verifica se o acesso a sequence qualificada por schema funciona na cobertura de fidelidade do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task SchemaQualifiedSequence_ShouldWork()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceSchemaQualifiedServiceTest>() as long[];

        _ = new long[] { 7L, 11L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies dropping a sequence inside a transaction is rolled back in PostgreSQL fidelity coverage.
    /// PT: Verifica se remover uma sequence dentro de uma transacao e revertido na cobertura de fidelidade do PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task DropSequence_ShouldRollbackWithTransaction()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<SequenceScenario, SequenceDropRollbackServiceTest>() as long[];

        _ = new long[] { 10L, 1L, 11L }.Should().Equal(result);
    }

    /// <summary>
    /// EN: Verifies CREATE SEQUENCE IF NOT EXISTS keeps the original PostgreSQL sequence definition.
    /// PT: Verifica se CREATE SEQUENCE IF NOT EXISTS preserva a definicao original da sequence PostgreSQL.
    /// </summary>
    [Fact]
    [Trait("Category", "ExtendedPostgreSqlMock")]
    public async Task CreateSequenceIfNotExists_ShouldPreserveExistingSequence()
    {
        using var testService = new FidelityTestService<NpgsqlConnectionMock, NpgsqlConnection>(
            () => new NpgsqlConnectionMock(),
            s => new NpgsqlConnection(s),
            new NpgsqlProviderSqlDialect());

        var result = await testService.RunTestAsync<NoopScenario, SequenceCreateIfNotExistsServiceTest>() as long[];

        _ = new long[] { 13L, 15L }.Should().Equal(result);
    }
}
