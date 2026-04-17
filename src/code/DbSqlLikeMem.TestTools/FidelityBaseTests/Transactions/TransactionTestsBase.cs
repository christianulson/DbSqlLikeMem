using DbSqlLikeMem.TestTools;
using DbSqlLikeMem.TestTools.DML;
using System.Data.Common;

namespace DbSqlLikeMem.TestTools.Tests.Transactions;

/// <summary>
/// EN: Provides shared transaction fidelity tests for commit, rollback, and savepoint workflows across mock and container runs.
/// PT: Fornece testes de fidelidade de transacao compartilhados para fluxos de commit, rollback e savepoint entre mock e container.
/// </summary>
public abstract class TransactionTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that a transaction commit persists the inserted row for the current provider.
    /// PT: Verifica se o commit de uma transacao persiste a linha inserida para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TransactionCommitTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario>(
            [],
            (s, a) => Task.FromResult<object?>(s.RunTransactionCommit()));

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a transaction rollback removes the inserted row for the current provider.
    /// PT: Verifica se o rollback de uma transacao remove a linha inserida para o provedor atual.
    /// </summary>
    [Fact]
    public async Task TransactionRollbackTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario>(
            [],
            (s, a) => Task.FromResult<object?>(s.RunTransactionRollback()));

        result.Should().Be(0);
    }

    /// <summary>
    /// EN: Verifies that creating a savepoint works for the current provider.
    /// PT: Verifica se a criacao de um savepoint funciona para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SavepointCreateTest()
    {
        var result = await RunFidelityTestAsync<NoopScenario>(
            [],
            (s, a) =>
            {
                s.RunSavepointCreate();
                return Task.FromResult<object?>(null);
            });

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that rolling back to a savepoint keeps the expected row count for the current provider.
    /// PT: Verifica se o rollback para um savepoint mantem a contagem de linhas esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task RollbackToSavepointTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario>(
            [],
            (s, a) => Task.FromResult<object?>(s.RunRollbackToSavepoint()));

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that releasing a savepoint works for the current provider.
    /// PT: Verifica se a liberacao de um savepoint funciona para o provedor atual.
    /// </summary>
    [Fact]
    public async Task ReleaseSavepointTest()
    {
        if (!SupportsReleaseSavepointWorkflow())
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<NoopScenario>(
                [],
                (s, a) =>
                {
                    s.RunReleaseSavepoint();
                    return Task.FromResult<object?>(null);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<NoopScenario>(
            [],
            (s, a) =>
            {
                s.RunReleaseSavepoint();
                return Task.FromResult<object?>(null);
            });

        result.Should().BeNull();
    }

    /// <summary>
    /// EN: Verifies that nested savepoints keep the expected row count for the current provider.
    /// PT: Verifica se savepoints aninhados mantem a contagem de linhas esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task NestedSavepointFlowTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario>(
            [],
            (s, a) => Task.FromResult<object?>(s.RunNestedSavepointFlow()));

        result.Should().Be(2);
    }

    private async Task<object?> RunFidelityTestAsync<TScenario>(
        object?[][] initialData,
        Func<DmlMutationServiceTest, object[], Task<object?>> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, DmlMutationServiceTest>(runTest, args);
    }

    private bool SupportsReleaseSavepointWorkflow()
        => dialect.Provider is not ProviderId.SqlServer
            and not ProviderId.SqlAzure
            and not ProviderId.Oracle;
}
