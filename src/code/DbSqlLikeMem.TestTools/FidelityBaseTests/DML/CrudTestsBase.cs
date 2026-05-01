using DbSqlLikeMem.TestTools.DML;

namespace DbSqlLikeMem.TestTools.Tests.DML;

/// <summary>
/// EN: Provides shared CRUD fidelity tests for update and delete workflows across mock and container runs.
/// PT-br: Fornece testes de fidelidade CRUD compartilhados para fluxos de update e delete entre mock e container.
/// </summary>
public abstract class CrudTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{
    /// <summary>
    /// EN: Verifies that an update followed by a delete keeps the expected row count and remaining value for the current provider.
    /// PT-br: Verifica se um update seguido de delete mantem a contagem esperada de linhas e o valor restante para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task UpdateDeleteRoundTripTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationUpdateDeleteRoundTripServiceTest>()).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that update and delete actions committed inside a transaction keep the expected final state.
    /// PT-br: Verifica se acoes de update e delete confirmadas dentro de uma transacao mantem o estado final esperado.
    /// </summary>
    [FidelityFact]
    public async Task TransactionalUpdateDeleteCommitTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationTransactionalUpdateDeleteCommitServiceTest>()).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that updating a single user row persists the expected value for the current provider.
    /// PT-br: Verifica se a atualizacao de uma unica linha de usuario persiste o valor esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task UpdateByPkTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationUpdateByPkServiceTest>()).Should().BeEquivalentTo("Alice-v2");
    }

    /// <summary>
    /// EN: Verifies that the returning-update benchmark alias keeps the same update-by-key result for the current provider.
    /// PT-br: Verifica se o alias de benchmark ReturningUpdate mantem o mesmo resultado de update por chave para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ReturningUpdateTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationUpdateByPkServiceTest>()).Should().BeEquivalentTo("Alice-v2");
    }

    /// <summary>
    /// EN: Verifies that deleting a single user row keeps the expected remaining row for the current provider.
    /// PT-br: Verifica se a exclusao de uma unica linha de usuario mantem a linha restante esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task DeleteByPkTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationDeleteByPkServiceTest>()).Should().BeEquivalentTo(new List<List<object[]>>
        {
            new() {
                new object[] { 2, "Bob" }
            }
        });
    }

    /// <summary>
    /// EN: Verifies that an update reports a valid affected-row count and persists the new value for the current provider.
    /// PT-br: Verifica se uma atualizacao retorna uma contagem valida de linhas afetadas e persiste o novo valor para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task RowCountAfterUpdateTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);

        (await testService.RunTestAsync<UsersScenario, DmlMutationRowCountAfterUpdateServiceTest>()).Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters update and delete rows correctly in the users table for the current provider, including Oracle empty-string normalization in the updated email column.
    /// PT-br: Verifica se parametros tipados do provedor atualizam e excluem linhas corretamente na tabela de usuarios do provedor atual, incluindo a normalizacao de string vazia no email atualizado para Oracle.
    /// </summary>
    [FidelityFact]
    public async Task ParameterUpdateDeleteRoundTripTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect,
            [(1, "Alice"), (2, "Bob")]);
        var updatedAt = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        var result = await testService.RunTestAsync<UsersScenario, DmlMutationParameterUpdateDeleteRoundTripServiceTest>(
            "Alice-v2",
            string.Empty,
            true,
            (short)31,
            123.45m,
            updatedAt,
            "{\"theme\":\"dark\"}",
            2);
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters insert rows correctly in the users table for the current provider.
    /// PT-br: Verifica se parametros tipados do provedor inserem linhas corretamente na tabela de usuarios do provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParameterInsertRoundTripTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect);
        var createdAt1 = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeParameterDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));
        var updatedAt1 = NormalizeParameterDateTimeInput(new DateTime(2024, 3, 4, 5, 6, 7, DateTimeKind.Unspecified));
        var updatedAt2 = NormalizeParameterDateTimeInput(new DateTime(2024, 4, 5, 6, 7, 8, DateTimeKind.Unspecified));

        var result = await testService.RunTestAsync<InsertUsersScenario, DmlMutationParameterInsertRoundTripServiceTest>(
            "Alice-v2",
                "Bob-v2",
                "alice@example.com",
                "bob@example.com",
                true,
                false,
                (short)31,
                (short)22,
                123.45m,
                67.89m,
                createdAt1,
                createdAt2,
                updatedAt1,
                updatedAt2,
                "{\"theme\":\"dark\"}",
                "{\"theme\":\"light\"}");
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters insert nullable values correctly in the users table for the current provider.
    /// PT-br: Verifica se parametros tipados do provedor inserem valores anulaveis corretamente na tabela de usuarios do provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParameterInsertNullRoundTripTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect);
        var createdAt = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        var result = await testService.RunTestAsync<InsertUsersScenario, DmlMutationParameterInsertNullRoundTripServiceTest>(
            "Alice-v2",
            null!,
            true,
            (short)31,
            123.45m,
            createdAt,
            null!,
            null!);
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters insert rows correctly inside a committed transaction for the current provider.
    /// PT-br: Verifica se parametros tipados do provedor inserem linhas corretamente dentro de uma transacao confirmada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParameterTransactionCommitTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect);
        var createdAt1 = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeParameterDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));

        var result = await testService.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionCommitServiceTest>(
            "Alice-v2",
            "Bob-v2",
            "alice@example.com",
            "bob@example.com",
            createdAt1,
            createdAt2);
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters roll back correctly inside a transaction for the current provider.
    /// PT-br: Verifica se parametros tipados do provedor fazem rollback corretamente dentro de uma transacao para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task ParameterTransactionRollbackTest()
    {
        using var testService = new FidelityTestService<T, T2>(
            connectionMock,
            connectionContainer,
            dialect);
        var createdAt1 = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var createdAt2 = NormalizeParameterDateTimeInput(new DateTime(2024, 2, 3, 4, 5, 6, DateTimeKind.Unspecified));

        var result = await testService.RunTestAsync<InsertUsersScenario, DmlMutationParameterTransactionRollbackServiceTest>(
            "Alice-v2",
            "Bob-v2",
            "alice@example.com",
            "bob@example.com",
            createdAt1,
            createdAt2);
        result.Should().BeEquivalentTo(new { count = 2, count2 = 1 });
    }

    /// <summary>
    /// EN: Normalizes DateTime input values used by parameter roundtrip tests for the current provider.
    /// PT-br: Normaliza valores de entrada DateTime usados pelos testes de roundtrip de parametros para o provedor atual.
    /// </summary>
    /// <param name="value">EN: The input DateTime value. PT-br: O valor DateTime de entrada.</param>
    /// <returns>EN: The normalized DateTime value. PT-br: O valor DateTime normalizado.</returns>
    protected virtual DateTime NormalizeParameterDateTimeInput(DateTime value)
        => value;
}


