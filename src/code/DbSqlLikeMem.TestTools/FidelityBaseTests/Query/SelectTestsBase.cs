using System.Runtime.CompilerServices;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Json;
using DbSqlLikeMem.TestTools.Query;
#if NET462 || NETSTANDARD2_0
using ITuple = DbSqlLikeMem.Compatibility.ITuple;
#endif

namespace DbSqlLikeMem.TestTools.Tests.Query;

/// <summary>
/// EN: Provides shared select fidelity tests across mock and container runs.
/// PT: Fornece testes compartilhados de fidelidade de select entre execucoes mock e container.
/// </summary>
public abstract class SelectTestsBase<T, T2>(
    ITestOutputHelper helper,
    ProviderSqlDialect dialect,
    Func<T> connectionMock,
    Func<string, T2> connectionContainer
    ) : XUnitTestBase(helper)
    where T : DbConnection
    where T2 : DbConnection
{

    /// <summary>
    /// EN: Verifies that primary-key selection returns the expected row for the current provider.
    /// PT: Verifica se a selecao por chave primaria retorna a linha esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectByPkTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<SelectTableScenario, SelectByPKServiceTest, QueryResultSnapshot>(
            (service, args) => service.RunSelectByPkAsync(args));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectByPkTest)), Snapshot(["Name"], Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies that selecting all rows returns the expected row count for the current provider.
    /// PT: Verifica se o select de todas as linhas retorna a contagem esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectAllRowsCountTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, int>(
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(dialect.InsertUser(s.Context, 2, "Bob"));
                return await s.RunRowCountAfterSelectAsync(a);
            });

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that selecting all rows returns the full rowset snapshot for the current provider.
    /// PT: Verifica se o select de todas as linhas retorna o snapshot completo do conjunto de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectAllRowsSnapshotTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(dialect.InsertUser(s.Context, 2, "Bob"));
                return await s.RunAllRowsSnapshotAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectAllRowsSnapshotTest)), Snapshot(["Name"], Row("Alice"), Row("Bob")));
    }

    /// <summary>
    /// EN: Verifies that a simple CTE query returns the expected rowset for the current provider.
    /// PT: Verifica se uma consulta CTE simples retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectCteSimpleTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunCteSimpleAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectCteSimpleTest)), Snapshot(["Name"], Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies that a MATERIALIZED CTE hint preserves the expected rowset for the current provider.
    /// PT: Verifica se um hint MATERIALIZED em CTE preserva o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectCteMaterializedHintTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsWithMaterializedHint)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
                (s, a) => s.RunCteMaterializedHintAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunCteMaterializedHintAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectCteMaterializedHintTest)), Snapshot(["Name"], Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies correlated scalar subqueries with CASE expressions for the current provider.
    /// PT: Verifica subconsultas escalares correlacionadas com expressoes CASE para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectScalarSubqueryCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectScalarSubqueryCaseMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectScalarSubqueryCaseMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCount", "HasNoOrders", "HasManyOrders"],
            Row(1m, "Alice", 3m, 0m, 1m),
            Row(2m, "Bob", 1m, 0m, 0m),
            Row(3m, "Carla", 0m, 1m, 0m)));
    }

    /// <summary>
    /// EN: Verifies the scalar CASE matrix over users and orders matches the current provider behavior.
    /// PT: Verifica se a matriz de CASE escalar nas tabelas de usuarios e pedidos coincide com o comportamento do provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectScalarCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectScalarCaseMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectScalarCaseMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCount", "HasNoOrders", "HasManyOrders"],
            Row(1m, "Alice", 3m, 0m, 1m),
            Row(2m, "Bob", 1m, 0m, 0m),
            Row(3m, "Carla", 0m, 1m, 0m)));
    }

    /// <summary>
    /// EN: Verifies that a NOT EXISTS predicate returns the expected anti-join rowset for the current provider.
    /// PT: Verifica se um predicado NOT EXISTS retorna o conjunto de linhas esperado de anti-join para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNotExistsPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectNotExistsPredicateAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectNotExistsPredicateTest)), Snapshot(["Id", "Name"], Row(3m, "Carla")));
    }

    /// <summary>
    /// EN: Verifies that a LEFT JOIN anti-join returns the expected rowset for the current provider.
    /// PT: Verifica se um anti-join com LEFT JOIN retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectLeftJoinAntiJoinTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectLeftJoinAntiJoinAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectLeftJoinAntiJoinTest)), Snapshot(["Id", "Name"], Row(3m, "Carla")));
    }

    /// <summary>
    /// EN: Verifies that a NOT IN subquery returns the expected anti-join rowset for the current provider.
    /// PT: Verifica se uma subconsulta NOT IN retorna o conjunto de linhas esperado de anti-join para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNotInSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectNotInSubqueryAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectNotInSubqueryTest)), Snapshot(["Id", "Name"], Row(3m, "Carla")));
    }

    /// <summary>
    /// EN: Verifies that NOT IN with a NULL value inside the subquery returns no rows for the current provider.
    /// PT: Verifica se NOT IN com um valor NULL dentro da subconsulta retorna nenhuma linha para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNotInSubqueryNullTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            async (QueryServiceTest s, object[] _) =>
            {
                using var command = s.Repo.Cnn.CreateCommand();
                command.CommandText = $"""
SELECT Id, Name
FROM {s.Context.TbUsersFullName}
WHERE Id NOT IN (
    SELECT 1
    FROM {s.Context.TbUsersFullName} u
    WHERE u.Id = 1
    UNION ALL
    SELECT NULL
    FROM {s.Context.TbUsersFullName} u
    WHERE u.Id = 1
)
ORDER BY Id
""";

                using var reader = await command.ExecuteReaderAsync();
                return QueryResultSnapshotReader.Capture(reader);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectNotInSubqueryNullTest)), Snapshot(["Id", "Name"]));
    }

    /// <summary>
    /// EN: Verifies that an EXISTS predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado EXISTS retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectExistsPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectExistsPredicateAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectExistsPredicateTest)), Snapshot(["Id", "Name"], Row(1m, "Alice"), Row(2m, "Bob")));
    }

    /// <summary>
    /// EN: Verifies that a correlated count predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado de contagem correlacionada retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectCorrelatedCountTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectCorrelatedCountAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectCorrelatedCountTest)), Snapshot(["UserId", "UserName", "OrderCount"], Row(1m, "Alice", 2m), Row(2m, "Bob", 1m)));
    }

    /// <summary>
    /// EN: Verifies that an IN subquery returns the expected rowset for the current provider.
    /// PT: Verifica se uma subconsulta IN retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectInSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunSelectInSubqueryAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectInSubqueryTest)), Snapshot(["Id", "Name"], Row(1m, "Alice"), Row(2m, "Bob")));
    }

    /// <summary>
    /// EN: Verifies that a scalar subquery returns the expected result for the current provider.
    /// PT: Verifica se uma subconsulta escalar retorna o resultado esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectScalarSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, long>(
            (s, a) => s.RunSelectScalarSubqueryAsync(a));
        result.Should().Be(2L);
    }

    /// <summary>
    /// EN: Verifies that a GROUP BY HAVING query returns the expected rowset for the current provider.
    /// PT: Verifica se uma consulta GROUP BY HAVING retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGroupByHavingTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunGroupByHavingAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectGroupByHavingTest)), Snapshot(["Id"], Row(1m)));
    }

    /// <summary>
    /// EN: Verifies that a UNION ALL projection returns the expected rowset for the current provider.
    /// PT: Verifica se uma projecao UNION ALL retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectUnionAllProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Bob")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunUnionAllProjectionAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectUnionAllProjectionTest)), Snapshot(["Name"], Row("Alice"), Row("Bob")));
    }

    /// <summary>
    /// EN: Verifies that a DISTINCT projection returns the expected rowset for the current provider.
    /// PT: Verifica se uma projecao DISTINCT retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Alice"), (3, "Bob")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunDistinctProjectionAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctProjectionTest)), Snapshot(["Name"], Row("Alice"), Row("Bob")));
    }

    /// <summary>
    /// EN: Verifies that a DISTINCT ON projection returns the first row per key selected by ORDER BY for the current provider.
    /// PT: Verifica se uma projecao DISTINCT ON retorna a primeira linha por chave escolhida pelo ORDER BY para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctOnProjectionTest()
    {
        if (!dialect.SupportsDistinctOnProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                (s, a) => s.RunDistinctOnProjectionAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunDistinctOnProjectionAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctOnProjectionTest)), Snapshot(ApplyProjectionColumnNames(),
            Row(1m, "Alice", "B"),
            Row(2m, "Bob", "C"),
            Row(3m, "Carla", null)));
    }

    /// <summary>
    /// EN: Verifies that a multi-join aggregate returns the expected rowset for the current provider.
    /// PT: Verifica se uma agregacao com multiplos joins retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectMultiJoinAggregateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunMultiJoinAggregateAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectMultiJoinAggregateTest)), Snapshot(["UserId", "FirstOrderId", "SecondOrderId"],
            Row(1m, 10m, 10m),
            Row(1m, 10m, 11m),
            Row(1m, 11m, 10m),
            Row(1m, 11m, 11m)));
    }

    /// <summary>
    /// EN: Verifies an IN-list predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado IN com lista retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectInListPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Bob"), (3, "Charlie")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunInListPredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectInListPredicateTest)), Snapshot(["Id", "Name"],
            Row(1m, "Alice"),
            Row(3m, "Charlie")));
    }

    /// <summary>
    /// EN: Verifies a BETWEEN predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado BETWEEN retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectBetweenPredicateTest()
    {

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunBetweenPredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectBetweenPredicateTest)), Snapshot(["Id", "Name"],
            Row(2m, "Bob"),
            Row(3m, "Charlie"),
            Row(4m, "Delta")));
    }

    /// <summary>
    /// EN: Verifies a LIKE predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado LIKE retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectLikePredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunLikePredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectLikePredicateTest)), Snapshot(["Id", "Name"],
            Row(1m, "Alice")));
    }

    /// <summary>
    /// EN: Verifies a combined BETWEEN, LIKE, and ORDER BY query returns the expected row order for the current provider.
    /// PT: Verifica se uma consulta combinada com BETWEEN, LIKE e ORDER BY retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectBetweenLikeOrderByTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData2]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunBetweenLikeOrderByMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectBetweenLikeOrderByTest)), Snapshot(["Name"],
            Row("Aaron"),
            Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies the combined BETWEEN, LIKE, and ORDER BY matrix against the current provider behavior.
    /// PT: Verifica a matriz combinada de BETWEEN, LIKE e ORDER BY contra o comportamento do provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectBetweenLikeOrderByMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData2]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunBetweenLikeOrderByMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectBetweenLikeOrderByMatrixTest)), Snapshot(["Name"],
            Row("Aaron"),
            Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies a NOT LIKE predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado NOT LIKE retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNotLikePredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunNotLikePredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectNotLikePredicateTest)), Snapshot(["Id", "Name"],
            Row(2m, "Bob"),
            Row(3m, "Charlie"),
            Row(4m, "Delta"),
            Row(5m, "Echo")));
    }

    /// <summary>
    /// EN: Verifies a not-equal predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado diferente de retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNotEqualPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunNotEqualPredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectNotEqualPredicateTest)), Snapshot(["Id", "Name"],
            Row(1m, "Alice"),
            Row(3m, "Charlie"),
            Row(4m, "Delta"),
            Row(5m, "Echo")));

    }

    /// <summary>
    /// EN: Verifies an equality predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado de igualdade retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectEqualPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunEqualPredicateMatrixAsync(a));
        AssertSnapshot(RequireSnapshot(result, nameof(SelectEqualPredicateTest)), Snapshot(["Id", "Name"],
            Row(2m, "Bob")));
    }

    /// <summary>
    /// EN: Verifies a parameterized name lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por nome retorna a linha esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectParameterByNameTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, string?>(
            (s, a) => s.RunParameterSelectByNameMatrixAsync(a),
            "Bob");
        result.Should().Be("Bob");
    }

    /// <summary>
    /// EN: Verifies a parameterized id lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por id retorna a linha esperada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectParameterByIdTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, string?>(
            (s, a) => s.RunParameterSelectByIdMatrixAsync(a),
            3,
            "Charlie");
        result.Should().Be("Charlie");
    }

    /// <summary>
    /// EN: Verifies parameter roundtrips over typed user columns for string, numeric, boolean, date, and null values.
    /// PT: Verifica roundtrips de parametros sobre colunas tipadas de usuario para valores de texto, numericos, booleanos, data e nulos.
    /// </summary>
    [FidelityFact]
    public async Task SelectParameterRoundTripMatrixTest()
    {
        var createdAt = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            (s, a) => s.RunParameterRoundTripMatrixAsync(a),
            1,
                "Param Alice",
                DBNull.Value,
                true,
                (short)31,
                12.34m,
                createdAt,
                DBNull.Value,
                DBNull.Value);
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters roundtrip correctly for ANSI text, fixed-length text, numeric, boolean, temporal, binary, GUID, and null values.
    /// PT: Verifica se parametros tipados do provedor fazem roundtrip corretamente para texto ANSI, texto de comprimento fixo, numericos, booleanos, temporais, binario, GUID e nulos.
    /// </summary>
    [FidelityFact]
    public async Task SelectParameterTypeMatrixTest()
    {
        var createdAt = NormalizeParameterDateTimeInput(new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified));
        var ansiFixedText = "Fixed ANSI";
        var fixedText = "Fixed Text";

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            (s, a) => s.RunParameterTypeMatrixAsync(a),
                "Typed param",
                "Ansi param",
                ansiFixedText,
                fixedText,
                (short)12,
                34,
                56L,
                true,
                78.90m,
                12.5d,
                TimeSpan.FromHours(1.5),
                new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero),
                createdAt,
                Guid.Parse("11111111-2222-3333-4444-555555555555"),
                new byte[] { 1, 2, 3, 4 });
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies typed provider parameters roundtrip correctly for date and currency values.
    /// PT: Verifica se parametros tipados do provedor fazem roundtrip corretamente para valores de data e moeda.
    /// </summary>
    [FidelityFact]
    public async Task SelectParameterDateCurrencyMatrixTest()
    {
        var dateValue = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var currencyValue = 123.45m;

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest, int>(
            (s, a) => s.RunParameterDateCurrencyMatrixAsync(a),
            dateValue,
            currencyValue);

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies the broad parameter projection benchmark returns the expected scalar value, including GUID binding.
    /// PT: Verifica se o benchmark amplo de projeção de parametros retorna o valor escalar esperado, incluindo o bind de GUID.
    /// </summary>
    [FidelityFact]
    public async Task ParameterProjectionTest()
    {
        var result = await RunFidelityTestAsync<InsertUsersScenario, QueryServiceTest, string>(
            [],
            (s, a) => Task.FromResult<string>(s.RunParameterProjection() ?? string.Empty));

        result.Should().Be("benchmark");
    }

    /// <summary>
    /// EN: Verifies a greater-than predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado maior que retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGreaterThanPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunGreaterThanPredicateMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectGreaterThanPredicateTest)), Snapshot(["Id", "Name"],
            Row(4m, "Delta"),
            Row(5m, "Echo")));
    }

    /// <summary>
    /// EN: Verifies a less-than predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado menor que retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectLessThanPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunLessThanPredicateMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectLessThanPredicateTest)), Snapshot(["Id", "Name"],
            Row(1m, "Alice"),
            Row(2m, "Bob")));
    }

    /// <summary>
    /// EN: Verifies a greater-than-or-equal predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado maior ou igual retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGreaterThanOrEqualPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunGreaterThanOrEqualPredicateMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectGreaterThanOrEqualPredicateTest)), Snapshot(["Id", "Name"],
            Row(3m, "Charlie"),
            Row(4m, "Delta"),
            Row(5m, "Echo")));
    }

    /// <summary>
    /// EN: Verifies a less-than-or-equal predicate returns the expected rowset for the current provider.
    /// PT: Verifica se um predicado menor ou igual retorna o conjunto de linhas esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectLessThanOrEqualPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunLessThanOrEqualPredicateMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectLessThanOrEqualPredicateTest)), Snapshot(["Id", "Name"],
            Row(1m, "Alice"),
            Row(2m, "Bob"),
            Row(3m, "Charlie")));
    }

    /// <summary>
    /// EN: Verifies ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByNameTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByNameTest)), Snapshot(["Name"],
            Row("Alice"),
            Row("Bob"),
            Row("Charlie")));
    }

    /// <summary>
    /// EN: Verifies ordering by Name matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByNameMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByNameMatrixTest)), Snapshot(["Name"],
            Row("Alice"),
            Row("Bob"),
            Row("Charlie")));
    }

    /// <summary>
    /// EN: Verifies that a UNION projection returns the expected distinct rowset for the current provider.
    /// PT: Verifica se uma projecao UNION retorna o conjunto de linhas distinto esperado para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectUnionDistinctProjectionTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Carla"));
                return await s.RunUnionDistinctProjectionAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectUnionDistinctProjectionTest)), Snapshot(["Name"], Row("Alice"), Row("Bob"), Row("Carla")));
    }

    /// <summary>
    /// EN: Verifies grouped names by initial with distinct counts and HAVING filtering for the current provider.
    /// PT: Verifica nomes agrupados por inicial com contagens distintas e filtro HAVING para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGroupByNameInitialMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]],
            (s, a) => s.RunGroupByNameInitialMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectGroupByNameInitialMatrixTest)), Snapshot(["NameInitial", "TotalCount", "DistinctCount", "AliceCount", "BobCount", "FirstName", "LastName", "HasAtLeastTwo"],
            Row("A", 3m, 2m, 2m, 0m, "Adam", "Alice", 1m),
            Row("B", 3m, 2m, 0m, 2m, "Bob", "Brian", 1m),
            Row("C", 2m, 2m, 0m, 0m, "Carla", "Chris", 1m)));
    }

    /// <summary>
    /// EN: Verifies GROUP BY Name with HAVING filtering over the configured users table for the current provider.
    /// PT: Verifica GROUP BY Name com filtro HAVING na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGroupByNameHavingTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie")]],
            (s, a) => s.RunGroupByNameHavingMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectGroupByNameHavingTest)), Snapshot(["Name", "TotalCount"],
            Row("Alice", 2m),
            Row("Bob", 3m)));
    }

    /// <summary>
    /// EN: Verifies GROUP BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de GROUP BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectGroupByOrdinalTest()
    {
        object?[][] initialData = [[(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]];

        if (!dialect.SupportsGroupByOrdinal)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
                initialData,
                (s, a) => s.RunGroupByOrdinalMatrixAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            initialData,
            (s, a) => s.RunGroupByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectGroupByOrdinalTest)), Snapshot(["NameInitial", "TotalCount"],
            Row("A", 3m),
            Row("B", 3m),
            Row("C", 2m)));
    }

    /// <summary>
    /// EN: Verifies ORDER BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de ORDER BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]],
            (s, a) => s.RunOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByOrdinalTest)), Snapshot(["Name", "Id"],
            Row("Charlie", 3m),
            Row("Bravo", 2m),
            Row("Alpha", 1m)));
    }

    /// <summary>
    /// EN: Verifies ordering by ordinal matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao por ordinal retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]],
            (s, a) => s.RunOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByOrdinalMatrixTest)), Snapshot(["Name", "Id"],
            Row("Charlie", 3m),
            Row("Bravo", 2m),
            Row("Alpha", 1m)));
    }

    /// <summary>
    /// EN: Verifies DISTINCT ordering by ordinal over the configured users table for the current provider.
    /// PT: Verifica a ordenacao DISTINCT por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctOrderByOrdinalTest)), Snapshot(["Name"],
            Row("Alice"),
            Row("Bob"),
            Row("Charlie"),
            Row("Delta")));
    }

    /// <summary>
    /// EN: Verifies DISTINCT ordering by ordinal matrix over the configured users table for the current provider.
    /// PT: Verifica a matriz de ordenacao DISTINCT por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctOrderByOrdinalMatrixTest)), Snapshot(["Name"],
            Row("Alice"),
            Row("Bob"),
            Row("Charlie"),
            Row("Delta")));
    }

    /// <summary>
    /// EN: Verifies DISTINCT with a text filter ordered by ordinal over the configured users table for the current provider.
    /// PT: Verifica DISTINCT com filtro de texto ordenado por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctLikeOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctLikeOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctLikeOrderByOrdinalTest)), Snapshot(["UPPER(Name)"],
            Row("ALICE"),
            Row("CHARLIE"),
            Row("DELTA")));
    }

    /// <summary>
    /// EN: Verifies DISTINCT with a text filter ordered by ordinal matrix over the configured users table for the current provider.
    /// PT: Verifica a matriz DISTINCT com filtro de texto ordenado por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectDistinctLikeOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctLikeOrderByOrdinalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectDistinctLikeOrderByOrdinalMatrixTest)), Snapshot(["UPPER(Name)"],
            Row("ALICE"),
            Row("CHARLIE"),
            Row("DELTA")));
    }

    /// <summary>
    /// EN: Verifies descending ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao descendente por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByNameDescendingTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameDescendingMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByNameDescendingTest)), Snapshot(["Name"],
            Row("Charlie"),
            Row("Bob"),
            Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies descending ordering by Name matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao descendente por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOrderByNameDescendingMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameDescendingMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOrderByNameDescendingMatrixTest)), Snapshot(["Name"],
            Row("Charlie"),
            Row("Bob"),
            Row("Alice")));
    }

    /// <summary>
    /// EN: Verifies a paged ordered query built with ROW_NUMBER for the current provider.
    /// PT: Verifica uma consulta ordenada e paginada construida com ROW_NUMBER para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectNamePaginationMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            [[(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]],
            (s, a) => s.RunNamePaginationMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectNamePaginationMatrixTest)), Snapshot(["Name"],
            Row("Bravo"),
            Row("Charlie"),
            Row("Delta")));
    }

    /// <summary>
    /// EN: Verifies native pagination syntax over an ordered users table for the current provider.
    /// PT: Verifica sintaxe nativa de paginação sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectPagedNameProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest, QueryResultSnapshot>(
            (s, a) => s.RunPagedNameProjectionMatrixAsync());
        AssertSnapshot(RequireSnapshot(result, nameof(SelectPagedNameProjectionTest)), Snapshot(["Name"],
            Row("Bravo"),
            Row("Charlie")));
    }

    /// <summary>
    /// EN: Verifies a relational query bundle across users and orders tables for the current provider.
    /// PT: Verifica um conjunto de consultas relacionais nas tabelas de usuarios e pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectRelationalCompositeTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers2, SelectTestsBaseSeeds.seedOrders2]);

        _ = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest, (
            QueryResultSnapshot cte,
            QueryResultSnapshot existsPredicate,
            QueryResultSnapshot correlatedCount,
            QueryResultSnapshot groupByHaving,
            QueryResultSnapshot unionAll,
            QueryResultSnapshot distinct,
            QueryResultSnapshot multiJoin,
            long scalarSubquery,
            QueryResultSnapshot inSubquery,
            int pivot)>(
            (s, a) => RunRelationalCompositeAssertionsAsync(s));
    }

    /// <summary>
    /// EN: Verifies that common window functions return the expected rowsets for the current provider.
    /// PT: Verifica se funcoes de janela comuns retornam os conjuntos de linhas esperados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowFunctionsTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, (QueryResultSnapshot rowNumber, QueryResultSnapshot lag, QueryResultSnapshot lead)>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Charlie"));

                var rowNumber = (QueryResultSnapshot)(await s.RunWindowRowNumberAsync(a)
                    ?? throw new InvalidOperationException("RunWindowRowNumberAsync did not return a query snapshot."));
                var lag = (QueryResultSnapshot)(await s.RunWindowLagAsync(a)
                    ?? throw new InvalidOperationException("RunWindowLagAsync did not return a query snapshot."));
                var lead = (QueryResultSnapshot)(await s.RunWindowLeadAsync(a)
                    ?? throw new InvalidOperationException("RunWindowLeadAsync did not return a query snapshot."));
                return (rowNumber, lag, lead);
            });

        var (rowNumber, lag, lead) = RequireSnapshotTriple(result, nameof(SelectWindowFunctionsTest));
        AssertSnapshot(rowNumber, Snapshot(["Name", "RowNumberValue"],
            Row("Alice", 1m),
            Row("Bob", 2m),
            Row("Charlie", 3m)));
        AssertSnapshot(lag, Snapshot(["Name", "PrevName"],
            Row("Alice", null),
            Row("Bob", "Alice"),
            Row("Charlie", "Bob")));
        AssertSnapshot(lead, Snapshot(["Name", "NextName"],
            Row("Alice", "Bob"),
            Row("Bob", "Charlie"),
            Row("Charlie", null)));
    }

    /// <summary>
    /// EN: Verifies ranking window functions with duplicate names for the current provider.
    /// PT: Verifica funcoes de janela de ranking com nomes duplicados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowRankDenseRankTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowRankDenseRank(a.Length > 0 ? a : ["Alice"]);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectWindowRankDenseRankTest)), Snapshot(["Name", "RankValue", "DenseRankValue", "RowNumberValue"],
            Row("Alice", 1m, 1m, 1m),
            Row("Bravo", 2m, 2m, 2m),
            Row("Bravo", 2m, 2m, 3m),
            Row("Charlie", 4m, 3m, 4m)));
    }

    /// <summary>
    /// EN: Verifies FIRST_VALUE and LAST_VALUE window projections for the current provider.
    /// PT: Verifica projeções FIRST_VALUE e LAST_VALUE de janela para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowFirstLastValueTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowFirstLastValue(a.Length > 0 ? a : ["Alice"]);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectWindowFirstLastValueTest)), Snapshot(["Name", "FirstName", "LastName"],
            Row("Alice", "Alice", "Charlie"),
            Row("Bravo", "Alice", "Charlie"),
            Row("Bravo", "Alice", "Charlie"),
            Row("Charlie", "Alice", "Charlie")));
    }


    /// <summary>
    /// EN: Verifies NTILE distribution over an ordered users table for the current provider.
    /// PT: Verifica a distribuicao NTILE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowNtileTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowNtile(a.Length > 0 ? a : ["Alice"]);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectWindowNtileTest)), Snapshot(["Name", "BucketValue"],
            Row("Alice", 1m),
            Row("Bravo", 1m),
            Row("Bravo", 2m),
            Row("Charlie", 2m)));
    }

    /// <summary>
    /// EN: Verifies PERCENT_RANK and CUME_DIST window projections for the current provider.
    /// PT: Verifica projeções de janela PERCENT_RANK e CUME_DIST para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowPercentRankCumeDistTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowPercentRankCumeDist(a.Length > 0 ? a : ["Alice"]);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectWindowPercentRankCumeDistTest)), Snapshot(["Name", "PercentRankValue", "CumeDistValue"],
            Row("Alice", 0.0m, 0.25m),
            Row("Bravo", 0.333333m, 0.5m),
            Row("Bravo", 0.666667m, 0.75m),
            Row("Charlie", 1.0m, 1.0m)));
    }

    /// <summary>
    /// EN: Verifies NTH_VALUE over an ordered users table for the current provider.
    /// PT: Verifica NTH_VALUE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectWindowNthValueTest()
    {
        if (!dialect.SupportsNthValueWindowFunction)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
                [],
                async (s, a) =>
                {
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                    return await s.RunWindowNthValue(a.Length > 0 ? a : ["Alice"]);
                }, "Alice")).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowNthValue(a.Length > 0 ? a : ["Alice"]);
            },
            "Alice");

        AssertSnapshot(RequireSnapshot(result, nameof(SelectWindowNthValueTest)), Snapshot(["Name", "SecondName"],
            Row("Alice", "Bravo"),
            Row("Bravo", "Bravo"),
            Row("Bravo", "Bravo"),
            Row("Charlie", "Bravo")));
    }

    /// <summary>
    /// EN: Verifies range filtering and pivot-style aggregation for the current provider.
    /// PT: Verifica filtragem por faixa e agregacao no estilo pivot para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectRangeAndPivotTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, (int partitionCount, int pivotCount)>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                for (var id = 3; id <= 12; id++)
                {
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, id, $"User-{id}"));
                }

                var partitionCount = await s.RunPartitionPruningSelectAsync(a);
                var pivotCount = await s.RunPivotCountAsync(a);
                return (partitionCount, pivotCount);
            });

        result.Should().BeEquivalentTo((6, 2));
    }

    /// <summary>
    /// EN: Verifies that a partition-pruning style query returns the expected row count for the current provider.
    /// PT: Verifica se uma consulta no estilo partition pruning retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectPartitionPruningTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, int>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                for (var id = 3; id <= 12; id++)
                {
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, id, $"User-{id}"));
                }

                return await s.RunPartitionPruningSelectAsync(a);
            });

        result.Should().Be(6);
    }

    /// <summary>
    /// EN: Verifies that a pivot-style count query returns the expected row count for the current provider.
    /// PT: Verifica se uma consulta de contagem no estilo pivot retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectPivotCountTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest, int>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                for (var id = 3; id <= 12; id++)
                {
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, id, $"User-{id}"));
                }

                return await s.RunPivotCountAsync(a);
            });

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that combines typed columns, aggregates, and calculations for the current provider.
    /// PT: Verifica uma projecao com junção agrupada que combina colunas tipadas, agregacoes e calculos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinTypedExpressionMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers2],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinTypedExpressionMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinTypedExpressionMatrixTest)), RawSnapshot(["UserId", "UserNameUpper", "UserNameLower", "OrderCount", "TotalQuantity", "TotalAmount", "AvgAmount", "FirstNote", "LastNote", "HasMultipleOrders", "AmountAtLeastThree"],
            Row(1m, "ALICE", "alice", 2m, 3m, 4.00m, 2.00m, "A", "B", 1m, 1m),
            Row(2m, "BOB", "bob", 1m, 4m, 5.50m, 5.50m, "C", "C", 0m, 1m)));
    }

    /// <summary>
    /// EN: Verifies a left join aggregate projection that preserves users without orders for the current provider.
    /// PT: Verifica uma projeção agregada com left join que preserva usuarios sem pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinNullAggregateMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinNullAggregateMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinNullAggregateMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCount", "TotalQuantity", "TotalAmount", "FirstNote", "HasNoOrders", "AmountIsNull", "HasLargeQuantity"],
            Row(1m, "Alice", 2m, 3m, 4.00m, "A", 0m, 0m, 1m),
            Row(2m, "Bob", 1m, 4m, 5.50m, "C", 0m, 0m, 1m),
            Row(3m, "Carla", 0m, 0m, 0.00m, "none", 1m, 1m, 0m)));
    }

    /// <summary>
    /// EN: Verifies a grouped left join projection that blends casts, null handling, and aggregate formatting for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que mistura casts, tratamento de null e formatacao de agregados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinCastNullMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinCastNullMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinCastNullMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "HasNoOrders", "NotesAreNull", "HasNote", "MeetsAmountThreshold"],
            Row(1m, "Alice", "2", "3", AmountText(4.00m), 0m, 0m, 1m, 1m),
            Row(2m, "Bob", "1", "4", AmountText(5.50m), 0m, 0m, 1m, 1m),
            Row(3m, "Carla", "0", "0", AmountText(0.00m), 1m, 1m, 0m, 0m)));
    }

    /// <summary>
    /// EN: Verifies a grouped left join projection that casts aggregate values to text and compares them for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que converte agregados para texto e os compara para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinCastTextComparisonMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinCastTextComparisonMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinCastTextComparisonMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "CountTextIsZero", "QuantityTextNonZero", "NotesAreMissing", "HasAnyNote"],
            Row(1m, "Alice", "2", "3", AmountText(4.00m), 0m, 1m, 0m, 1m),
            Row(2m, "Bob", "1", "4", AmountText(5.50m), 0m, 1m, 0m, 1m),
            Row(3m, "Carla", "0", "0", AmountText(0.00m), 1m, 0m, 1m, 0m)));
    }

    /// <summary>
    /// EN: Verifies a grouped join query with HAVING filters and casted aggregate outputs for the current provider.
    /// PT: Verifica uma consulta agrupada com filtros HAVING e saidas agregadas convertidas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinHavingCastMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinHavingCastMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinHavingCastMatrixTest)), RawSnapshot(["UserId", "UserName", "OrderCountText", "TotalQuantityText", "TotalAmountText", "HasTwoOrMoreOrders", "AmountAtLeastFour", "StartsAtA"],
            Row(1m, "Alice", "2", "3", AmountText(4.00m), 1m, 1m, 1m)));
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that mixes string-length expressions with numeric conversions and aggregates for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura expressoes de comprimento de texto com conversoes numericas e agregados para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinLengthNumericMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinLengthNumericMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinLengthNumericMatrixTest)), RawSnapshot(["UserId", "UserName", "NameLenText", "TotalQuantityText", "TotalAmountText", "MaxNoteLenText", "NameLenGe4", "QuantityGe3", "AmountGe4", "HasNotes"],
            Row(1m, "Alice", "5", "3", AmountText(4.00m), "1", 1m, 1m, 1m, 1m),
            Row(2m, "Bob", "3", "4", AmountText(5.50m), "1", 0m, 1m, 1m, 1m),
            Row(3m, "Carla", "5", "0", AmountText(0.00m), "0", 1m, 0m, 0m, 0m)));
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that blends string case, string length, and aggregate comparisons for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura caixa de texto, comprimento de texto e comparacoes agregadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinTextCaseLengthMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinTextCaseLengthMatrixAsync(a);
            });

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinTextCaseLengthMatrixTest)), RawSnapshot(["UserId", "NameUpper", "NameLower", "NameTrimmed", "NameLenText", "MaxNoteLenText", "NameLenGe4", "IsUpperAlready", "IsLowerAlready", "TwoOrMoreOrders", "QuantityGe3"],
            Row(1m, "ALICE", "alice", "Alice", "5", "1", 1m, TextMatchAlreadyValue, TextMatchAlreadyValue, 1m, 1m),
            Row(2m, "BOB", "bob", "Bob", "3", "1", 0m, TextMatchAlreadyValue, TextMatchAlreadyValue, 0m, 1m),
            Row(3m, "CARLA", "carla", "Carla", "5", "0", 1m, TextMatchAlreadyValue, TextMatchAlreadyValue, 0m, 0m)));
    }

    /// <summary>
    /// EN: Verifies grouped left-join reporting with distinct counts, repeated values, and CASE expressions for the current provider.
    /// PT: Verifica um relatorio agrupado com left join, contagens distintas, valores repetidos e expressoes CASE para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinDistinctCaseMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders],
            (s, a) => s.RunJoinDistinctCaseMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinDistinctCaseMatrixTest)), RawSnapshot(["UserId", "OrderCount", "DistinctNoteCount", "NoteACount", "HasMultipleDistinctNotes", "HasRepeatedNoteA"],
            Row(1m, 3m, 2m, 2m, 1m, 1m),
            Row(2m, 1m, 1m, 0m, 0m, 0m),
            Row(3m, 0m, 0m, 0m, 0m, 0m)));
    }

    /// <summary>
    /// EN: Verifies grouped left-join filtering with HAVING and distinct note counts for the current provider.
    /// PT: Verifica filtragem agrupada com left join, HAVING e contagens distintas de notas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinDistinctHavingMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders],
            (s, a) => s.RunJoinDistinctHavingMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinDistinctHavingMatrixTest)), RawSnapshot(["UserId", "OrderCount", "DistinctNoteCount", "NoteACount", "HasMultipleDistinctNotes", "HasRepeatedNoteA"],
            Row(1m, 3m, 2m, 2m, 1m, 1m),
            Row(3m, 0m, 0m, 0m, 0m, 0m)));
    }

    /// <summary>
    /// EN: Verifies that a user and order join returns the expected row count for the current provider.
    /// PT: Verifica se a junção entre usuarios e pedidos retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinCountTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, DmlMutationSelectJoinServiceTest, int>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunSelectJoinCountAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies the basic users and orders join against the current provider behavior.
    /// PT: Verifica o join basico entre usuarios e pedidos contra o comportamento do provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, DmlMutationSelectJoinServiceTest, int>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunSelectJoinCountAsync(a));

        result.Should().Be(2);
    }

    private async Task<(
        QueryResultSnapshot cte,
        QueryResultSnapshot existsPredicate,
        QueryResultSnapshot correlatedCount,
        QueryResultSnapshot groupByHaving,
        QueryResultSnapshot unionAll,
        QueryResultSnapshot distinct,
        QueryResultSnapshot multiJoin,
        long scalarSubquery,
        QueryResultSnapshot inSubquery,
        int pivot)> RunRelationalCompositeAssertionsAsync(
        QueryServiceTest serviceTest)
    {
        var cte = RequireSnapshot(await serviceTest.RunCteSimpleAsync(), nameof(serviceTest.RunCteSimpleAsync));
        var existsPredicate = RequireSnapshot(await serviceTest.RunSelectExistsPredicateAsync(), nameof(serviceTest.RunSelectExistsPredicateAsync));
        var correlatedCount = RequireSnapshot(await serviceTest.RunSelectCorrelatedCountAsync(), nameof(serviceTest.RunSelectCorrelatedCountAsync));
        var groupByHaving = RequireSnapshot(await serviceTest.RunGroupByHavingAsync(), nameof(serviceTest.RunGroupByHavingAsync));
        var unionAll = RequireSnapshot(await serviceTest.RunUnionAllProjectionAsync(), nameof(serviceTest.RunUnionAllProjectionAsync));
        var distinct = RequireSnapshot(await serviceTest.RunDistinctProjectionAsync(), nameof(serviceTest.RunDistinctProjectionAsync));
        var multiJoin = RequireSnapshot(await serviceTest.RunMultiJoinAggregateAsync(), nameof(serviceTest.RunMultiJoinAggregateAsync));
        var scalarSubquery = await serviceTest.RunSelectScalarSubqueryAsync();
        var inSubquery = RequireSnapshot(await serviceTest.RunSelectInSubqueryAsync(), nameof(serviceTest.RunSelectInSubqueryAsync));
        var pivot = await serviceTest.RunPivotCountAsync();
        AssertSnapshot(cte, Snapshot(["Name"], Row("Alice")));
        AssertSnapshot(existsPredicate, Snapshot(["Id", "Name"], Row(1m, "Alice"), Row(2m, "Bob")));
        AssertSnapshot(correlatedCount, Snapshot(["UserId", "UserName", "OrderCount"], Row(1m, "Alice", 2m), Row(2m, "Bob", 1m)));
        AssertSnapshot(groupByHaving, Snapshot(["Id"], Row(1m)));
        AssertSnapshot(unionAll, Snapshot(["Name"], Row("Alice"), Row("Bob")));
        AssertSnapshot(distinct, Snapshot(["Name"], Row("Alice"), Row("Bob")));
        AssertSnapshot(multiJoin, Snapshot(["UserId", "FirstOrderId", "SecondOrderId"],
            Row(1m, 10m, 10m),
            Row(1m, 10m, 11m),
            Row(1m, 11m, 10m),
            Row(1m, 11m, 11m)));
        scalarSubquery.Should().Be(2L);
        AssertSnapshot(inSubquery, Snapshot(["Id", "Name"], Row(1m, "Alice"), Row(2m, "Bob")));
        pivot.Should().Be(2);

        return (cte, existsPredicate, correlatedCount, groupByHaving, unionAll, distinct, multiJoin, scalarSubquery, inSubquery, pivot);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that blends temporal comparisons with aggregate counts for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura comparacoes temporais com contagens agregadas para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinTemporalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinTemporalMatrixTest)), RawSnapshot(["UserId", "OrderCount", "HasNoOrders", "MinOrderedBeforeNow", "MaxOrderedBeforeNextDay", "PendingDeliveries", "UserCreatedBeforeNow"],
            Row(1m, 2m, 0m, 1m, 1m, 2m, 1m),
            Row(2m, 1m, 0m, 1m, 1m, 1m, 1m),
            Row(3m, 0m, 1m, 1m, 1m, 1m, 1m)));
    }

    /// <summary>
    /// EN: Verifies a joined window-function projection over the shared users and orders scenario for the current provider.
    /// PT: Verifica uma projeção com funcoes de janela em join no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinWindowMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinWindowMatrixTest)), RawSnapshot(["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote"],
            Row(1m, 10m, 1m, 2m, null),
            Row(1m, 11m, 2m, 2m, "A"),
            Row(2m, 12m, 1m, 1m, null)));
    }

    /// <summary>
    /// EN: Verifies joined window projections together with temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinWindowTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowTemporalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinWindowTemporalMatrixTest)), RawSnapshot(["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote", "OrderedBeforeNow", "NextDayAfterOrder", "UserCreatedBeforeNow"],
            Row(1m, 10m, 1m, 2m, null, 1m, 1m, 1m),
            Row(1m, 11m, 2m, 2m, "A", 1m, 1m, 1m),
            Row(2m, 12m, 1m, 1m, null, 1m, 1m, 1m)));
    }

    /// <summary>
    /// EN: Verifies joined window projections together with aggregate and temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes agregadas e temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [FidelityFact]
    public async Task SelectJoinWindowAggregateTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowAggregateTemporalMatrixAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectJoinWindowAggregateTemporalMatrixTest)), RawSnapshot(["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "QuantityPerUser", "AmountPerUser", "PreviousNote", "OrderedBeforeNow", "NextDayAfterOrder", "UserCreatedBeforeNow"],
            Row(1m, 10m, 1m, 2m, 2m, 0m, null, 1m, 1m, 1m),
            Row(1m, 11m, 2m, 2m, 2m, 0m, "A", 1m, 1m, 1m),
            Row(2m, 12m, 1m, 1m, 1m, 0m, null, 1m, 1m, 1m)));
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY and OUTER APPLY projections against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeções CROSS APPLY e OUTER APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectApplyProjectionTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply)>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = await s.RunCrossApplyProjectionAsync(a);
                    var outerApply = await s.RunOuterApplyProjectionAsync(a);
                    return (crossApply, outerApply);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply)>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = (QueryResultSnapshot?)await s.RunCrossApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyProjectionTest cross-apply result was null.");
                var outerApply = (QueryResultSnapshot?)await s.RunOuterApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyProjectionTest outer-apply result was null.");
                return (crossApply, outerApply);
            });

        var (crossApplyResult, outerApplyResult) = RequireSnapshotPair(result, nameof(SelectApplyProjectionTest));
        AssertSnapshot(crossApplyResult, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C")));
        AssertSnapshot(outerApplyResult, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C"), Row(3m, "Carla", null)));
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY projection against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeção CROSS APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectCrossApplyProjectionTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunCrossApplyProjectionAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectCrossApplyProjectionTest)), Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C")));
    }

    /// <summary>
    /// EN: Verifies OUTER APPLY projection against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeção OUTER APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectOuterApplyProjectionTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                (s, a) => s.RunOuterApplyProjectionAsync(a))).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, QueryResultSnapshot>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunOuterApplyProjectionAsync(a));

        AssertSnapshot(RequireSnapshot(result, nameof(SelectOuterApplyProjectionTest)), Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C"), Row(3m, "Carla", null)));
    }

    /// <summary>
    /// EN: Verifies STRING_SPLIT materializes the expected split rows for the shared users scenario.
    /// PT: Verifica se STRING_SPLIT materializa as linhas divididas esperadas no cenario compartilhado de usuarios.
    /// </summary>
    [FidelityFact]
    public async Task SelectStringSplitFunctionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsApplyClause || !dialect.SupportsStringSplitFunction)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
                async (s, _) =>
                {
                    await s.Repo.ExecuteNonQueryAsync($"INSERT INTO {s.Context.TbUsersFullName} (Id, Name, Email) VALUES (3, 'Csv', 'red,blue')");
                    return await s.RunStringSplitProjectionAsync();
                },
                Array.Empty<object>())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, QueryResultSnapshot>(
            async (s, _) =>
            {
                await s.Repo.ExecuteNonQueryAsync($"INSERT INTO {s.Context.TbUsersFullName} (Id, Name, Email) VALUES (3, 'Csv', 'red,blue')");
                return await s.RunStringSplitProjectionAsync();
            },
            Array.Empty<object>());

        AssertSnapshot(RequireSnapshot(result, nameof(SelectStringSplitFunctionTest)), Snapshot(["value"],
            Row("red"),
            Row("blue")));
    }

    /// <summary>
    /// EN: Verifies FOR JSON PATH serializes the shared users projection for the current provider.
    /// PT: Verifica se FOR JSON PATH serializa a projecao compartilhada de usuarios para o provedor atual.
    /// </summary>
    [FidelityFact]
    public async Task SelectForJsonPathTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        if (!dialect.SupportsForJsonClause)
        {
            await FluentActions.Awaiting(() => testService.RunTestAsync<SelectTableScenario, QueryServiceTest, string?>(
                (s, _) => s.RunForJsonPathProjectionAsync(),
                Array.Empty<object>())).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await testService.RunTestAsync<SelectTableScenario, QueryServiceTest, string?>(
            async (s, _) =>
            {
                await s.Repo.ExecuteNonQueryAsync(dialect.InsertUser(s.Context, 2, "Bob"));
                return await s.RunForJsonPathProjectionAsync();
            },
            Array.Empty<object>());

        JsonTextAssertions.ShouldMatchJsonText(
            result,
            """
{"users":[{"User":{"Id":1,"Name":"Alice"}},{"User":{"Id":2,"Name":"Bob"}}]}
""");
    }

    /// <summary>
    /// EN: Verifies APPLY projections together with temporal join comparisons for the shared users and orders scenario.
    /// PT: Verifica projeções APPLY junto com comparacoes temporais em join no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [FidelityFact]
    public async Task SelectApplyTemporalCompositeTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply, QueryResultSnapshot temporal)>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = (QueryResultSnapshot?)await s.RunCrossApplyProjectionAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest cross-apply result was null.");
                    var outerApply = (QueryResultSnapshot?)await s.RunOuterApplyProjectionAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest outer-apply result was null.");
                    var temporal = (QueryResultSnapshot?)await s.RunJoinTemporalMatrixAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest temporal result was null.");
                    return (crossApply, outerApply, temporal);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply, QueryResultSnapshot temporal)>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = (QueryResultSnapshot?)await s.RunCrossApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest cross-apply result was null.");
                var outerApply = (QueryResultSnapshot?)await s.RunOuterApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest outer-apply result was null.");
                var temporal = (QueryResultSnapshot?)await s.RunJoinTemporalMatrixAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyTemporalCompositeTest temporal result was null.");
                return (crossApply, outerApply, temporal);
            });

        var (applyCross, applyOuter, temporal) = RequireSnapshotTriple(result, nameof(SelectApplyTemporalCompositeTest));
        AssertSnapshot(applyCross, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C")));
        AssertSnapshot(applyOuter, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C"), Row(3m, "Carla", null)));
        AssertSnapshot(temporal, RawSnapshot(["UserId", "OrderCount", "HasNoOrders", "MinOrderedBeforeNow", "MaxOrderedBeforeNextDay", "PendingDeliveries", "UserCreatedBeforeNow"],
            Row(1m, 2m, 0m, 1m, 1m, 2m, 1m),
            Row(2m, 1m, 0m, 1m, 1m, 1m, 1m),
            Row(3m, 0m, 1m, 1m, 1m, 1m, 1m)));
    }

    /// <summary>
    /// EN: Verifies APPLY, window functions, and temporal join comparisons together for the shared users and orders scenario.
    /// PT: Verifica APPLY, funcoes de janela e comparacoes temporais em join juntas no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [FidelityFact]
    public async Task SelectApplyWindowTemporalCompositeTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply, QueryResultSnapshot window, QueryResultSnapshot temporal)>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = (QueryResultSnapshot?)await s.RunCrossApplyProjectionAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest cross-apply result was null.");
                    var outerApply = (QueryResultSnapshot?)await s.RunOuterApplyProjectionAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest outer-apply result was null.");
                    var window = (QueryResultSnapshot?)await s.RunJoinWindowMatrixAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest window result was null.");
                    var temporal = (QueryResultSnapshot?)await s.RunJoinWindowTemporalMatrixAsync(a)
                        ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest temporal result was null.");
                    return (crossApply, outerApply, window, temporal);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest, (QueryResultSnapshot crossApply, QueryResultSnapshot outerApply, QueryResultSnapshot window, QueryResultSnapshot temporal)>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = (QueryResultSnapshot?)await s.RunCrossApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest cross-apply result was null.");
                var outerApply = (QueryResultSnapshot?)await s.RunOuterApplyProjectionAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest outer-apply result was null.");
                var window = (QueryResultSnapshot?)await s.RunJoinWindowMatrixAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest window result was null.");
                var temporal = (QueryResultSnapshot?)await s.RunJoinWindowTemporalMatrixAsync(a)
                    ?? throw new InvalidOperationException("SelectApplyWindowTemporalCompositeTest temporal result was null.");
                return (crossApply, outerApply, window, temporal);
            });

        var (windowCross, windowOuter, windowCount, windowTemporal) = RequireSnapshotQuad(result, nameof(SelectApplyWindowTemporalCompositeTest));
        AssertSnapshot(windowCross, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C")));
        AssertSnapshot(windowOuter, Snapshot(ApplyProjectionColumnNames(), Row(1m, "Alice", "B"), Row(2m, "Bob", "C"), Row(3m, "Carla", null)));
        AssertSnapshot(windowCount, RawSnapshot(["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote"],
            Row(1m, 10m, 1m, 2m, null),
            Row(1m, 11m, 2m, 2m, "A"),
            Row(2m, 12m, 1m, 1m, null)));
        AssertSnapshot(windowTemporal, RawSnapshot(["UserId", "OrderId", "RowNumberInUser", "OrdersPerUser", "PreviousNote", "OrderedBeforeNow", "NextDayAfterOrder", "UserCreatedBeforeNow"],
            Row(1m, 10m, 1m, 2m, null, 1m, 1m, 1m),
            Row(1m, 11m, 2m, 2m, "A", 1m, 1m, 1m),
            Row(2m, 12m, 1m, 1m, null, 1m, 1m, 1m)));
    }

    private static QueryResultSnapshot RequireSnapshot(object? value, string label)
        => value is QueryResultSnapshot snapshot
            ? snapshot
            : throw new InvalidOperationException($"{label} did not return a query snapshot.");

    private static (QueryResultSnapshot First, QueryResultSnapshot Second) RequireSnapshotPair(object? value, string label)
    {
        if (!TryDecomposeSnapshots(value, 2, out var snapshots))
            throw new InvalidOperationException($"{label} did not return two query snapshots.");

        return (
            RequireSnapshot(snapshots[0], $"{label}[0]"),
            RequireSnapshot(snapshots[1], $"{label}[1]")
        );
    }

    private static (QueryResultSnapshot First, QueryResultSnapshot Second, QueryResultSnapshot Third) RequireSnapshotTriple(object? value, string label)
    {
        if (!TryDecomposeSnapshots(value, 3, out var snapshots))
            throw new InvalidOperationException($"{label} did not return three query snapshots.");

        return (
            RequireSnapshot(snapshots[0], $"{label}[0]"),
            RequireSnapshot(snapshots[1], $"{label}[1]"),
            RequireSnapshot(snapshots[2], $"{label}[2]")
        );
    }

    private static (QueryResultSnapshot First, QueryResultSnapshot Second, QueryResultSnapshot Third, QueryResultSnapshot Fourth) RequireSnapshotQuad(object? value, string label)
    {
        if (!TryDecomposeSnapshots(value, 4, out var snapshots))
            throw new InvalidOperationException($"{label} did not return four query snapshots.");

        return (
            RequireSnapshot(snapshots[0], $"{label}[0]"),
            RequireSnapshot(snapshots[1], $"{label}[1]"),
            RequireSnapshot(snapshots[2], $"{label}[2]"),
            RequireSnapshot(snapshots[3], $"{label}[3]")
        );
    }

    private static bool TryDecomposeSnapshots(object? value, int expectedLength, out object?[] snapshots)
    {
        snapshots = [];

        if (value is null)
            return false;

        if (value is object?[] array)
        {
            if (array.Length != expectedLength)
                return false;

            snapshots = array;
            return true;
        }

        if (value is not string && value is System.Collections.IEnumerable enumerable)
        {
            var items = new List<object?>(expectedLength);
            foreach (var item in enumerable)
            {
                items.Add(item);
                if (items.Count > expectedLength)
                    return false;
            }

            if (items.Count != expectedLength)
                return false;

            snapshots = items.ToArray();
            return true;
        }

        if (value is ITuple tuple)
        {
            if (tuple.Length != expectedLength)
                return false;

            snapshots = new object?[expectedLength];
            for (var i = 0; i < expectedLength; i++)
                snapshots[i] = tuple[i];

            return true;
        }

        var valueType = value.GetType();
        if (valueType.FullName?.StartsWith("System.ValueTuple`", StringComparison.Ordinal) == true)
        {
            snapshots = new object?[expectedLength];
            for (var i = 0; i < expectedLength; i++)
            {
                var field = valueType.GetField($"Item{i + 1}");
                if (field is null)
                    return false;

                snapshots[i] = field.GetValue(value);
            }

            return true;
        }

        return false;
    }

    private QueryResultSnapshot Snapshot(string[] columnNames, params object?[][] rows)
    {
        var snapshots = new QueryResultRowSnapshot[rows.Length];
        for (var i = 0; i < rows.Length; i++)
        {
            snapshots[i] = new QueryResultRowSnapshot
            {
                Values = rows[i],
            };
        }

        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(columnNames),
            Rows = snapshots,
        };
    }

    private static QueryResultSnapshot RawSnapshot(string[] columnNames, params object?[][] rows)
    {
        var snapshots = new QueryResultRowSnapshot[rows.Length];
        for (var i = 0; i < rows.Length; i++)
        {
            snapshots[i] = new QueryResultRowSnapshot
            {
                Values = rows[i],
            };
        }

        return new QueryResultSnapshot
        {
            ColumnNames = columnNames,
            Rows = snapshots,
        };
    }

    /// <summary>
    /// EN: Normalizes DateTime input values used by parameter roundtrip tests for the current provider.
    /// PT: Normaliza valores de entrada DateTime usados pelos testes de roundtrip de parametros para o provedor atual.
    /// </summary>
    /// <param name="value">EN: The input DateTime value. PT: O valor DateTime de entrada.</param>
    /// <returns>EN: The normalized DateTime value. PT: O valor DateTime normalizado.</returns>
    protected virtual DateTime NormalizeParameterDateTimeInput(DateTime value)
        => value;

    /// <summary>
    /// EN: Normalizes snapshot column names based on the current provider's behavior to ensure consistent assertions across providers.
    /// PT: Normaliza os nomes de colunas dos snapshots com base no comportamento do provedor atual para garantir asserções consistentes entre os provedores.
    /// </summary>
    /// <param name="columnNames"></param>
    /// <returns></returns>
    protected virtual string[] NormalizeSnapshotColumnNames(string[] columnNames)
        => columnNames;

    /// <summary>
    /// EN: Provides the expected column names for the APPLY projection tests based on the current provider's behavior.
    /// PT: Fornece os nomes de colunas esperados para os testes de projeção APPLY com base no comportamento do provedor atual.
    /// </summary>
    protected virtual string[] ApplyProjectionColumnNames()
        => ["UserId", "UserName", "Note"];

    private static object?[] Row(params object?[] values) => values;

    /// <summary>
    /// EN: Gets the numeric value used when text case comparisons already match for the current provider.
    /// PT: Obtem o valor numerico usado quando as comparacoes de caixa de texto ja coincidem para o provedor atual.
    /// </summary>
    protected virtual decimal TextMatchAlreadyValue => 1m;

    /// <summary>
    /// EN: Formats decimal amounts for snapshot assertions based on the current provider's behavior, ensuring consistent representations across providers.
    /// PT: Formata valores decimais para asserções de snapshot com base no comportamento do provedor atual, garantindo representações consistentes entre os provedores.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    protected virtual string AmountText(decimal value)
        => value.ToString("0.00", CultureInfo.InvariantCulture);

    private static void AssertSnapshot(QueryResultSnapshot actual, QueryResultSnapshot expected)
    {
        actual.Should().BeEquivalentTo(expected, options => options.WithStrictOrdering());
    }

    private async Task<TResult> RunFidelityTestAsync<TScenario, TServiceTest, TResult>(
        object?[][] initialData,
        Func<TServiceTest, object[], Task<TResult>> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest, TResult>(runTest, args);
    }


    private static async Task SeedJoinOrdersAsync(BaseServiceTest serviceTest)
    {
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 10, 1, "A", "o-10", 1.25m, 2, false, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 11, 1, "B", "o-11", 2.75m, 1, true, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 12, 2, "C", "o-12", 5.50m, 4, false, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
    }
}
