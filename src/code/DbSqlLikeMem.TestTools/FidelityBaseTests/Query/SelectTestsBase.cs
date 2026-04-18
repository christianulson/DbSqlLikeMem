using FluentAssertions;
using DbSqlLikeMem.TestTools.DML;
using DbSqlLikeMem.TestTools.Query;

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
    [Fact]
    public async Task SelectByPkTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SelectTableScenario, SelectByPKServiceTest>();
    }

    /// <summary>
    /// EN: Verifies that selecting all rows returns the expected row count for the current provider.
    /// PT: Verifica se o select de todas as linhas retorna a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectAllRowsCountTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SelectTableScenario, QueryServiceTest>(
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(dialect.InsertUser(s.Context, 2, "Bob"));
                return await s.RunRowCountAfterSelectAsync(a);
            });
    }

    /// <summary>
    /// EN: Verifies that a simple CTE query returns the expected result for the current provider.
    /// PT: Verifica se uma consulta CTE simples retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectCteSimpleTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect);

        await testService.RunTestAsync<SelectTableScenario, QueryServiceTest>(
            (s, a) => s.RunCteSimpleAsync(a));
    }

    /// <summary>
    /// EN: Verifies correlated scalar subqueries with CASE expressions for the current provider.
    /// PT: Verifica subconsultas escalares correlacionadas com expressoes CASE para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectScalarSubqueryCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectScalarSubqueryCaseMatrixAsync(a));
        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies the scalar CASE matrix over users and orders matches the current provider behavior.
    /// PT: Verifica se a matriz de CASE escalar nas tabelas de usuarios e pedidos coincide com o comportamento do provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectScalarCaseMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectScalarCaseMatrixAsync(a));
        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies that a NOT EXISTS predicate returns the expected anti-join count for the current provider.
    /// PT: Verifica se um predicado NOT EXISTS retorna a contagem esperada de anti-join para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectNotExistsPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectNotExistsPredicateAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a LEFT JOIN anti-join returns the expected row count for the current provider.
    /// PT: Verifica se um anti-join com LEFT JOIN retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectLeftJoinAntiJoinTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectLeftJoinAntiJoinAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a NOT IN subquery returns the expected anti-join count for the current provider.
    /// PT: Verifica se uma subconsulta NOT IN retorna a contagem esperada de anti-join para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectNotInSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectNotInSubqueryAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that an EXISTS predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado EXISTS retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectExistsPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectExistsPredicateAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a correlated count predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado de contagem correlacionada retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectCorrelatedCountTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectCorrelatedCountAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that an IN subquery returns the expected row count for the current provider.
    /// PT: Verifica se uma subconsulta IN retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectInSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectInSubqueryAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a scalar subquery returns the expected result for the current provider.
    /// PT: Verifica se uma subconsulta escalar retorna o resultado esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectScalarSubqueryTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunSelectScalarSubqueryAsync(a));
        result.Should().Be(2L);
    }

    /// <summary>
    /// EN: Verifies that a GROUP BY HAVING query returns the expected row count for the current provider.
    /// PT: Verifica se uma consulta GROUP BY HAVING retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGroupByHavingTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunGroupByHavingAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies that a UNION ALL projection returns the expected row count for the current provider.
    /// PT: Verifica se uma projecao UNION ALL retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectUnionAllProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Bob")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunUnionAllProjectionAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a DISTINCT projection returns the expected row count for the current provider.
    /// PT: Verifica se uma projecao DISTINCT retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectDistinctProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Alice"), (3, "Bob")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunDistinctProjectionAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a multi-join aggregate returns the expected row count for the current provider.
    /// PT: Verifica se uma agregacao com multiplos joins retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectMultiJoinAggregateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect,
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2]);

        var result = await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => s.RunMultiJoinAggregateAsync(a));
        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies an IN-list predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado IN com lista retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectInListPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Alice"), (2, "Bob"), (3, "Charlie")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunInListPredicateMatrixAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a BETWEEN predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado BETWEEN retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectBetweenPredicateTest()
    {

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunBetweenPredicateMatrixAsync(a));
        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a LIKE predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado LIKE retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectLikePredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunLikePredicateMatrixAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies a combined BETWEEN, LIKE, and ORDER BY query returns the expected row order for the current provider.
    /// PT: Verifica se uma consulta combinada com BETWEEN, LIKE e ORDER BY retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectBetweenLikeOrderByTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunBetweenLikeOrderByMatrixAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies the combined BETWEEN, LIKE, and ORDER BY matrix against the current provider behavior.
    /// PT: Verifica a matriz combinada de BETWEEN, LIKE e ORDER BY contra o comportamento do provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectBetweenLikeOrderByMatrixTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData2]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunBetweenLikeOrderByMatrixAsync(a));
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a NOT LIKE predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado NOT LIKE retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectNotLikePredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunNotLikePredicateMatrixAsync(a));
        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies a not-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado diferente de retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectNotEqualPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunNotEqualPredicateMatrixAsync(a));
        result.Should().Be(4);

    }

    /// <summary>
    /// EN: Verifies an equality predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado de igualdade retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectEqualPredicateTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunEqualPredicateMatrixAsync(a));
        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies a parameterized name lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por nome retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectParameterByNameTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunParameterSelectByNameMatrixAsync(a),
            "Bob");
        result.Should().Be("Bob");
    }

    /// <summary>
    /// EN: Verifies a parameterized id lookup returns the expected row for the current provider.
    /// PT: Verifica se uma consulta parametrizada por id retorna a linha esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectParameterByIdTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunParameterSelectByIdMatrixAsync(a),
            "Charlie");
        result.Should().Be("Charlie");
    }

    /// <summary>
    /// EN: Verifies parameter roundtrips over typed user columns for string, numeric, boolean, date, and null values.
    /// PT: Verifica roundtrips de parametros sobre colunas tipadas de usuario para valores de texto, numericos, booleanos, data e nulos.
    /// </summary>
    [Fact]
    public async Task SelectParameterRoundTripMatrixTest()
    {
        var createdAt = dialect.Provider == ProviderId.Npgsql
            ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
            : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
    /// EN: Verifies typed provider parameters roundtrip correctly for ANSI text, fixed-length text, numeric, boolean, temporal, GUID, binary, and null values.
    /// PT: Verifica se parametros tipados do provedor fazem roundtrip corretamente para texto ANSI, texto de comprimento fixo, numericos, booleanos, temporais, GUID, binario e nulos.
    /// </summary>
    [Fact]
    public async Task SelectParameterTypeMatrixTest()
    {
        var createdAt = dialect.Provider == ProviderId.Npgsql
            ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
            : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        var ansiFixedText = "Fixed ANSI";
        var fixedText = "Fixed Text";

        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [SelectTestsBaseSeeds.InicialData]);

        var result = await testService.RunTestAsync<InsertUsersScenario, QueryServiceTest>(
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
    [Fact]
    public async Task SelectParameterDateCurrencyMatrixTest()
    {
        var dateValue = new DateTime(2024, 1, 2, 0, 0, 0, DateTimeKind.Unspecified);
        var currencyValue = 123.45m;

        var result = await RunFidelityTestAsync<InsertUsersScenario, QueryServiceTest>(
            [],
            (s, a) => s.RunParameterDateCurrencyMatrixAsync(a),
            dateValue,
            currencyValue);

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies the broad parameter projection benchmark returns the expected scalar value for the current provider.
    /// PT: Verifica se o benchmark amplo de projeção de parametros retorna o valor escalar esperado para o provedor atual.
    /// </summary>
    [Fact]
    public async Task ParameterProjectionTest()
    {
        var result = await RunFidelityTestAsync<InsertUsersScenario, QueryServiceTest>(
            [],
            (s, a) => s.RunParameterProjection());

        result.Should().Be("benchmark");
    }

    /// <summary>
    /// EN: Verifies a greater-than predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado maior que retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGreaterThanPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunGreaterThanPredicateMatrixAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a less-than predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado menor que retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectLessThanPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunLessThanPredicateMatrixAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a greater-than-or-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado maior ou igual retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGreaterThanOrEqualPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunGreaterThanOrEqualPredicateMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a less-than-or-equal predicate returns the expected row count for the current provider.
    /// PT: Verifica se um predicado menor ou igual retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectLessThanOrEqualPredicateTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData],
            (s, a) => s.RunLessThanOrEqualPredicateMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByNameTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies ordering by Name matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByNameMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies that a UNION projection returns the expected distinct row count for the current provider.
    /// PT: Verifica se uma projecao UNION retorna a contagem distinta esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectUnionDistinctProjectionTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Carla"));
                return await s.RunUnionDistinctProjectionAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies grouped names by initial with distinct counts and HAVING filtering for the current provider.
    /// PT: Verifica nomes agrupados por inicial com contagens distintas e filtro HAVING para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGroupByNameInitialMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]],
            (s, a) => s.RunGroupByNameInitialMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies GROUP BY Name with HAVING filtering over the configured users table for the current provider.
    /// PT: Verifica GROUP BY Name com filtro HAVING na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGroupByNameHavingTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie")]],
            (s, a) => s.RunGroupByNameHavingMatrixAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies GROUP BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de GROUP BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectGroupByOrdinalTest()
    {
        object?[][] initialData = [[(1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris")]];

        if (!dialect.SupportsGroupByOrdinal)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
                initialData,
                (s, a) => s.RunGroupByOrdinalMatrixAsync(a))).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            initialData,
            (s, a) => s.RunGroupByOrdinalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies ORDER BY ordinal resolution over the configured users table for the current provider.
    /// PT: Verifica a resolucao de ORDER BY ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]],
            (s, a) => s.RunOrderByOrdinalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies ordering by ordinal matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao por ordinal retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Alpha"), (2, "Bravo"), (3, "Charlie")]],
            (s, a) => s.RunOrderByOrdinalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies DISTINCT ordering by ordinal over the configured users table for the current provider.
    /// PT: Verifica a ordenacao DISTINCT por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectDistinctOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctOrderByOrdinalMatrixAsync(a));

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies DISTINCT ordering by ordinal matrix over the configured users table for the current provider.
    /// PT: Verifica a matriz de ordenacao DISTINCT por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectDistinctOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctOrderByOrdinalMatrixAsync(a));

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies DISTINCT with a text filter ordered by ordinal over the configured users table for the current provider.
    /// PT: Verifica DISTINCT com filtro de texto ordenado por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectDistinctLikeOrderByOrdinalTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctLikeOrderByOrdinalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies DISTINCT with a text filter ordered by ordinal matrix over the configured users table for the current provider.
    /// PT: Verifica a matriz DISTINCT com filtro de texto ordenado por ordinal na tabela de usuarios configurada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectDistinctLikeOrderByOrdinalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.InicialData3],
            (s, a) => s.RunDistinctLikeOrderByOrdinalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies descending ordering by Name returns the expected row order for the current provider.
    /// PT: Verifica se a ordenacao descendente por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByNameDescendingTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameDescendingMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies descending ordering by Name matrix returns the expected row order for the current provider.
    /// PT: Verifica se a matriz de ordenacao descendente por Name retorna a ordem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOrderByNameDescendingMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Charlie"), (2, "Bob"), (3, "Alice")]],
            (s, a) => s.RunOrderByNameDescendingMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a paged ordered query built with ROW_NUMBER for the current provider.
    /// PT: Verifica uma consulta ordenada e paginada construida com ROW_NUMBER para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectNamePaginationMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersScenario, QueryServiceTest>(
            [[(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]],
            (s, a) => s.RunNamePaginationMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies native pagination syntax over an ordered users table for the current provider.
    /// PT: Verifica sintaxe nativa de paginação sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectPagedNameProjectionTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, [[(1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo")]]);

        var result = await testService.RunTestAsync<UsersScenario, QueryServiceTest>(
            (s, a) => s.RunPagedNameProjectionMatrixAsync());
        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a relational query bundle across users and orders tables for the current provider.
    /// PT: Verifica um conjunto de consultas relacionais nas tabelas de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectRelationalCompositeTest()
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, 
            [SelectTestsBaseSeeds.seedUsers2, SelectTestsBaseSeeds.seedOrders2]);

        await testService.RunTestAsync<UsersOrdersScenario, QueryServiceTest>(
            (s, a) => RunRelationalCompositeAssertionsAsync(s));
    }

    /// <summary>
    /// EN: Verifies that common window functions return the expected row counts for the current provider.
    /// PT: Verifica se funcoes de janela comuns retornam a contagem esperada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowFunctionsTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bob"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Charlie"));

                var rowNumber = await s.RunWindowRowNumberAsync(a);
                var lag = await s.RunWindowLagAsync(a);
                var lead = await s.RunWindowLeadAsync(a);
                return (rowNumber, lag, lead);
            });

        result.Should().BeEquivalentTo((3, 3, 2));
    }

    /// <summary>
    /// EN: Verifies ranking window functions with duplicate names for the current provider.
    /// PT: Verifica funcoes de janela de ranking com nomes duplicados para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowRankDenseRankTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowRankDenseRank(a.Length > 0 ? a : ["Users"]);
            });

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies FIRST_VALUE and LAST_VALUE window projections for the current provider.
    /// PT: Verifica projeções FIRST_VALUE e LAST_VALUE de janela para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowFirstLastValueTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowFirstLastValue(a.Length > 0 ? a : ["Users"]);
            });

        result.Should().Be(4);
    }


    /// <summary>
    /// EN: Verifies NTILE distribution over an ordered users table for the current provider.
    /// PT: Verifica a distribuicao NTILE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowNtileTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowNtile(a.Length > 0 ? a : ["Users"]);
            });

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies PERCENT_RANK and CUME_DIST window projections for the current provider.
    /// PT: Verifica projeções de janela PERCENT_RANK e CUME_DIST para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowPercentRankCumeDistTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowPercentRankCumeDist(a.Length > 0 ? a : ["Users"]);
            });

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies NTH_VALUE over an ordered users table for the current provider.
    /// PT: Verifica NTH_VALUE sobre uma tabela de usuarios ordenada para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectWindowNthValueTest()
    {
        if (!dialect.SupportsNthValueWindowFunction)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
                [],
                async (s, a) =>
                {
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                    await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                    return await s.RunWindowNthValue(a.Length > 0 ? a : ["Users"]);
                }, "Users")).Should().ThrowAsync<NotSupportedException>();
            return;
        }

        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
            [],
            async (s, a) =>
            {
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 2, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 3, "Bravo"));
                await s.Repo.ExecuteNonQueryAsync(s.Repo.Dialect.InsertUser(s.Context, 4, "Charlie"));
                return await s.RunWindowNthValue(a.Length > 0 ? a : ["Users"]);
            },
            "Users");

        result.Should().Be(4);
    }

    /// <summary>
    /// EN: Verifies range filtering and pivot-style aggregation for the current provider.
    /// PT: Verifica filtragem por faixa e agregacao no estilo pivot para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectRangeAndPivotTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
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
    [Fact]
    public async Task SelectPartitionPruningTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
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
    [Fact]
    public async Task SelectPivotCountTest()
    {
        var result = await RunFidelityTestAsync<SelectTableScenario, QueryServiceTest>(
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
    [Fact]
    public async Task SelectJoinTypedExpressionMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers2],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinTypedExpressionMatrixAsync(a);
            });

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies a left join aggregate projection that preserves users without orders for the current provider.
    /// PT: Verifica uma projeção agregada com left join que preserva usuarios sem pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinNullAggregateMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinNullAggregateMatrixAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a grouped left join projection that blends casts, null handling, and aggregate formatting for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que mistura casts, tratamento de null e formatacao de agregados para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinCastNullMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinCastNullMatrixAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a grouped left join projection that casts aggregate values to text and compares them for the current provider.
    /// PT: Verifica uma projeção agrupada com left join que converte agregados para texto e os compara para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinCastTextComparisonMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinCastTextComparisonMatrixAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a grouped join query with HAVING filters and casted aggregate outputs for the current provider.
    /// PT: Verifica uma consulta agrupada com filtros HAVING e saidas agregadas convertidas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinHavingCastMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinHavingCastMatrixAsync(a);
            });

        result.Should().Be(1);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that mixes string-length expressions with numeric conversions and aggregates for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura expressoes de comprimento de texto com conversoes numericas e agregados para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinLengthNumericMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinLengthNumericMatrixAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that blends string case, string length, and aggregate comparisons for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura caixa de texto, comprimento de texto e comparacoes agregadas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinTextCaseLengthMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers],
            async (s, a) =>
            {
                await SeedJoinOrdersAsync(s);
                return await s.RunJoinTextCaseLengthMatrixAsync(a);
            });

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies grouped left-join reporting with distinct counts, repeated values, and CASE expressions for the current provider.
    /// PT: Verifica um relatorio agrupado com left join, contagens distintas, valores repetidos e expressoes CASE para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinDistinctCaseMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders],
            (s, a) => s.RunJoinDistinctCaseMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies grouped left-join filtering with HAVING and distinct note counts for the current provider.
    /// PT: Verifica filtragem agrupada com left join, HAVING e contagens distintas de notas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinDistinctHavingMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders],
            (s, a) => s.RunJoinDistinctHavingMatrixAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies that a user and order join returns the expected row count for the current provider.
    /// PT: Verifica se a junção entre usuarios e pedidos retorna a contagem esperada de linhas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinCountTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, DmlMutationSelectJoinServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunTestAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies the basic users and orders join against the current provider behavior.
    /// PT: Verifica o join basico entre usuarios e pedidos contra o comportamento do provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, DmlMutationSelectJoinServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunTestAsync(a));

        result.Should().Be(2);
    }

    private static async Task<object?> RunRelationalCompositeAssertionsAsync(
        QueryServiceTest serviceTest)
    {
        var cte = await serviceTest.RunCteSimpleAsync();
        var existsPredicate = await serviceTest.RunSelectExistsPredicateAsync();
        var correlatedCount = await serviceTest.RunSelectCorrelatedCountAsync();
        var groupByHaving = await serviceTest.RunGroupByHavingAsync();
        var unionAll = await serviceTest.RunUnionAllProjectionAsync();
        var distinct = await serviceTest.RunDistinctProjectionAsync();
        var multiJoin = await serviceTest.RunMultiJoinAggregateAsync();
        var scalarSubquery = Convert.ToInt64(await serviceTest.RunSelectScalarSubqueryAsync(), CultureInfo.InvariantCulture);
        var inSubquery = await serviceTest.RunSelectInSubqueryAsync();
        var pivot = await serviceTest.RunPivotCountAsync();
        cte.Should().Be(1);
        existsPredicate.Should().Be(2);
        correlatedCount.Should().Be(2);
        groupByHaving.Should().Be(1);
        unionAll.Should().Be(2);
        distinct.Should().Be(2);
        multiJoin.Should().Be(4);
        scalarSubquery.Should().Be(2L);
        inSubquery.Should().Be(2);
        pivot.Should().Be(2);

        return (cte, existsPredicate, correlatedCount, groupByHaving, unionAll, distinct, multiJoin, scalarSubquery, inSubquery, pivot);
    }

    /// <summary>
    /// EN: Verifies a grouped join projection that blends temporal comparisons with aggregate counts for the current provider.
    /// PT: Verifica uma projeção agrupada que mistura comparacoes temporais com contagens agregadas para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinTemporalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies a joined window-function projection over the shared users and orders scenario for the current provider.
    /// PT: Verifica uma projeção com funcoes de janela em join no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectJoinWindowMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies joined window projections together with temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public async Task SelectJoinWindowTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowTemporalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies joined window projections together with aggregate and temporal comparisons for the shared users and orders scenario.
    /// PT: Verifica projecoes com janela em join junto com comparacoes agregadas e temporais no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public async Task SelectJoinWindowAggregateTemporalMatrixTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunJoinWindowAggregateTemporalMatrixAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY and OUTER APPLY projections against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeções CROSS APPLY e OUTER APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectApplyProjectionTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = await s.RunCrossApplyProjectionAsync(a);
                    var outerApply = await s.RunOuterApplyProjectionAsync(a);
                    return (crossApply, outerApply);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = await s.RunCrossApplyProjectionAsync(a);
                var outerApply = await s.RunOuterApplyProjectionAsync(a);
                return (crossApply, outerApply);
            });

        result.Should().BeEquivalentTo((2, 3));
    }

    /// <summary>
    /// EN: Verifies CROSS APPLY projection against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeção CROSS APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectCrossApplyProjectionTest()
    {
        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunCrossApplyProjectionAsync(a));

        result.Should().Be(2);
    }

    /// <summary>
    /// EN: Verifies OUTER APPLY projection against the shared users and orders scenario for the current provider.
    /// PT: Verifica projeção OUTER APPLY no cenário compartilhado de usuarios e pedidos para o provedor atual.
    /// </summary>
    [Fact]
    public async Task SelectOuterApplyProjectionTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                (s, a) => s.RunOuterApplyProjectionAsync(a))).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            (s, a) => s.RunOuterApplyProjectionAsync(a));

        result.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies APPLY projections together with temporal join comparisons for the shared users and orders scenario.
    /// PT: Verifica projeções APPLY junto com comparacoes temporais em join no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public async Task SelectApplyTemporalCompositeTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = await s.RunCrossApplyProjectionAsync(a);
                    var outerApply = await s.RunOuterApplyProjectionAsync(a);
                    var temporal = await s.RunJoinTemporalMatrixAsync(a);
                    return (crossApply, outerApply, temporal);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = await s.RunCrossApplyProjectionAsync(a);
                var outerApply = await s.RunOuterApplyProjectionAsync(a);
                var temporal = await s.RunJoinTemporalMatrixAsync(a);
                return (crossApply, outerApply, temporal);
            });

        result.Should().BeEquivalentTo((2, 3, 3));
    }

    /// <summary>
    /// EN: Verifies APPLY, window functions, and temporal join comparisons together for the shared users and orders scenario.
    /// PT: Verifica APPLY, funcoes de janela e comparacoes temporais em join juntas no cenário compartilhado de usuarios e pedidos.
    /// </summary>
    [Fact]
    public async Task SelectApplyWindowTemporalCompositeTest()
    {
        if (!dialect.SupportsOuterApplyProjection)
        {
            await FluentActions.Awaiting(() => RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
                [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
                async (s, a) =>
                {
                    var crossApply = await s.RunCrossApplyProjectionAsync(a);
                    var outerApply = await s.RunOuterApplyProjectionAsync(a);
                    var window = await s.RunJoinWindowMatrixAsync(a);
                    var temporal = await s.RunJoinWindowTemporalMatrixAsync(a);
                    return (crossApply, outerApply, window, temporal);
                })).Should().ThrowAsync<NotSupportedException>();

            return;
        }

        var result = await RunFidelityTestAsync<UsersOrdersScenario, QueryServiceTest>(
            [SelectTestsBaseSeeds.seedUsers, SelectTestsBaseSeeds.seedOrders2],
            async (s, a) =>
            {
                var crossApply = await s.RunCrossApplyProjectionAsync(a);
                var outerApply = await s.RunOuterApplyProjectionAsync(a);
                var window = await s.RunJoinWindowMatrixAsync(a);
                var temporal = await s.RunJoinWindowTemporalMatrixAsync(a);
                return (crossApply, outerApply, window, temporal);
            });

        result.Should().BeEquivalentTo((2, 3, 3, 3));
    }

    private async Task<object?> RunFidelityTestAsync<TScenario, TServiceTest>(
        object?[][] initialData,
        Func<TServiceTest, object[], object?> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest>(
            (serviceTest, runArgs) => Task.FromResult(runTest(serviceTest, runArgs)),
            args);
    }

    private async Task<object?> RunFidelityTestAsync<TScenario, TServiceTest>(
        object?[][] initialData,
        Func<TServiceTest, object[], Task<object?>> runTest,
        params object[] args)
        where TScenario : BaseScenario, ITestScenario
        where TServiceTest : BaseServiceTest
    {
        using var testService = new FidelityTestService<T, T2>(connectionMock, connectionContainer, dialect, initialData);

        return await testService.RunTestAsync<TScenario, TServiceTest>(runTest, args);
    }

    private static async Task SeedJoinOrdersAsync(BaseServiceTest serviceTest)
    {
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 10, 1, "A", "o-10", 1.25m, 2, false, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 11, 1, "B", "o-11", 2.75m, 1, true, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
        await serviceTest.Repo.ExecuteNonQueryAsync(serviceTest.Repo.Dialect.InsertOrder(serviceTest.Context, 12, 2, "C", "o-12", 5.50m, 4, false, serviceTest.Repo.Dialect.TemporalCurrentTimestampExpression()));
    }
}

