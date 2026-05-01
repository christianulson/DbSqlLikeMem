namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Reads a user name by primary key through the shared SelectByPK service and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária pelo service compartilhado SelectByPK e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var state = GetPreparedSelectByPkState();
        var value = state.Service.RunTestAsync(1).GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var state = GetPreparedSelectJoinState();
        var value = state.Service.RunTestAsync(1).GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the join-count benchmark by reusing the shared join flow.
    /// PT-br: Executa o benchmark de contagem do join reutilizando o fluxo compartilhado de join.
    /// </summary>
    protected virtual void RunSelectJoinCount()
        => RunSelectJoin();

    /// <summary>
    /// EN: Executes the APPLY projection benchmark by chaining CROSS APPLY and OUTER APPLY projections.
    /// PT-br: Executa o benchmark de projeção APPLY encadeando projecoes CROSS APPLY e OUTER APPLY.
    /// </summary>
    protected virtual void RunSelectApplyProjection()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
    }

    /// <summary>
    /// EN: Executes the window-functions benchmark by chaining row-number, lag, and lead projections.
    /// PT-br: Executa o benchmark de funcoes de janela encadeando row-number, lag e lead.
    /// </summary>
    protected virtual void RunSelectWindowFunctions()
    {
        RunWindowRowNumber();
        RunWindowLag();
        RunWindowLead();
    }

    /// <summary>
    /// EN: Executes the scalar-subquery CASE matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz CASE com subconsulta escalar e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunSelectScalarSubqueryCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "SelectScalarSubqueryCaseMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 1, "o-3"), (4, 2, "o-4")]);
        var value = state.Service.RunSelectScalarSubqueryCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the range-and-pivot benchmark by chaining partition pruning and pivot counting.
    /// PT-br: Executa o benchmark de faixa e pivot encadeando partition pruning e contagem pivot.
    /// </summary>
    protected virtual void RunSelectRangeAndPivot()
    {
        var state = GetPreparedSelectTableQueryState("SelectRangeAndPivot");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            for (var id = 3; id <= 12; id++)
            {
                state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, id, $"User-{id}")).GetAwaiter().GetResult();
            }

            var partitionCount = state.Service.RunPartitionPruningSelectAsync().GetAwaiter().GetResult();
            var pivotCount = state.Service.RunPivotCountAsync().GetAwaiter().GetResult();
            GC.KeepAlive(partitionCount);
            GC.KeepAlive(pivotCount);
        }
        finally
        {
            for (var id = 2; id <= 12; id++)
            {
                state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = {id}").GetAwaiter().GetResult();
            }
        }
    }

    /// <summary>
    /// EN: Executes the IN-list predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado IN com lista e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunInListPredicate()
    {
        var state = GetPreparedUsersQueryState("InListPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        var value = state.Service.RunInListPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the BETWEEN predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado BETWEEN e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunBetweenPredicate()
    {
        var state = GetPreparedUsersQueryState("BetweenPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunBetweenPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the LIKE predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado LIKE e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLikePredicate()
    {
        var state = GetPreparedUsersQueryState("LikePredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLikePredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the NOT LIKE predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado NOT LIKE e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotLikePredicate()
    {
        var state = GetPreparedUsersQueryState("NotLikePredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNotLikePredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the not-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado diferente de e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("NotEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNotEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the equality predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado de igualdade e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("EqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the greater-than predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado maior que e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGreaterThanPredicate()
    {
        var state = GetPreparedUsersQueryState("GreaterThanPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunGreaterThanPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the less-than predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado menor que e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLessThanPredicate()
    {
        var state = GetPreparedUsersQueryState("LessThanPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLessThanPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the greater-than-or-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado maior ou igual e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGreaterThanOrEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("GreaterThanOrEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunGreaterThanOrEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the less-than-or-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado menor ou igual e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLessThanOrEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("LessThanOrEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLessThanOrEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the NOT IN subquery with NULL benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de subconsulta NOT IN com NULL e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotInSubqueryNull()
    {
        var state = GetPreparedUsersQueryState("NotInSubqueryNull", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        using var command = state.Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Id, Name
FROM {state.Context.TbUsersFullName}
WHERE Id NOT IN (
    SELECT 1
    FROM {state.Context.TbUsersFullName} u
    WHERE u.Id = 1
    UNION ALL
    SELECT NULL
    FROM {state.Context.TbUsersFullName} u
    WHERE u.Id = 1
)
ORDER BY Id
""";

        using var reader = command.ExecuteReaderAsync().GetAwaiter().GetResult();
        var snapshot = QueryResultSnapshotReader.Capture(reader);
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the relational composite benchmark by chaining the main relational query flows.
    /// PT-br: Executa o benchmark composto relacional encadeando os principais fluxos relacionais.
    /// </summary>
    protected virtual void RunRelationalComposite()
    {
        RunCteSimple();
        RunSelectExistsPredicate();
        RunSelectNotExistsPredicate();
        RunSelectLeftJoinAntiJoin();
        RunSelectCorrelatedCount();
        RunSelectScalarCaseMatrix();
        RunGroupByHaving();
        RunUnionAllProjection();
        RunUnionDistinctProjection();
        RunDistinctProjection();
        RunMultiJoinAggregate();
        RunSelectScalarSubquery();
        RunSelectInSubquery();
        RunSelectNotInSubquery();
        RunSelectBetweenLikeOrderByMatrix();
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunPivotCount();
    }

    /// <summary>
    /// EN: Executes the all-rows count benchmark and keeps the row-count result alive.
    /// PT-br: Executa o benchmark de contagem de todas as linhas e mantem o resultado da contagem ativo.
    /// </summary>
    protected virtual void RunAllRowsCount()
    {
        var state = GetPreparedSelectTableQueryState("AllRowsCount");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            var count = state.Service.RunRowCountAfterSelectAsync().GetAwaiter().GetResult();
            GC.KeepAlive(count);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the all-rows snapshot benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de snapshot de todas as linhas e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunAllRowsSnapshot()
    {
        var state = GetPreparedSelectTableQueryState("AllRowsSnapshot");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            var value = state.Service.RunAllRowsSnapshotAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the CTE MATERIALIZED benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de CTE MATERIALIZED e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunCteMaterializedHint()
    {
        if (!Dialect.SupportsWithMaterializedHint)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("CteMaterializedHint");
        var value = state.Service.RunCteMaterializedHintAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT ON projection benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de projecao DISTINCT ON e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctOnProjection()
    {
        if (!Dialect.SupportsDistinctOnProjection)
        {
            return;
        }

        var state = GetPreparedUsersOrdersQueryState(
            "DistinctOnProjection",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunDistinctOnProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY Name matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY Name e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByNameMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByNameMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"));
        var value = state.Service.RunOrderByNameMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByOrdinalMatrix", (1, "Alpha"), (2, "Bravo"), (3, "Charlie"));
        var value = state.Service.RunOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY Name descending matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY Name descendente e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByNameDescendingMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByNameDescendingMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"));
        var value = state.Service.RunOrderByNameDescendingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the name pagination matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz de paginacao por nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNamePaginationMatrix()
    {
        var state = GetPreparedUsersQueryState("NamePaginationMatrix", (1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNamePaginationMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY name initial matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY por inicial do nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByNameInitialMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "GroupByNameInitialMatrix",
            (1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris"));
        var value = state.Service.RunGroupByNameInitialMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY name HAVING matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY com HAVING por nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByNameHavingMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "GroupByNameHavingMatrix",
            (1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie"));
        var value = state.Service.RunGroupByNameHavingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY por ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByOrdinalMatrix()
    {
        if (!Dialect.SupportsGroupByOrdinal)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "GroupByOrdinalMatrix",
            (1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris"));
        var value = state.Service.RunGroupByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT order-by-ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("DistinctOrderByOrdinalMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var value = state.Service.RunDistinctOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT text-filter order-by-ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com filtro de texto e ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctLikeOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("DistinctLikeOrderByOrdinalMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var value = state.Service.RunDistinctLikeOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined typed-expression matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com expressoes tipadas em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinTypedExpressionMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinTypedExpressionMatrix");
        var value = state.Service.RunJoinTypedExpressionMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined null-aggregate matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz agregada com null em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinNullAggregateMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinNullAggregateMatrix");
        var value = state.Service.RunJoinNullAggregateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined cast-null matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com cast e null em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinCastNullMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinCastNullMatrix");
        var value = state.Service.RunJoinCastNullMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined cast-text comparison matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com cast e comparacao textual em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinCastTextComparisonMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinCastTextComparisonMatrix");
        var value = state.Service.RunJoinCastTextComparisonMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined HAVING cast matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz HAVING com cast em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinHavingCastMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinHavingCastMatrix");
        var value = state.Service.RunJoinHavingCastMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined length-and-numeric matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com comprimento e numericos em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinLengthNumericMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinLengthNumericMatrix");
        var value = state.Service.RunJoinLengthNumericMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined text-case-length matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com caixa, texto e comprimento em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinTextCaseLengthMatrix()
    {
        var state = GetPreparedUsersOrdersMetricsQueryState("JoinTextCaseLengthMatrix");
        var value = state.Service.RunJoinTextCaseLengthMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined distinct-case matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com CASE em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinDistinctCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinDistinctCaseMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "A"), (12, 1, "B"), (13, 2, "C")]);
        var value = state.Service.RunJoinDistinctCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined distinct-HAVING matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com HAVING em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinDistinctHavingMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinDistinctHavingMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "A"), (12, 1, "B"), (13, 2, "C")]);
        var value = state.Service.RunJoinDistinctHavingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the STRING_SPLIT projection benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark de projecao STRING_SPLIT e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunStringSplitProjection()
    {
        if (!Dialect.SupportsApplyClause || !Dialect.SupportsStringSplitFunction)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("StringSplitProjection");
        try
        {
            state.Repo.ExecuteNonQueryAsync($"INSERT INTO {state.Context.TbUsersFullName} (Id, Name, Email) VALUES (3, 'Csv', 'red,blue')").GetAwaiter().GetResult();
            var value = state.Service.RunStringSplitProjectionAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 3").GetAwaiter().GetResult();
        }
    }
}
