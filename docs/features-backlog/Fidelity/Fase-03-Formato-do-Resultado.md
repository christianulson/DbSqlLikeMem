# Fase 3 - Cobrir o shape completo do resultado

## Status

DONE

## Percentual de entrega

100%

## O que foi feito

- `SelectByPkTest`, `SelectAllRowsCountTest`, `SelectCteSimpleTest`, `SelectNotExistsPredicateTest`, `SelectLeftJoinAntiJoinTest`, `SelectNotInSubqueryTest`, `SelectExistsPredicateTest`, `SelectCorrelatedCountTest`, `SelectInSubqueryTest`, `SelectUnionAllProjectionTest`, `SelectDistinctProjectionTest`, `SelectUnionDistinctProjectionTest`, `SelectMultiJoinAggregateTest`, `SelectCrossApplyProjectionTest`, `SelectOuterApplyProjectionTest`, `SelectApplyProjectionTest`, `SelectApplyTemporalCompositeTest` e `SelectApplyWindowTemporalCompositeTest` agora validam `QueryResultSnapshot` completo quando o contrato é relacional.
- `SelectScalarSubqueryCaseMatrixTest`, `SelectScalarCaseMatrixTest`, `SelectJoinTypedExpressionMatrixTest`, `SelectJoinNullAggregateMatrixTest`, `SelectJoinCastNullMatrixTest`, `SelectJoinCastTextComparisonMatrixTest`, `SelectJoinHavingCastMatrixTest`, `SelectJoinLengthNumericMatrixTest`, `SelectJoinTextCaseLengthMatrixTest`, `SelectJoinDistinctCaseMatrixTest`, `SelectJoinDistinctHavingMatrixTest`, `SelectApplyTemporalCompositeTest` e `SelectApplyWindowTemporalCompositeTest` também passaram a comparar o rowset completo.
- `SelectWindowRankDenseRankTest`, `SelectWindowFirstLastValueTest`, `SelectWindowNtileTest`, `SelectWindowPercentRankCumeDistTest` e `SelectWindowNthValueTest` também passaram a comparar o rowset completo.
- `SelectWindowFunctionsTest` também passou a comparar o rowset completo para `ROW_NUMBER`, `LAG` e `LEAD`.
- `SelectBetweenLikeOrderByTest`, `SelectBetweenLikeOrderByMatrixTest`, `SelectOrderByNameTest`, `SelectOrderByNameMatrixTest`, `SelectGroupByNameInitialMatrixTest`, `SelectGroupByNameHavingTest`, `SelectGroupByOrdinalTest`, `SelectOrderByOrdinalTest`, `SelectOrderByOrdinalMatrixTest`, `SelectDistinctOrderByOrdinalTest`, `SelectDistinctOrderByOrdinalMatrixTest`, `SelectDistinctLikeOrderByOrdinalTest`, `SelectDistinctLikeOrderByOrdinalMatrixTest`, `SelectOrderByNameDescendingTest`, `SelectOrderByNameDescendingMatrixTest`, `SelectNamePaginationMatrixTest` e `SelectPagedNameProjectionTest` também passaram a comparar o rowset completo.
- `SelectInListPredicateTest`, `SelectBetweenPredicateTest`, `SelectLikePredicateTest`, `SelectNotLikePredicateTest`, `SelectNotEqualPredicateTest`, `SelectEqualPredicateTest`, `SelectGreaterThanPredicateTest`, `SelectLessThanPredicateTest`, `SelectGreaterThanOrEqualPredicateTest`, `SelectLessThanOrEqualPredicateTest` e `SelectGroupByHavingTest` agora validam o rowset completo em vez de comprimir o resultado em contagem.
- `StringAggregateSummaryMatrixTest` e `StringAggregationSummaryMatrixTest` também passaram a comparar o rowset completo.
- `StringAggregateGroupCaseMatrixTest` e `StringAggregationGroupCaseMatrixTest` também passaram a comparar o rowset completo.
- O helper `RunRelationalCompositeAssertionsAsync` passou a comparar snapshots inteiros para os blocos relacionais do composite.
- O serviço de select por chave primaria agora retorna snapshot completo em vez de apenas o valor escalar.
- Os relatórios relacionais de janela e join que ainda retornavam apenas a quantidade de linhas agora devolvem o rowset completo com `QueryResultSnapshot`.

## Próximos passos

- Iniciar a Fase 4 e inventariar sintaxes rejeitadas por provider.
- Centralizar suportes e restrições no `Dialect`.
- Transformar `skip` em validação negativa quando fizer sentido.
