# Plano de Ação - Fidelidade

## Objetivo

Manter a fidelidade entre mock e container com prioridade para comportamento observável, tipos nativos, parser, estrutura de resultado e exceções.

## Estrutura De Acompanhamento

- Cada fase deste plano deve ter um arquivo de acompanhamento na mesma pasta.
- Nome sugerido: `Fase-XX-Nome-Da-Fase.md`.
- Cada arquivo de fase deve registrar:
  - o que foi feito
  - o percentual de entrega da fase
  - os próximos passos
- O plano principal deve começar com todas as fases em `TODO`.
- O percentual geral do plano deve ser atualizado conforme as fases avancarem.
- A matriz de fidelidade por provider deve ficar em `Percentual-de-Fidelidade-por-Provider.md`.

## Status Do Plano

Percentual geral atual: 99%

| Fase | Arquivo | Status | Entrega |
| --- | --- | --- | --- |
| Fase 1 - Inventário dos tipos nativos | [Fase-01-Parametros.md](./Fase-01-Parametros.md) | DONE | 100% |
| Fase 2 - Expandir os testes de tipo DbParameter | [Fase-02-Tipos-Nativos.md](./Fase-02-Tipos-Nativos.md) | DONE | 100% |
| Fase 3 - Cobrir o shape completo do resultado | [Fase-03-Formato-do-Resultado.md](./Fase-03-Formato-do-Resultado.md) | DONE | 100% |
| Fase 4 - Parser e sintaxe | [Fase-04-Parser-e-Sintaxe.md](./Fase-04-Parser-e-Sintaxe.md) | IN PROGRESS | 75% |
| Fase 5 - Funções e semântica | [Fase-05-Funcoes-e-Semantica.md](./Fase-05-Funcoes-e-Semantica.md) | IN PROGRESS | 15% |
| Fase 6 - Transações e savepoints | [Fase-06-Transacoes-e-Savepoints.md](./Fase-06-Transacoes-e-Savepoints.md) | IN PROGRESS | 98% |
| Fase 7 - Exceções e mensagens | [Fase-07-Excecoes-e-Mensagens.md](./Fase-07-Excecoes-e-Mensagens.md) | DONE | 100% |
| Fase 8 - Testes faltantes do banco real | [Fase-08-Testes-Faltantes.md](./Fase-08-Testes-Faltantes.md) | DONE | 100% |

## Regra Central

- Se o banco real aceita, o mock deve aceitar.
- Se o banco real rejeita, o mock deve rejeitar.
- Se o mock precisar falhar melhor, a exceção pode ter mais contexto, mas sem mudar o gatilho principal.
- Regras por provider devem viver no `Dialect`, não espalhadas em `if`/`switch` nos testes.
- Nao normalizar input, output ou valores lidos do reader dentro do teste so para fazer mock e container parecerem iguais.
- Se a forma do valor precisar ser igual nos dois lados, tratar isso no core da aplicacao ou no dialect.

## Escopo Prioritário

1. Parâmetros.
2. Tipos nativos por provider.
3. Formato do resultado.
4. Parser e sintaxe.
5. Funções e semântica.
6. Transações e savepoints.
7. Exceções e mensagens.
8. Testes faltantes do banco real.

## Fase 1 - Inventário dos tipos nativos

Mapear, por provider, todos os tipos nativos relevantes que aparecem nos testes de fidelidade.

Cobertura mínima esperada por provider:

- `DbConnection`
- `DbCommand`
- `DbParameter`
- `DbTransaction`
- `DbDataReader`

Cobertura adicional quando o provider expuser tipos específicos fora do contrato base:

- tipos de conexão nativos
- tipos de comando nativos
- tipos de parâmetro nativos
- tipos de transação nativos
- tipos de reader nativos
- wrappers/interceptors quando fizerem parte do contrato observável

### Acompanhamento Da Fase

- Status: TODO
- Percentual de entrega: 0%
- Próximos passos:
  - levantar todos os tipos nativos por provider
  - separar o que é contrato comum do que é contrato específico
  - mapear gaps por provider e por wrapper

## Fase 2 - Expandir os testes de tipo

Adicionar contratos de tipo para cada provider nativo, não só os cenários atuais de `DbParameter`.

Direção sugerida:

- validar o runtime type do `DbMock`
- validar o runtime type da conexão
- validar o runtime type do comando criado a partir da conexão
- validar o runtime type do parâmetro criado a partir do comando
- validar o runtime type da transação aberta
- validar o runtime type do reader retornado por consulta
- validar que o teste nao reescreve tipo, forma ou nulabilidade dos valores para compensar diferencas do provider

### Acompanhamento Da Fase

- Status: TODO
- Percentual de entrega: 0%
- Próximos passos:
  - listar wrappers nativos existentes em cada provider
  - criar contratos de tipo para conexão, comando, parâmetro, transação e reader
  - ligar cada contrato ao dialect correspondente

## Fase 3 - Cobrir o shape completo do resultado

Substituir testes que comprimem o resultado para `COUNT(*)` quando o contrato for relacional.

Padrão:

- capturar `QueryResultSnapshot`
- comparar linhas, colunas, aliases e ordem
- normalizar apenas o que o provider real também normaliza

### Acompanhamento Da Fase

- Status: DONE
- Percentual de entrega: 100%
- O que foi feito:
  - `SelectByPkTest`, `SelectAllRowsCountTest`, `SelectAllRowsSnapshotTest`, `SelectCteSimpleTest`, `SelectNotExistsPredicateTest`, `SelectLeftJoinAntiJoinTest`, `SelectNotInSubqueryTest`, `SelectExistsPredicateTest`, `SelectCorrelatedCountTest`, `SelectInSubqueryTest`, `SelectUnionAllProjectionTest`, `SelectDistinctProjectionTest`, `SelectUnionDistinctProjectionTest`, `SelectMultiJoinAggregateTest`, `SelectCrossApplyProjectionTest`, `SelectOuterApplyProjectionTest`, `SelectApplyProjectionTest`, `SelectApplyTemporalCompositeTest` e `SelectApplyWindowTemporalCompositeTest` validam `QueryResultSnapshot` completo quando o contrato é relacional.
  - `SelectScalarSubqueryCaseMatrixTest`, `SelectScalarCaseMatrixTest`, `SelectJoinTypedExpressionMatrixTest`, `SelectJoinNullAggregateMatrixTest`, `SelectJoinCastNullMatrixTest`, `SelectJoinCastTextComparisonMatrixTest`, `SelectJoinHavingCastMatrixTest`, `SelectJoinLengthNumericMatrixTest`, `SelectJoinTextCaseLengthMatrixTest`, `SelectJoinDistinctCaseMatrixTest`, `SelectJoinDistinctHavingMatrixTest`, `SelectApplyTemporalCompositeTest` e `SelectApplyWindowTemporalCompositeTest` também comparam o rowset completo.
  - `SelectWindowRankDenseRankTest`, `SelectWindowFirstLastValueTest`, `SelectWindowNtileTest`, `SelectWindowPercentRankCumeDistTest` e `SelectWindowNthValueTest` também comparam o rowset completo.
  - `SelectWindowFunctionsTest` também compara o rowset completo para `ROW_NUMBER`, `LAG` e `LEAD`.
  - `SelectBetweenLikeOrderByTest`, `SelectBetweenLikeOrderByMatrixTest`, `SelectOrderByNameTest`, `SelectOrderByNameMatrixTest`, `SelectGroupByNameInitialMatrixTest`, `SelectGroupByNameHavingTest`, `SelectGroupByOrdinalTest`, `SelectOrderByOrdinalTest`, `SelectOrderByOrdinalMatrixTest`, `SelectDistinctOrderByOrdinalTest`, `SelectDistinctOrderByOrdinalMatrixTest`, `SelectDistinctLikeOrderByOrdinalTest`, `SelectDistinctLikeOrderByOrdinalMatrixTest`, `SelectOrderByNameDescendingTest`, `SelectOrderByNameDescendingMatrixTest`, `SelectNamePaginationMatrixTest` e `SelectPagedNameProjectionTest` também comparam o rowset completo.
  - `SelectInListPredicateTest`, `SelectBetweenPredicateTest`, `SelectLikePredicateTest`, `SelectNotLikePredicateTest`, `SelectNotEqualPredicateTest`, `SelectEqualPredicateTest`, `SelectGreaterThanPredicateTest`, `SelectLessThanPredicateTest`, `SelectGreaterThanOrEqualPredicateTest`, `SelectLessThanOrEqualPredicateTest` e `SelectGroupByHavingTest` validam o rowset completo em vez de comprimir o resultado em contagem.
  - `StringAggregateSummaryMatrixTest` e `StringAggregationSummaryMatrixTest` também comparam o rowset completo.
  - `StringAggregateGroupCaseMatrixTest` e `StringAggregationGroupCaseMatrixTest` também comparam o rowset completo.
  - O helper `RunRelationalCompositeAssertionsAsync` passou a comparar snapshots inteiros para os blocos relacionais do composite.
  - O serviço de select por chave primaria passou a retornar snapshot completo em vez de apenas o valor escalar.
  - O snapshot de `SELECT *` básico passou a cobrir duas linhas para validar o rowset completo do contrato relacional.
  - Os relatórios relacionais de janela e join que ainda retornavam apenas a quantidade de linhas agora devolvem o rowset completo com `QueryResultSnapshot`.
  - Os helpers de decomposição de snapshot ficaram mais robustos e deixaram de tratar `string` como sequência de snapshots.
  - A decomposição de tuplas nos helpers compartilhados passou a usar `ITuple` diretamente, evitando reflexão para snapshots compostos.
- Próximos passos:
  - manter a cobertura de snapshot completa quando novos relatórios relacionais surgirem
  - evitar regressões que comprimam rowset em contagem quando o contrato exigir o shape completo

## Fase 4 - Parser e sintaxe

## Acompanhamento Da Fase

- Status: IN PROGRESS
- Percentual de entrega: 75%
- O que foi feito:
- movidos os trechos SQL de `UPDATE/DELETE JOIN` para o `Dialect` base e para os dialects específicos de SQL Server e PostgreSQL
- eliminado o `skip` para `json_each` e `json_tree` e substituido por validacao negativa quando o provider nao suporta funcoes JSON tabulares
- tornado `INSERT RETURNING` uma capability explicita no dialect e restringido o teste compartilhado aos providers que realmente suportam essa sintaxe
- expandido o parser e o executor de `ALTER SEQUENCE` para suportar `INCREMENT BY` sem perder o caminho de `RESTART WITH`
- expandido o parser e o executor de `ALTER SEQUENCE` para suportar `OWNED BY NONE` e `OWNED BY tabela.coluna`, com drop automatico da sequence quando a tabela proprietaria e removida
- expandido o parser e o executor de `CREATE SEQUENCE` para suportar `OWNED BY NONE` e `OWNED BY tabela.coluna` com o mesmo estado de ownership
- adicionados testes de parser para `CREATE SEQUENCE ... OWNED BY` e `ALTER SEQUENCE ... OWNED BY NONE`
- Próximos passos:
  - inventariar sintaxes rejeitadas por provider
  - mover suportes e restrições para o dialect
  - transformar `skip` em validação negativa quando fizer sentido

## Fase 5 - Funções e semântica

- Cobrir funções temporais, JSON, string, janela, agregação e joins/applies.
- Validar tipo e valor, não só texto formatado.
- Ajustar normalização por provider quando o retorno nativo diferir.

### Acompanhamento Da Fase

- Status: IN PROGRESS
- Percentual de entrega: 15%
- O que foi feito:
  - adicionado o primeiro wrapper de `JsonTableFunctionTestsBase` na suite de fidelidade
  - adicionado o wrapper de fidelidade para `JSON insert/cast`, cobrindo o benchmark escalar de leitura JSON com coerção
  - iniciado o coverage de `json_each` e `json_tree`
  - ligados os handlers de `json_each` e `json_tree` ao executor de table functions do mock
  - mantida a validacao negativa quando o provider nao suporta funcoes JSON tabulares
- Próximos passos:
  - separar funções por categoria
  - cobrir tipos nativos de retorno e de parâmetro
  - alinhar temporais, JSON e window functions com o banco real

## Fase 6 - Transações

- Cobrir begin, commit, rollback, nested transactions, savepoint e release.
- Validar o mesmo gatilho de falha do provider real.

### Acompanhamento Da Fase

- Status: TODO
- Percentual de entrega: 0%
- Próximos passos:
  - mapear contratos por provider
  - validar savepoints e nested flow
  - confirmar mensagens e exceções esperadas

## Fase 7 - Exceções

- Manter o mesmo tipo principal de falha do provider real.
- Adicionar contexto somente quando não alterar o contrato.
- Garantir consistência entre mock e container.

### Acompanhamento Da Fase

- Status: TODO
- Percentual de entrega: 0%
- Próximos passos:
  - levantar exceções reais por cenário
  - padronizar enriquecimento de debug sem mudar o gatilho
  - revisar mensagens dependentes de parser, parâmetro e execução

## Fase 8 - Testes faltantes do banco real

- Criar testes para funcionalidades que existem no banco real mas ainda não têm cobertura.
- Não importa se a funcionalidade existe no mock, o que importa é que deve existir teste.
- Cada teste criado serve de base para futura implementação no código.

### Acompanhamento Da Fase

- Status: TODO
- Percentual de entrega: 0%
- Próximos passos:
  - inventariar funcionalidades faltantes por provider
  - criar testes even if unimplemented
  - documentar gaps no acompanhamento

## Definition of Done

- O teste de fidelidade usa o provider real como referência.
- O `Dialect` concentra as diferenças por provider.
- Os testes de tipo cobrem todos os tipos nativos relevantes do provider.
- Os testes de resultado usam snapshot quando a comparação é relacional.
- As falhas esperadas são validadas explicitamente, não escondidas por skip.
