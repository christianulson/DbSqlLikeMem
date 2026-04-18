# Fase 5 - Funções e semântica

## Status

IN PROGRESS

## Percentual de entrega

5%

## O que foi feito

- Adicionado o primeiro wrapper de `JsonTableFunctionTestsBase` na suite de fidelidade e iniciado o coverage de `json_each` e `json_tree`.
- Ligados os handlers de `json_each` e `json_tree` ao executor de table functions do mock.
- Mantida a validacao negativa quando o provider nao suporta funcoes JSON tabulares.

## Próximos passos

- Separar funções por categoria.
- Cobrir tipos nativos de retorno e de parâmetro.
- Alinhar temporais, JSON e window functions com o banco real.
