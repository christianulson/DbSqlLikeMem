# Fase 5 - Funções e semântica

## Status

IN PROGRESS

## Percentual de entrega

15%

## O que foi feito

- Adicionado o primeiro wrapper de `JsonTableFunctionTestsBase` na suite de fidelidade e iniciado o coverage de `json_each` e `json_tree`.
- Adicionado o wrapper de fidelidade para `JSON insert/cast`, cobrindo o benchmark escalar de leitura JSON com coerção.
- Ligados os handlers de `json_each` e `json_tree` ao executor de table functions do mock.
- Mantida a validacao negativa quando o provider nao suporta funcoes JSON tabulares.
- Expandido o parser e o avaliador para `CAST`, `CONVERT`, `TRY_CAST`, `TRY_CONVERT`, `PARSE` e `TRY_PARSE`, com restricao por capability no dialect.
- Adicionados caminhos de sintaxe e avaliacao para funcoes Firebird como `DATEADD`, `SUBSTRING`, `HASH` e `CRYPT_HASH` quando o dialect suporta a chamada.

## Próximos passos

- Separar funções por categoria.
- Cobrir tipos nativos de retorno e de parâmetro.
- Alinhar temporais, JSON e window functions com o banco real.
