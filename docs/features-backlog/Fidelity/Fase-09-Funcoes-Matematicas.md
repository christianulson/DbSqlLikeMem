# Fase 9 - Funcoes matematicas restantes

## Status

DONE

## Percentual de entrega

100%

## Objetivo

Cobrir as funcoes matematicas aceitas por cada provider real que ainda nao entraram no contrato compartilhado de fidelidade.

## Base Ja Coberta

- `ABS`
- `CEIL` / `CEILING`
- `DEGREES`
- `FLOOR`
- `LN`
- `LOG10`
- `LOG2`
- `LOG(base, value)` nos providers que expoem a forma com dois argumentos
- `PI`
- `COT` nos providers que expoem a funcao
- `BIN`, `GREATEST`, `LEAST`, `LOG2`, `MOD`, `POW` e `TRUNCATE` nos providers da familia MySQL que expoem a surface utilitaria correspondente
- `ACOS`, `ASIN`, `ATAN`, `ATAN2`, `COS`, `EXP`, `SIN` e `TAN` nos providers que expoem a surface transcendente compartilhada
- `RAND` no SQL Server, SQL Azure, MySQL, MariaDB, Db2 e Firebird
- `REMAINDER` no Oracle
- `TRUNC(X)` nos providers que expoem a forma numerica compartilhada
- `POWER`
- `RADIANS`
- `ROUND`
- `SIGN`
- `SQRT`
- `SQUARE` no SQL Server e no SQL Azure

## Inventario De Pendencias

### SQL Server / SQL Azure

- Nenhuma pendencia registrada.

### MySQL

- Nenhuma pendencia registrada.

### MariaDB

- Nenhuma pendencia registrada.

### Npgsql

- Nenhuma pendencia registrada.

### Db2

- Nenhuma pendencia registrada.

### Firebird

- Nenhuma pendencia registrada.

### Oracle

- Nenhuma pendencia registrada.

### SQLite

- Nenhuma pendencia registrada.

## Proxima Rodada

- Separar as funcoes por grupo de sintaxe para reaproveitar o mesmo contrato entre providers que compartilham o mesmo nome nativo.
- Criar testes de fidelidade dedicados para os grupos que ainda estao faltando.
- Atualizar o dialeto compartilhado so quando a funcao existir no banco real e o teste puder ser executado sem false positive.
