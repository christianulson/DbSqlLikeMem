# Fase 2 - Expandir os testes de tipo DbParameter

## Status

DONE

## Percentual de entrega

100%

## O que foi feito

- Expandimos o contrato compartilhado de `DbMockConnectionFactory` para validar o `DbParameter` concreto de cada provider.
- Ajustamos os wrappers de SQLite, SQL Server, SQL Azure, MySQL, MariaDB, Npgsql, Oracle, Db2 e Firebird para informar seu tipo nativo de parametro.
- Mantivemos a validacao do `DbParameter` nativo ligada ao contrato real de `DbType`.

## Próximos passos

- Iniciar a Fase 3 e levar os testes relacionais para `QueryResultSnapshot` quando o contrato exigir o shape completo.
