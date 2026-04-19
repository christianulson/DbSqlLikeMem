# Fase 7 - Exceções e mensagens

## Status

IN PROGRESS

## Percentual de entrega

100%

## O que foi feito

- Adicionados casos de excecao transacional nas estrategias de MariaDB, MySQL, Firebird, Db2, Npgsql, SQLite, Oracle, SQL Server e SQL Azure para validar savepoint inexistente, savepoint sem transacao ativa, rollback sem transacao ativa e validacao de nome em branco.
- Adicionado um teste de parser no SQL Server para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no MySQL para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no Npgsql para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no SQLite para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no Db2 para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no Oracle para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no MariaDB para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionado um teste de parser no Firebird para validar a mensagem de parametro quando o SQL recebido esta em branco.
- Adicionados testes de savepoint com nome em branco no MySQL para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no MariaDB para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no SQL Azure para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no Oracle para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no Firebird para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no Db2 para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no Npgsql para validar a mensagem de validacao de parametro existente.
- Adicionados testes de savepoint com nome em branco no SQLite para validar a mensagem de validacao de parametro existente.

## Próximos passos

- Levantar exceções reais por cenário.
- Padronizar enriquecimento de debug sem mudar o gatilho.
- Revisar mensagens dependentes de parser, parâmetro e execução.
