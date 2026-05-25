# Fase 6 - Transações e savepoints

## Status

DONE

## Percentual de entrega

100%

## O que foi feito

- Padronizado o contrato base de `ProviderSqlDialect` para savepoints e parametros especiais, com XML docs EN/PT consistentes.
- Mapeado o contrato de `SupportsReleaseSavepoints` por provider: SQL Server, SQL Azure e Oracle desabilitam release; Firebird habilita; os demais herdam o comportamento padrão do dialect base.
- Alinhada a documentação de savepoints ao fluxo transacional compartilhado, em vez de limitar o texto ao benchmark.
- Ajustada a documentação dos comandos `SAVEPOINT`, `ROLLBACK TO SAVEPOINT` e `RELEASE SAVEPOINT` para o mesmo fluxo transacional compartilhado.
- Documentadas as implementações concretas de `ReleaseSavepoint` no SQL Server e no Oracle para deixar explícita a intenção do retorno por provider.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa na suíte de estratégia do SQL Azure, cobrindo o gatilho comum antes da validação de capability.
- Criada a suíte de estratégia de transações do Firebird com rollback para savepoint e release sem transação ativa.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa no Db2, fechando mais uma lacuna de comportamento transacional compartilhado.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa no MySQL, alinhando o gatilho comum ao restante dos providers.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa no Npgsql, mantendo o mesmo comportamento acionável.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa no SQLite, fechando a cobertura homóloga entre providers principais.
- Adicionado o caso de `ReleaseSavepoint` sem transação ativa no Oracle, completando o mesmo gatilho comum na suíte do provider.
- Adicionado o caso de `ReleaseSavepoint` não suportado no Oracle com transação ativa, cobrindo o gatilho de capability da API.
- Adicionado o caso de savepoints aninhados no MySQL para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no SQLite para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no Npgsql para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no SQL Server para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados na MariaDB para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no Firebird para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no Db2 para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no Oracle para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados no SQL Azure para validar rollback para o snapshot externo selecionado.
- Adicionado o caso de savepoints aninhados nas suítes Dapper de SQL Server, Db2, Firebird, MySQL, Npgsql, Oracle e SQLite para validar rollback para o snapshot externo selecionado.
- Criada a suíte de contrato do dialect Oracle para documentar `SupportsReleaseSavepoints` e o SQL emitido para release-savepoint.
- Criada a suíte de contrato do dialect SQL Server para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect Db2 para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect MySQL para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect SQLite para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect Npgsql para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect SQL Azure para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de contrato do dialect MariaDB para documentar `SupportsReleaseSavepoints` e o SQL emitido para savepoint/release-savepoint.
- Criada a suíte de estratégia de transações do MariaDB com commit, rollback, rollback para savepoint e release sem transação ativa.

## Próximos passos

- Mapear contratos por provider.
- Validar savepoints e nested flow.
- Confirmar mensagens e exceções esperadas.
