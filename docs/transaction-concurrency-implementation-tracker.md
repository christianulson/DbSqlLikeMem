# Transaction Concurrency - Plano de Implementação e Acompanhamento

Este documento acompanha a evolução da confiabilidade de **paralelismo e concorrência transacional** dos testes Dapper no projeto.

## Objetivo
Consolidar e evoluir testes de concorrência entre provedores (MySql, Npgsql, SqlServer, Oracle, Db2, Sqlite), reduzindo duplicação e aumentando cobertura de cenários críticos de transações.

---

## Status geral
- Progresso total: **35%**
- Última atualização: **2026-02-26**
- Responsável: Time de qualidade/contribuidores do core

---

## Fases e progresso

### 1) Hardening da base compartilhada — **100%**
- [x] Consolidar helpers comuns em `DapperTransactionConcurrencyTestsBase`
- [x] Padronizar criação de conexões por provedor via factory
- [x] Refatorar suites por provedor para wrappers finos

**Resultado esperado:** base única com menor duplicação e manutenção simplificada.

---

### 2) Isolamento e visibilidade de leitura — **0%**
- [ ] `ReadOutsideTransaction_ShouldNotSeeUncommittedChanges`
- [ ] `ReadAfterCommit_ShouldSeeCommittedChanges`
- [ ] Cobertura por versão usando `MemberData*Version`

**Resultado esperado:** proteção contra dirty read e garantia de visibilidade após commit.

---

### 3) Savepoint concorrente e rollback parcial — **0%**
- [ ] Cenário concorrente com `Save` + `Rollback(savepoint)`
- [ ] Assert de estado final com interferência entre conexões
- [ ] Variante com `Release(savepoint)` quando aplicável

**Resultado esperado:** previsibilidade em rollback parcial sob concorrência.

---

### 4) Conflito de escrita/PK sob concorrência — **0%**
- [ ] Múltiplas tasks inserindo mesma PK
- [ ] Garantir integridade final (1 linha válida)
- [ ] Documentar comportamento esperado de exceção por provedor

**Resultado esperado:** robustez em cenários de disputa de escrita.

---

### 5) Stress controlado (N writers) — **0%**
- [ ] Parametrizar número de workers (8/16/32)
- [ ] Incrementos concorrentes no mesmo registro
- [ ] Assert determinístico do valor final

**Resultado esperado:** identificação de race conditions sob maior pressão.

---

### 6) Governança e observabilidade — **10%**
- [x] Definir trilha de evolução por fases
- [ ] Criar traits específicas por categoria de concorrência
- [ ] Definir execução CI (smoke x completo)
- [ ] Documentar política de execução e aceitação

**Resultado esperado:** evolução contínua previsível e com visibilidade.

---

## Backlog priorizado (próximos passos)
1. Implementar testes de visibilidade transacional (fase 2)
2. Implementar cenário de savepoint concorrente (fase 3)
3. Implementar conflito de PK concorrente (fase 4)
4. Adicionar stress parametrizado por workers (fase 5)

---

## Critérios de aceite por fase
- Testes sem duplicação de lógica entre provedores
- Nomenclatura e `summary` seguindo padrão existente
- Sem regressão nas suites atuais de transaction reliability
- Execução estável (sem flaky) nas rotinas de CI definidas

---

## Histórico de atualizações
- **2026-02-26**: criação inicial do tracker com fases, percentuais e backlog.
