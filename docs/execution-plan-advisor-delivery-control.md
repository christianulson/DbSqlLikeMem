# Controle de Entrega — Execution Plan Advisor

## Objetivo da entrega
Evoluir incrementalmente o Execution Plan Advisor para aumentar valor ao desenvolvedor sem quebra de contrato textual de `PlanWarnings` e sem regressão em `IndexRecommendations`.

## Itens da entrega (rodadas contínuas)
- [x] Criar controle de entrega em documento dedicado.  
  **Progresso do item:** 100%
- [x] Implementar `PlanRiskScore` agregado quando há warnings.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanRiskScore` com testes unitários e de integração base.  
  **Progresso do item:** 100%
- [x] Implementar `PlanWarningSummary` (resumo determinístico por severidade/código).  
  **Progresso do item:** 100%
- [x] Cobrir `PlanWarningSummary` com testes unitários e de integração base.  
  **Progresso do item:** 100%
- [x] Implementar `PlanPrimaryWarning` (warning mais prioritário por severidade/código).  
  **Progresso do item:** 100%
- [x] Cobrir `PlanPrimaryWarning` com testes unitários e de integração base.  
  **Progresso do item:** 100%
- [x] Implementar `IndexRecommendationSummary` agregado para síntese de recomendações de índice.  
  **Progresso do item:** 100%
- [x] Cobrir `IndexRecommendationSummary` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Implementar `PlanWarningCounts` para visão agregada por severidade (`high`, `warning`, `info`).  
  **Progresso do item:** 100%
- [x] Cobrir `PlanWarningCounts` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Adicionar `PlanMetadataVersion` para versionamento explícito dos metadados agregados do plano.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanMetadataVersion` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Implementar `IndexPrimaryRecommendation` para destacar a recomendação de índice mais prioritária.  
  **Progresso do item:** 100%
- [x] Cobrir `IndexPrimaryRecommendation` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Implementar `PlanFlags` para sinalização rápida de presença de warnings e recomendações de índice.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanFlags` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Implementar `PlanPerformanceBand` para categorização rápida de latência do plano (`Fast|Moderate|Slow`).  
  **Progresso do item:** 100%
- [x] Cobrir `PlanPerformanceBand` com testes unitários e integração base.  
  **Progresso do item:** 100%
- [x] Atualizar plano principal (`docs/p7-p10-implementation-plan.md`) com decisões/checklist da rodada atual.  
  **Progresso do item:** 100%

## Percentual geral da entrega
- **0%** — planejamento iniciado.
- **20%** — estrutura de controle criada.
- **45%** — `PlanRiskScore` implementado.
- **65%** — cobertura de `PlanRiskScore` adicionada.
- **80%** — `PlanWarningSummary` implementado.
- **90%** — cobertura de `PlanWarningSummary` adicionada.
- **95%** — `PlanPrimaryWarning` implementado.
- **98%** — cobertura de `PlanPrimaryWarning` adicionada.
- **99%** — `IndexRecommendationSummary` implementado e coberto.
- **99.5%** — `PlanWarningCounts` implementado e coberto.
- **99.8%** — `PlanMetadataVersion` implementado e coberto.
- **99.9%** — `IndexPrimaryRecommendation` implementado e coberto.
- **99.95%** — `PlanFlags` implementado e coberto.
- **99.97%** — `PlanPerformanceBand` implementado e coberto.
- **100%** — documentação consolidada e plano principal atualizado.

## Escopo e segurança
- Sem refatoração agressiva.
- Sem alteração da ordem do contrato textual interno de cada warning (`Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`, `MetricName`, `ObservedValue`, `Threshold`).
- `Threshold` técnico parseável preservado (`key:value;key:value`).
- `IndexRecommendations` preservado.
- Novos campos agregados (`PlanMetadataVersion`, `PlanFlags`, `PlanPerformanceBand`, `PlanRiskScore`, `PlanWarningSummary`, `PlanWarningCounts`, `PlanPrimaryWarning`, `IndexRecommendationSummary`, `IndexPrimaryRecommendation`) adicionados de forma incremental e backward-compatible no output textual.


## Referência de backlog
- Backlog completo de próximas evoluções: `docs/execution-plan-advisor-backlog.md`.
