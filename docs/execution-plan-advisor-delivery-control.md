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
- [x] Implementar `PlanQualityGrade` (A/B/C/D) derivado de `PlanRiskScore` + `PlanPerformanceBand`.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanQualityGrade` com testes unitários e integração base (presença/ausência + thresholds).  
  **Progresso do item:** 100%
- [x] Implementar `PlanTopActions` (Top 3 ações) derivado de warnings/recomendações com ordenação determinística.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanTopActions` com testes unitários e integração base (presença/ausência + limite de 3).  
  **Progresso do item:** 100%
- [x] Implementar `PlanNoiseScore` para quantificação de redundância de warnings.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanNoiseScore` com testes unitários e integração base (presença/ausência + matriz PW004/PW005).  
  **Progresso do item:** 100%
- [x] Implementar `PlanCorrelationId` por execução de plano.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanCorrelationId` com testes de presença e formato técnico estável.  
  **Progresso do item:** 100%
- [x] Criar suíte de contrato textual para agregados (`Plan*`, `Index*Summary`) com assertiva estável em testes de formatter.  
  **Progresso do item:** 100%
- [x] Consolidar padrões parseáveis dos agregados em seção única de documentação.  
  **Progresso do item:** 100%
- [x] Definir política semântica de versionamento para `PlanMetadataVersion`.  
  **Progresso do item:** 100%
- [x] Garantir i18n dos labels agregados `Plan*`/`Index*` sem traduzir tokens técnicos canônicos.  
  **Progresso do item:** 100%
- [x] Implementar payload JSON opcional do plano para campos agregados comuns.  
  **Progresso do item:** 100%
- [x] Cobrir equivalência texto vs JSON para campos comuns e ausência de campos derivados sem warnings.  
  **Progresso do item:** 100%
- [x] Implementar `PlanPrimaryCauseGroup` para agrupamento da causa principal por warning primário.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanPrimaryCauseGroup` com testes de presença/ausência e equivalência com payload JSON opcional.  
  **Progresso do item:** 100%
- [x] Implementar `PlanDelta` para comparação opcional entre execução atual e snapshot anterior.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanDelta` com testes de presença/ausência e cenário controlado de delta.  
  **Progresso do item:** 100%
- [x] Implementar `PlanSeverityHint` com contexto escalável (`dev|ci|prod`) sem quebrar defaults.  
  **Progresso do item:** 100%
- [x] Cobrir `PlanSeverityHint` com testes de default e override de contexto.  
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
- **99.99%** — `PlanQualityGrade` implementado e coberto.
- **99.995%** — `PlanTopActions` implementado e coberto.
- **99.998%** — `PlanNoiseScore` implementado e coberto.
- **99.999%** — `PlanCorrelationId` implementado e coberto.
- **100%** — suíte de contrato textual, i18n, payload JSON, deltas/hints contextuais, evidências e governança de contrato consolidados.

## Escopo e segurança
- Sem refatoração agressiva.
- Sem alteração da ordem do contrato textual interno de cada warning (`Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`, `MetricName`, `ObservedValue`, `Threshold`).
- `Threshold` técnico parseável preservado (`key:value;key:value`).
- `IndexRecommendations` preservado.
- Novos campos agregados (`PlanMetadataVersion`, `PlanCorrelationId`, `PlanFlags`, `PlanPerformanceBand`, `PlanRiskScore`, `PlanQualityGrade`, `PlanWarningSummary`, `PlanWarningCounts`, `PlanNoiseScore`, `PlanTopActions`, `PlanPrimaryWarning`, `PlanPrimaryCauseGroup`, `PlanDelta`, `PlanSeverityHint`, `IndexRecommendationSummary`, `IndexPrimaryRecommendation`, `IndexRecommendationEvidence`) adicionados de forma incremental e backward-compatible no output textual.


## Referência de backlog
- Backlog completo de próximas evoluções: `docs/execution-plan-advisor-backlog.md`.


## Padrões parseáveis consolidados
- `PlanMetadataVersion: <int>`
- `PlanCorrelationId: <32hexLower>`
- `PlanFlags: hasWarnings:<true|false>;hasIndexRecommendations:<true|false>`
- `PlanPerformanceBand: <Fast|Moderate|Slow>`
- `PlanRiskScore: <0..100>`
- `PlanQualityGrade: <A|B|C|D>`
- `PlanWarningSummary: <Code>:<Severity>[;<Code>:<Severity>...]`
- `PlanWarningCounts: high:<n>;warning:<n>;info:<n>`
- `PlanNoiseScore: <0..100>`
- `PlanTopActions: <code>:<actionKey>[;<code>:<actionKey>...]`
- `PlanPrimaryWarning: <Code>:<Severity>`
- `IndexRecommendationSummary: count:<n>;avgConfidence:<n.nn>;maxGainPct:<n.nn>`
- `IndexPrimaryRecommendation: table:<name>;confidence:<n>;gainPct:<n.nn>`

## Política semântica de versionamento (`PlanMetadataVersion`)
- Incrementar **major** (ex.: `1 -> 2`) quando houver quebra de parsing esperado para consumidores (remoção/renomeação de chave, mudança de formato canônico).
- Incrementar **minor** via documentação/checklist quando houver apenas adição backward-compatible de novos agregados sem alterar formato dos existentes.
- Não alterar retroativamente o significado de chaves canônicas já publicadas.
- Garantir teste de contrato textual para novos agregados antes de promover versão.
