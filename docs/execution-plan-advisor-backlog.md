# Backlog Completo — Execution Plan Advisor

## Objetivo
Organizar as próximas evoluções do Execution Plan Advisor em ondas incrementais, preservando:
- contrato textual interno dos warnings (`Code`, `Message`, `Reason`, `SuggestedAction`, `Severity`, `MetricName`, `ObservedValue`, `Threshold`);
- `Threshold` técnico parseável;
- comportamento de `IndexRecommendations`.

## Estado atual (já entregue)
- [x] `PlanMetadataVersion`
- [x] `PlanFlags`
- [x] `PlanPerformanceBand`
- [x] `PlanRiskScore`
- [x] `PlanWarningSummary`
- [x] `PlanWarningCounts`
- [x] `PlanPrimaryWarning`
- [x] `IndexRecommendationSummary`
- [x] `IndexPrimaryRecommendation`

---

## Backlog Priorizado

### Onda 1 — Alto valor / baixo risco
1. **PlanQualityGrade (A/B/C/D)**
   - Descrição: adicionar grade qualitativa derivada de `PlanRiskScore` + `PlanPerformanceBand`.
   - Valor: leitura rápida por humanos e dashboards.
   - Risco: baixo (aditivo).
   - Critério de pronto:
     - campo emitido em formato estável: `PlanQualityGrade: <A|B|C|D>`;
     - testes de presença/ausência e thresholds.

2. **PlanTopActions (Top 3 ações)**
   - Descrição: emitir até 3 ações prioritárias derivadas de warnings/recomendações.
   - Valor: reduz tempo até correção prática.
   - Risco: baixo/médio (ordenação determinística).
   - Critério de pronto:
     - formato parseável: `PlanTopActions: <code>:<actionKey>;...`;
     - sem alterar `SuggestedAction` original dos warnings.

3. **PlanNoiseScore**
   - Descrição: score de ruído para quantificar redundância de warnings.
   - Valor: evitar sobrealerta em cenários com múltiplos sinais semelhantes.
   - Risco: médio.
   - Critério de pronto:
     - fórmula documentada;
     - testes de regressão com matriz `PW004/PW005`.

### Onda 2 — Automação/observabilidade
4. **Payload JSON opcional do plano**
   - Descrição: expor representação estruturada do plano além do texto.
   - Valor: integração robusta com CI, IDE e observabilidade.
   - Risco: médio (serialização e compat).
   - Critério de pronto:
     - propriedade/retorno opcional JSON;
     - teste de equivalência texto vs JSON para campos comuns.

5. **CorrelationId por execução de plano**
   - Descrição: ID único para rastrear plano em logs/pipelines.
   - Valor: troubleshooting e auditoria.
   - Risco: baixo.
   - Critério de pronto:
     - campo `PlanCorrelationId` estável;
     - teste de presença e formato.

6. **PlanDelta (comparação entre execuções)**
   - Descrição: destacar mudança de risco/custo entre última execução e atual.
   - Valor: alerta de regressão de performance.
   - Risco: médio/alto (estado histórico).
   - Critério de pronto:
     - delta opcional quando histórico existir;
     - testes com cenários controlados.

### Onda 3 — Inteligência de recomendação
7. **IndexRecommendationEvidence**
   - Descrição: evidências por recomendação (colunas de filtro, ordenação, seletividade estimada).
   - Valor: aumenta confiança do dev ao aplicar índice.
   - Risco: baixo/médio.
   - Critério de pronto:
     - campo técnico parseável com chave fixa;
     - testes de coerência com query.

8. **PlanPrimaryCauseGroup**
   - Descrição: agrupar warnings em causa primária (ex.: "ScanWithoutFilter", "SortWithoutLimit").
   - Valor: reduz complexidade cognitiva.
   - Risco: médio.
   - Critério de pronto:
     - taxonomia estável documentada;
     - testes de mapeamento por regra.

9. **Hint de severidade escalável por contexto**
   - Descrição: calibrar pesos/severidade por perfil de ambiente (dev/ci/prod).
   - Valor: sinal mais aderente ao contexto.
   - Risco: médio/alto.
   - Critério de pronto:
     - configuração opcional sem quebrar defaults;
     - testes para defaults e overrides.

---

## Itens técnicos transversais
- [ ] Consolidar padrões parseáveis dos novos agregados em seção única de documentação.
- [ ] Criar suíte de teste de contrato textual agregados (`Plan*`, `Index*Summary`) com snapshot estável.
- [ ] Garantir i18n dos novos labels sem traduzir tokens técnicos/canônicos SQL.
- [ ] Definir política semântica de versionamento para `PlanMetadataVersion`.

## Plano de execução recomendado
1. Executar Onda 1 em PRs pequenos (1 feature por PR).
2. Validar contratos textuais + não regressão de `IndexRecommendations` em todos os PRs.
3. Só iniciar Onda 2 após estabilidade das métricas agregadas em CI.
4. Revisar Onda 3 com telemetria real da adoção das ações/recomendações.
