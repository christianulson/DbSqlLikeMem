# Benchmark Manual Runbook

Guia rapido para validar e publicar os benchmarks de forma manual.

## Smoke

- Executar `Scripts/validate-catalog-and-maps.ps1`.
- Confirmar que os mapas JSON e o catalogo estruturado seguem parseando.
- Rodar o perfil `smoke` quando for preciso validar o fluxo completo sem custo alto.

## Core

- Rodar `Scripts/run-core-matrix.ps1`.
- Usar este perfil quando precisar da matriz essencial para validacao manual.

## Full

- Subir os bancos com `Scripts/start-benchmark-databases.ps1`.
- Rodar `Scripts/run-benchmarks-preprovisioned.ps1` com o filtro apropriado.
- Comparar com o baseline usando `Scripts/compare-benchmark-baseline.ps1`.
- Gerar a wiki com `Scripts/export-wiki-all.ps1`.
- Publicar artefatos apenas depois de validar que a execucao terminou com sucesso.

## Regras

- Nao publicar artefatos se a execucao falhar ou gerar resultado incompleto.
- Separar falha de infraestrutura de falha de benchmark.
- Registrar no historico qualquer mudanca que altere o fluxo manual.
