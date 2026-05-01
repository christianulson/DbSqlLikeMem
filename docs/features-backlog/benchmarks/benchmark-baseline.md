# Benchmark Baseline and Regression

Poltica de baselines e comparacao historica para resultados publicados do benchmark.

## Armazenamento sugerido

- `docs/Wiki/BenchmarkResults/history`
- `BenchmarkDotNet.Artifacts/history`

## Arquivos de baseline por perfil

- `baseline-smoke.json`
- `baseline-core.json`
- `baseline-full.json`

## Chave de comparacao

- `BenchmarkStableId`
- `ProviderId`
- `Engine`
- `Profile`

## Estrutura minima do baseline

- `providerId`
- `engine`
- `profile`
- `entries`
- cada entrada em `entries` deve informar:
  - `methodName`
  - `meanMicroseconds`
  - `status` opcional

## Campos recomendados por entrada

- identificador estavel da feature
- identificador do provedor
- engine
- perfil
- media
- margem de erro
- ratio
- numero de iteracoes
- status estruturado
- timestamp UTC

## Regras de comparacao

- So compare resultados com o mesmo `BenchmarkStableId`, `ProviderId`, `Engine` e `Profile`.
- Use a versao do provider no catalogo como contexto operacional, nao como substituto do ID estavel.
- Mudancas estruturais nao quebram a comparacao historica se o `BenchmarkStableId` continuar igual.
- `smoke` serve para validacao rapida e nao para baseline principal.
- `core` serve para comparacao manual recorrente.
- `full` serve para comparacao completa.
- `diagnostic` serve para investigacao, nao para baseline.

## Atualizacao do baseline

- Atualizar o baseline somente quando a mudanca representar uma nova referencia aceita.
- Registrar a razao da troca no historico de features ou no changelog operacional.
- Evitar sobrescrever baselines sem manter a trilha da execucao anterior.

## Pendencias de automacao

- `Scripts/compare-benchmark-baseline.ps1`.
- Resumo publicado junto com a wiki em `docs/Wiki/BenchmarkResults/benchmark-regression-summary.md`.
- Thresholds iniciais de regressao automatizada.
