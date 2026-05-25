Idioma: [English](Home) | Português (Brasil)

# Resultados de Benchmark

Esta pagina e a entrada para as matrizes publicadas, o manifesto de ambiente e o resumo de regressao.

## O que ler primeiro

- [Matriz comparativa](performance-matrix)
- [Matriz app-specific](performance-matrix-app-specific)
- [Resumo de regressao](benchmark-regression-summary)
- [Manifesto de ambiente](benchmark-run.environment.json)

## Legenda atual

| Marcador | Significado |
| --- | --- |
| `OK` | Executado com sucesso |
| `NS` | Nao suportado pelo provider real |
| `SKIP` | Ignorado por perfil ou regra |
| `FAIL` | Falha inesperada |
| `NOISY` | Resultado instavel ou com ruido |

## O que cada pagina mostra

- Matriz comparativa: resultados por provider contra o baseline mock do DbSqlLikeMem.
- Matriz app-specific: cobertura mock-only e features independentes de provider.
- Resumo de regressao: comparacao com baseline para o perfil corrente.
- Manifesto de ambiente: perfil, identificador da execucao, identificador do job, SO, runtime e versao do BenchmarkDotNet.

## Onde ficam os artefatos brutos

- Relatorios de resultado: `docs/Wiki/BenchmarkResults/results`
- Logs da execucao: `src/benchmark/DbSqlLikeMem.Benchmarks/Logs/<RunId>/`
