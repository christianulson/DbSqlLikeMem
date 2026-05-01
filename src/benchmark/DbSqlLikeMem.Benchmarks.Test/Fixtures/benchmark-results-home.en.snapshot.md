Language: English | [Português (Brasil)](Home.pt-BR)

# Benchmark Results

This page is the entry point for the published benchmark matrices, the environment manifest, and the regression summary.

## What to read first

- [Comparative matrix](performance-matrix)
- [App-specific matrix](performance-matrix-app-specific)
- [Regression summary](benchmark-regression-summary)
- [Environment manifest](benchmark-run.environment.json)

## Current legend

| Mark | Meaning |
| --- | --- |
| `OK` | Executed successfully |
| `NS` | Not supported by the real provider |
| `SKIP` | Skipped by profile or rule |
| `FAIL` | Unexpected failure |
| `NOISY` | Result is unstable or noisy |

## What each page shows

- Comparative matrix: provider results against the DbSqlLikeMem mock baseline.
- App-specific matrix: mock-only coverage and provider-independent features.
- Regression summary: baseline comparison for the current run profile.
- Environment manifest: run profile, run identifier, job identifier, OS, runtime, and BenchmarkDotNet version.

## Where the raw artifacts live

- Result reports: `docs/Wiki/BenchmarkResults/results`
- Run logs: `src/benchmark/DbSqlLikeMem.Benchmarks/Logs/<RunId>/`
