# Benchmark Structure Plan

Objetivo: alinhar a estrutura e o cadastro dos benchmarks com estas regras:
- rodar somente benchmarks suportados por cada banco real;
- manter benchmarks de comportamento/fidelidade em outra camada da aplicacao;
- evitar falsos positivos na validacao do catalogo;
- preservar a superficie publica dos benchmarks que ja existem.

## Estado atual

- `Sequence` ja foi separado da base comum e esta em uma base propria.
- `MySql` e `Sqlite` nao herdam mais benchmarks de sequence.
- O validador do catalogo agora resolve aliases por IL e nao depende mais de nome igual ao `BenchmarkFeatureId`.
- O catalogo estatico dos wrappers esta coerente com o `FeatureCatalog`: nao ha features desconhecidas nem features faltando no mapeamento.
- A hierarquia de sequence esta coerente com o suporte de cada provedor.

## Plano de correcao

- [x] Ajustar o validador para aceitar aliases de metodo que apontam para o mesmo `BenchmarkFeatureId`.
- [x] Confirmar que benchmarks mock-only e de diagnostico estao modelados na hierarquia correta.
- [x] Verificar se algum provider ainda expõe benchmark nao suportado por heranca.
- [x] Atualizar o backlog com o que foi corrigido e o que permanecer pendente.

## Historico

- 2026-05-01: criado apos a separacao da base de sequence e a identificacao de falsos positivos no catalogo.
- 2026-05-01: o validador foi ajustado para resolver aliases reais dos wrappers de benchmark.
- 2026-05-01: a validacao estatica confirmou o mapeamento completo dos wrappers e a heranca correta da suite de sequence.
