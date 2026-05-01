# Benchmark Execution Status

Guia de leitura dos resultados estruturados publicados pelo benchmark.

## Campos estruturados principais

- `benchmarkStableId`: identificador estavel usado para comparar resultados ao longo do tempo.
- `runId`: identificador de correlacao da execucao publicada.
- `benchmarkFeatureId`: identificador enum da feature executada.
- `providerId`: identificador do provedor.
- `engine`: etiqueta do engine usado na execucao.
- `suiteName`: nome publico da suite.
- `methodName`: nome do metodo publico executado.
- `category`: categoria logica do benchmark.
- `environment`: bloco com perfil, runtime e dados basicos da execucao.
- `status`: resultado estruturado da linha exportada.
- `meanMicroseconds`: media principal da execucao.
- `errorMicroseconds`: margem de erro quando disponivel.
- `ratio`: relacao entre resultados quando a comparacao publica usa uma referencia.
- `iterationCount`: quantidade de iteracoes medidas.
- `tags`: classificacao adicional para consumo downstream.

## Status oficiais

- `Succeeded`: a linha foi executada e exportada com sucesso.
- `Skipped`: a linha foi omitida de forma intencional por perfil, filtro ou politica de execucao.
- `NotSupported`: o provider ou o dialeto nao suporta a feature.
- `Failed`: a execucao falhou de forma inesperada.

## Regras de interpretacao

- `NotSupported` nao deve ser tratado como falha.
- `Skipped` nao deve ser tratado como problema de suporte.
- `Failed` deve apontar para uma falha real de execucao ou infraestrutura.
- As comparacoes historicas devem usar `benchmarkStableId + providerId + engine + profile`.

## Leitura publica

- A wiki mostra o status na tabela para evitar confusao entre suporte ausente e falha.
- O benchmark mede performance e comportamento de execucao, nao fidelidade funcional completa.
