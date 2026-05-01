# Benchmark Coherence Roadmap

Objetivo: organizar o projeto de benchmark para manter tres regras ao mesmo tempo:
- benchmarks reais so executam quando o provider real suporta a feature;
- benchmarks do mock executam em todos os providers;
- o fluxo final publica duas matrizes geradas por script: comparativo por banco e matriz do mock-only.

## Avaliacao atual

### O que esta coerente

- A separacao entre `benchmark-feature-map.json` e `benchmark-feature-map.app-specific.json` esta alinhada com a ideia de banco real versus mock-only.
- A base de `Sequence` foi extraida para `SequenceBenchmarkSuiteBase`, e os providers sem sequence nao herdam mais esse grupo.
- O validador de catalogo agora resolve aliases reais dos wrappers e nao depende mais de nome igual ao `BenchmarkFeatureId`.
- As suites de benchmark seguem um padrao consistente de nomeacao por provider e engine.
- O fluxo de geracao de wiki ja existe em dois scripts separados:
  - `Scripts/export-wiki.ps1` para a matriz comparativa;
  - `Scripts/export-wiki-app-specific.ps1` para a matriz do mock-only.

### O que ainda merece melhoria

- O arquivo `README.Benchmarks.md` ainda descreve a regra antiga de que metodo e `BenchmarkFeatureId` sao sempre iguais.
- Parte da organizacao continua muito concentrada em `BenchmarkSuiteBase` e em `Core/Prepared`, o que aumenta o risco de acoplamento quando um grupo novo precisa de suporte por provider.
- Os scripts de exportacao existem, mas ainda dependem de convencoes espalhadas e de catalogos separados sem um orquestrador unico de geracao.
- A regra de suporte por provider real existe no dominio, mas a estrutura ainda pode explicitar melhor isso no codigo e na documentacao gerada.
- A matriz final ainda pode ganhar um fluxo mais claro para produzir, em uma unica execucao, os dois artefatos publicados em `docs/Wiki`.

## Regras de dominio

- Funcionalidade contra banco real:
  - so entra na matriz comparativa se o provider real suportar a feature;
  - nao deve gerar erro indevido por executar benchmark fora do suporte do provider.
- Funcionalidade somente do mock:
  - deve ser executada para todos os providers;
  - deve aparecer na matriz do mock-only sem depender de suporte externo.
- Fidelidade funcional:
  - continua sendo validada em outra camada da aplicacao;
  - benchmark nao deve reproduzir toda a bateria de comportamento, apenas medir o caminho suportado.

## Melhorias propostas

### 1. Formalizar os dois grupos de benchmark

- Manter o catalogo em dois blocos claramente separados:
  - features comparaveis contra provider real;
  - features app-specific/mock-only.
- Evitar que uma feature mude de categoria sem registro explicito no backlog.
- Documentar o criterio de classificacao na mesma pasta do backlog de benchmarks.
- Diferenciar de forma explicita, em codigo e documentacao:
  - benchmark real suportado por provider;
  - benchmark mock-only executado em todos os providers.

### 2. Espelhar dominio entre TestTools e Benchmarks

- Avaliar mover `Core/Prepared` para uma arvore orientada a dominio em `Benchmarks/Features`.
- Espelhar a estrutura do `TestTools` quando isso reduzir atrito de manutencao:
  - `DDL`;
  - `DML`;
  - `Query`;
  - grupos de diagnostico, lifecycle e setup quando fizer sentido.
- O objetivo e deixar obvio qual benchmark corresponde a qual teste de fidelidade, sem precisar abrir varios arquivos.

### 3. Extrair bases por capacidade

- Manter `BenchmarkSuiteBase` apenas com o conjunto comum minimo.
- Criar bases intermediarias quando um grupo exigir suporte especifico:
  - sequence;
  - diagnostics/mock-only;
  - upsert/merge/returning, se a superficie continuar crescendo;
  - outros grupos que dependam de um subconjunto real de providers.
- O objetivo e reduzir heranca com heranca acidental e deixar a ausencia de suporte visivel na estrutura.

### 4. Centralizar o mapa de suporte por provider

- Tratar `benchmark-feature-map.json` como fonte de verdade para features reais.
- Tratar `benchmark-feature-map.app-specific.json` como fonte de verdade para features mock-only.
- Extrair uma politica central de execucao para responder se um benchmark pode rodar em um provider dado.
- Fazer essa politica ser consumida por:
  - suites;
  - validacao de catalogo;
  - geracao da matriz;
  - futuras rotinas de skip/abortamento gracioso.

### 5. Simplificar o comportamento das Sessions

- Manter a separacao entre:
  - `DbSqlLikeMem` mock session;
  - `External` testcontainers session;
  - `Native` quando houver engine local dedicada.
- A politica central deve ser avaliada antes da execucao do payload para evitar erro indevido em provider sem suporte.
- Quando um benchmark nao for suportado, a session deve registrar skip ou retorno controlado, nao falha inesperada.

### 6. Reorganizar a geracao das matrizes

- Criar um script orquestrador unico para gerar os artefatos:
  - um arquivo de comparativo por banco;
  - matriz do mock-only.
- O script orquestrador deve apenas chamar os geradores existentes ou futuras variantes.
- A saida comparativa deve ser quebrada em um arquivo por banco/provider, para isolar leitura e manutencao.
- A saida atual de comparativo deve virar um indice/landing page com links para cada arquivo por banco.
- O resultado do mock-only deve continuar em um arquivo unico separado em `docs/Wiki`.
- O input da geracao deve ser estruturado, preferencialmente em JSON, para evitar reprocessamento de texto livre do BenchmarkDotNet.
- O formato exato do JSON de resultados atual deve ser documentado antes de qualquer refactor maior de exportacao.

### 7. Melhorar a rastreabilidade

- Manter um backlog por pasta com:
  - item;
  - status;
  - origem do problema;
  - criterio de conclusao.
- Registrar quando uma feature muda de categoria, de base de suite ou de script gerador.

### 8. Ajustar a documentacao de entrada

- Atualizar `README.Benchmarks.md` para refletir a realidade atual:
  - wrappers podem usar alias;
  - nem toda classe publica precisa repetir a regra de mapeamento;
  - o catalogo e a base determinam a superficie real.
- Explicar em linguagem direta a diferenca entre:
  - benchmark comparativo;
  - benchmark mock-only;
  - teste de fidelidade funcional.

## Plano de execucao sugerido

### Fase 1 - Inventario

- Revisar todas as suites publicas.
- Separar o que e comparavel, o que e mock-only e o que e legado/fallback.
- Listar as classes-base que ainda concentram muito comportamento.
- Mapear os dominios que podem ser espelhados entre `TestTools` e `Benchmarks`.
- Levantar o formato exato do JSON de resultados usado hoje pelo BenchmarkDotNet e pelos scripts de exportacao.

### Fase 2 - Estrutura

- Extrair bases intermediarias onde houver grupo claro de capacidades.
- Reduzir o tamanho de `BenchmarkSuiteBase`.
- Revisar o contrato dos wrappers para manter a superficie previsivel.
- Avaliar a substituicao de `Core/Prepared` por uma arvore `Benchmarks/Features/<Dominio>`.

### Fase 3 - Catalogo e scripts

- Consolidar a regra de mapeamento dos benchmarks no catalogo.
- Criar um orquestrador unico para gerar as duas matrizes.
- Garantir que a wiki publicada continue consumindo apenas artefatos gerados.
- Definir o contrato de dados de exportacao para a geracao das matrizes.

### Fase 4 - Documentacao

- Atualizar o README do projeto de benchmarks.
- Atualizar o backlog com as decisoes estruturais finais.
- Registrar o criterio de suporte por provider para evitar regressao futura.
- Documentar a regra de execucao separada entre provider real e mock-only.

## Pontos de verificacao

- Nao pode existir benchmark real de provider executando em provider sem suporte.
- Todo benchmark mock-only deve permanecer disponivel em todos os providers.
- Todo wrapper publico deve continuar mapeado no catalogo.
- A matriz comparativa deve ser publicada em um arquivo indice mais um arquivo por banco/provider.
- A matriz mock-only deve continuar sendo gerada em um arquivo unico.
- A documentacao de entrada deve refletir a estrutura real, sem afirmar que nomes de metodo e enum sao sempre iguais.
- O caminho de exportacao deve ser previsivel e consumir dados estruturados, nao heuristicas fragilizadas por texto solto.
- A estrutura de pastas deve tornar obvio o dominio do benchmark sem abrir varios arquivos.

## Historico

- 2026-05-01: planilha criada para organizar a revisao estrutural dos benchmarks apos a separacao de sequence, o ajuste do catalogo e a definicao das duas matrizes de saida.
