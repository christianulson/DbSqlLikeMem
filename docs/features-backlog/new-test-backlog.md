# Plano de refatoracao da suite de testes e benchmarks em services compartilhados

Este backlog descreve uma refatoracao maior da organizacao atual de testes e benchmarks.

Hoje existem testes de provider, testes de integracao, benchmarks e suportes duplicados em varios projetos. A proposta e unificar a logica comum em `services` dentro dos projetos `TestTools`, para que o mesmo fluxo possa ser executado em tres modos:

1. Teste + cobertura, usando o `src\.runsettings` padrao e sem parametro adicional (IS_REAL_DB_TEST).
2. Teste sem cobertura contra a base real em docker, usando parametro (IS_REAL_DB_TEST) no `src\real-db.runsettings` (já criado).
3. Benchmark em mock e base real, comparando os dois resultados via script que monta a matriz atual.

Com isso, o projeto ganha:

- mais velocidade para desenvolvimento e criacao de novos testes
- mais fidelidade de comportamento contra a base real
- visibilidade de performance contra a base real

## Objetivo da refatoracao

Centralizar o comportamento repetido em services compartilhados e reduzir a dependencia de implementacoes duplicadas em projetos de provider, testes e benchmarks.

O foco nao e apenas mover codigo, mas padronizar a forma de executar os mesmos cenarios em contextos diferentes.

## Regra principal de separacao

O que for compartilhado entre teste e benchmark deve viver em `TestTools`.

O que for comportamento especifico de validacao continua no projeto de `Test`.

O que for medicao de tempo continua no projeto de `Benchmark`.

## Modos de execucao esperados

### 1. Teste + cobertura

Usa o `src/.runsettings` padrao.

Caracteristicas:

- sem parametro (IS_REAL_DB_TEST)) no `.runsettings`
- roda rapido
- prioriza cobertura e feedback curto
- valida comportamento do mock e da camada de execucao local

### 2. Teste sem cobertura contra base real

Usa o mesmo fluxo de teste, mas com parametro (IS_REAL_DB_TEST) no `src\real-db.runsettings` para ativar a execucao contra a base real em docker.

Caracteristicas:

- sem cobertura de codigo
- conecta na base real quando o parametro estiver habilitado
- serve para validar fidelidade de comportamento
- nao substitui a suite rapida, apenas complementa

### 3. Benchmark mock + base real

O benchmark deve executar a suite mock e a suite com base real, e depois comparar os resultados por meio de script que monta a matriz, como ja acontece hoje.

Caracteristicas:

- mede o mock e a base real
- permite comparacao objetiva entre os dois caminhos
- reutiliza a mesma logica de cenarios sempre que possivel
- nao deve incluir suites que nao fazem sentido em performance real e testes que não são ponta a ponta como os de parser

## Estrutura de organizacao

A organizacao dos projetos deve seguir areas funcionais, para evitar bagunça e facilitar evolucao.

### Areas sugeridas

- `DML`
- `DDL`
- `Query`
- `Transactions`
- `StoredProcedures`
- `Views`
- `TemporaryTable`
- `Performance`
- `Schema`
- `Interception`

### Regra de pastas

- Se o projeto crescer dentro de uma area, criar subpastas por necessidade.
- Evitar colocar tudo no nivel raiz do projeto.
- Manter a mesma ideia de organizacao entre `TestTools`, `Test` e `Benchmark` quando a area existir nos tres.

## Escopo dos TestTools

Os projetos `TestTools` devem concentrar a logica compartilhada, principalmente em forma de services reutilizaveis.

Exemplos do que deve viver em `TestTools`:

- construcao de cenarios
- montagem de schema e seed
- execucao de comandos e consultas
- comparacao de saidas
- helpers de workflow para mock e base real
- adaptadores para benchmark reaproveitarem o mesmo fluxo

## Escopo dos projetos de Test

Os projetos `Test` continuam sendo o local das validacoes de comportamento.

Devem permanecer aqui:

- testes de provider
- testes de parser
- testes de Dapper
- testes de ORMs
- testes de validacao de comportamento do mock
- testes que dependem de construcao por codigo e nao por script SQL, quando esse for o caso

Nao devem ser migrados para a nova estrutura os testes que dependem de `Seed`, porque eles nao conseguem ser recriados no banco real em docker com o mesmo fluxo.

### Regra de real database

Esses testes devem obrigatoriamente bater na base real em docker.
Devem ser chamados após cada testes de compativilidade e comparar o valor do resultado do Mock versos o da base real.

### Regra de benchmark

Esses testes nao devem ir para o benchmark se forem:

- testes de parser
- testes de Dapper
- testes de ORMs
- testes de Linq
- testes cuja montagem do mock seja feita por codigo e nao por script SQL

## Escopo dos Benchmarks

Os benchmarks devem conter somente cenarios que tragam valor real de comparacao de performance.

Devem entrar:

- cenarios DML e DDL que possam ser repetidos entre mock e base real
- fluxos de comparacao entre engine mock e engine real
- casos que tenham script ou montagem equivalente para os dois lados

Nao devem entrar:

- parser
- Dapper
- ORMs
- Linq
- cenarios que existam apenas por construcao de mock em codigo
- testes cuja montagem dependa de `Seed` e nao tenha equivalente reproduzivel no banco real
- casos sem equivalente util na base real

## Direcao funcional

### Para os testes

- manter o caminho rapido como default
- mover a logica comum para services em `TestTools`
- deixar os testes como consumidores desses services
- separar por area funcional para facilitar crescimento

### Para a base real

- ativar por parametro (IS_REAL_DB_TEST) no `src\real-db.runsettings`
- usar os containers e conexoes ja suportados
- manter a execucao sem cobertura
- preservar a fidelidade de comportamento por provider

### Para os benchmarks

- reutilizar os mesmos services sempre que possivel
- executar mock e base real
- comparar por matriz
- manter o benchmark focado em comportamento e performance, e em cobertura de senários SQL e não de código

## Fases de implementacao

### Fase 1 - Definir a arquitetura compartilhada

- Mapear quais cenarios hoje estao duplicados entre `Test`, `TestTools` e `Benchmark`.
- Definir o contrato dos services compartilhados.
- Definir a classificacao das areas funcionais.
- Separar claramente o que fica em `TestTools`, `Test` e `Benchmark`.
- Lembrando que temos TestTools Global e TestTools por provider.

### Fase 2 - Extrair services para `TestTools`

- Criar os services por area funcional.
- Mover a montagem de cenarios repetidos para esses services.
- Garantir que os services suportem mock e base real quando fizer sentido.
- Evitar duplicacao entre providers.

### Fase 3 - Adaptar os projetos de `Test`

- Trocar implementacoes repetidas pelo uso dos services.
- Manter os testes de parser, Dapper e ORMs fora do caminho de base real.
- Manter testes de comportamento do mock no fluxo rapido.
- Organizar as pastas por area funcional.

### Fase 4 - Adaptar os projetos de `Benchmark`

- Fazer os benchmarks consumirem os mesmos services.
- Remover do benchmark tudo que nao tiver valor de comparacao real.
- Garantir que mock e base real gerem resultados comparaveis.
- Ajustar a matriz de execucao para refletir os dois lados.

### Fase 5 - Ajustar runsettings e scripts

- Usar parametro para ativar a base real em docker.
- Garantir que o benchmark continue gerando a matriz comparativa.

### Fase 6 - Documentar o novo fluxo

- Atualizar os documentos de estrategia de teste.
- Explicar quando usar cada modo.
- Explicar quais suites entram em `Test`, `TestTools` e `Benchmark`.
- Registrar as regras de exclusao de parser, Dapper, ORMs e mocks por codigo do benchmark.

### Fase 7 - Validar a reorganizacao

- Confirmar que a suite rapida continua independente de base externa.
- Confirmar que a execucao real fica opt-in.
- Confirmar que os benchmarks comparam mock e base real.
- Confirmar que a nova organizacao por areas nao quebrou a descoberta dos testes.

## Critérios de aceite

- A logica comum relevante para teste e benchmark esta concentrada em `TestTools`.
- Os projetos de `Test` usam services compartilhados e ficam organizados por area funcional.
- Os projetos de `Benchmark` reutilizam os mesmos services e executam mock + base real.
- `src/.runsettings` continua sendo o caminho default para teste com cobertura.
- A base real em docker so roda quando o parametro apropriado estiver habilitado.
- Parser, Dapper e ORMs nao entram no benchmark nem no fluxo de base real obrigatorio.
- Testes com mock montado por codigo nao sao movidos para benchmark.
- A matriz de benchmark continua comparando mock e base real.

## Importante

- inicialmente pegue 1 arquivo e 1 método para deixar no novo padrão, depois de validarmos vamos seguir com as alterações do restante.
- informe aqui os percentuais de implantação ao final de cada interação.
- documente onde parou após cada atividade e no inicio da apróxima veja de onde deve continuar.
- após cada migração remova o teste/benckmark atual.

# Arquivos Modelo

## Base

- Sem Retorno: src\DbSqlLikeMem.TestTools\DDL\TableServicesTest.cs
- Com Retorno: src\DbSqlLikeMem.TestTools\Query\SelectServiceTest.cs

## TestBase

- Sem Comparativo de Dados: src\DbSqlLikeMem.TestTools\Tests\DDL\TableTestsBase.cs
- Com Comparativo de Dados: src\DbSqlLikeMem.TestTools\Tests\Query\SelectTestsBase.cs

## Test Dialect por Provider

- src\DbSqlLikeMem.MySql.TestTools\MySqlProviderSqlDialect.cs (migrado de benchmark\DbSqlLikeMem.Benchmarks\Benchmarks\Dialects\MySqlDbDialect.cs)

## Test Fidelity
- src\DbSqlLikeMem.MySql.Test\Fidelity\DDL\TableTests.cs
- src\DbSqlLikeMem.MySql.Test\Fidelity\Query\SelectTests.cs