# Plano de migracao do registry de funcoes, procedures e table functions por dialeto

## Objetivo

Centralizar a descoberta e a validacao de funcoes por dialeto usando os novos registries:

- `public IDictionary<string, DbScalarFunctionDef> ScalarFunctions { get; }` em cada `Dialect`.
- `internal IDictionary<string, ScalarFunctionDef> ScalarFunctions { get; }` em `SchemaMock` para funcoes customizadas.

A ideia e simples:

1. O `Dialect` informa quais funcoes aquele provider suporta e em quais versoes.
2. O `SchemaMock` complementa com funcoes customizadas do schema.
3. O parser consulta primeiro o registry do `Dialect`.
4. Se nao encontrar, consulta o registry do `SchemaMock`.
5. Se nao existir em nenhum dos dois, a funcao deve ser tratada como nao suportada ou invalida para aquele contexto.
6. A faixa de versao minima e maxima deve ficar na montagem do dicionario, nao dentro do objeto da funcao.

## Direcao arquitetural

- A ideia aqui e desinxar as peculiaridades de cada banco/versao do Projeto Core da aplicacao.
- Manter no projeto `DbSqlLikeMem` somente os corpos das funcoes compartilhadas por mais de um banco.
- Levar para cada projeto de provider o que for especifico dele.
- Fazer o mesmo padrao para procedures e table functions, usando registries equivalentes por contexto.
- Fazer o parser depender do registry para validar suporte, sem precisar de grandes cadeias de `if` e `switch`.
- Manter a validacao por versao no ponto de cadastro do dicionario, nao espalhada pelo parser.
- Usar `docs/SQL_functions` como inventario de referencia para as funcoes de cada banco e suas versoes suportadas.

## Estrutura esperada dos metadados

Os registros de funcao devem permitir, no minimo:

- nome normalizado da funcao
- aliases, se existirem
- quantidade minima e maxima de argumentos
- corpo ou evaluator da funcao
- observacoes de compatibilidade, quando necessario

A faixa de versao deve ser definida no cadastro do dicionario, e nao no objeto da funcao em si.

Para procedures e table functions, o mesmo criterio vale: o registro decide em quais versoes cada item existe, e o objeto guarda apenas o contrato e o corpo/executor.

## Ordem de resolucao no parser

Quando o parser for validar ou executar uma funcao, procedure ou table function:

1. Procurar em `Dialect.ScalarFunctions`.
2. Se nao existir, procurar em `SchemaMock.ScalarFunctions`.
3. Se nao existir em nenhum dos dois, manter apenas os caminhos legados que ainda forem realmente necessarios para builtin muito basico.
4. Se houver conflito, o `Dialect` deve ter prioridade sobre o `SchemaMock`.

O mesmo fluxo deve ser repetido para procedures e table functions quando os registries equivalentes estiverem disponiveis.

## Fases da migracao

| Fase | Entrega | Saida esperada |
| --- | --- | --- |
| 1 | Inventario completo das funcoes, procedures e table functions atuais | Lista por provider: shared, especifica e custom |
| 2 | Catalogo de corpos compartilhados no core | `DbSqlLikeMem` concentrando os corpos comuns |
| 3 | Registro por provider | Cada provider preenchendo seus registries por versao |
| 4 | Resolucao no parser | Parser consultando `Dialect` e depois `SchemaMock` |
| 5 | Limpeza dos caminhos antigos | Remocao de switches/ifs duplicados e helpers obsoletos |
| 6 | Verificacao final | Cobertura para limites de versao, aliases e custom functions |

## Estado atual

- [x] A propriedade `ScalarFunctions` foi adicionada aos objetos de `Dialect`.
- [x] A propriedade `ScalarFunctions` foi adicionada ao `SchemaMock` para funcoes customizadas.
- [ ] O mesmo padrao precisa ser aplicado para procedures.
- [ ] O mesmo padrao precisa ser aplicado para table functions.
- [ ] O parser ainda precisa ser adaptado para consultar o registry antes de validar uma funcao.
- [ ] Os providers ainda precisam registrar as funcoes suportadas por versao.
- [ ] O core ainda precisa receber o corpo das funcoes compartilhadas por mais de um banco.
- [ ] As funcoes especificas de cada provider ainda precisam ser movidas para seus projetos.
- [ ] O `DbSqlLikeMem.Auto` ainda precisa ser criado como etapa final da migração.

## Ponto de pausa

O ponto de pausa atual e a migracao da descoberta de funcoes para o registry novo.
O proximo passo e ligar o parser ao `Dialect.ScalarFunctions`, depois aplicar o fallback para `SchemaMock.ScalarFunctions`, e repetir o mesmo desenho para procedures e table functions.
Depois disso, avaliar a criacao do projeto `DbSqlLikeMem.Auto` para hospedar `AutoSqlDialect`, movendo essa composicao para fora do Core caso ela fique mais alinhada com a separacao por responsabilidade.
O ponto de decisao e se o agregador de compatibilidade `AutoSqlDialect` deve ficar no Core comum ou em um projeto proprio `DbSqlLikeMem.Auto`, sem misturar essa escolha com as peculiaridades especificas de cada provider.

## Procedures

O mesmo padrao deve ser aplicado para procedures:

- manter no core os corpos compartilhados;
- levar para cada provider as procedures especificas;
- registrar a faixa de versao no momento em que a entrada e adicionada ao dicionario;
- consultar o registry antes de cair em validacoes legadas;
- manter o fallback para definicoes customizadas do schema quando fizer sentido no fluxo de execucao.

## Table Functions

O mesmo padrao deve ser aplicado para table functions:

- manter no core os corpos compartilhados;
- levar para cada provider as table functions especificas;
- registrar a faixa de versao no momento em que a entrada e adicionada ao dicionario;
- consultar o registry antes de cair em validacoes legadas;
- manter o fallback para definicoes customizadas do schema quando fizer sentido no fluxo de execucao.

## Proximos slices sugeridos

1. Inventariar todas as funcoes atuais por provider e marcar o que e compartilhado.
2. Inventariar procedures e table functions com o mesmo criterio.
3. Criar o catalogo comum de corpos compartilhados no projeto `DbSqlLikeMem`.
4. Registrar as funcoes, procedures e table functions por provider em cada projeto `DbSqlLikeMem.<Provider>`.
5. Ajustar o parser para resolver por registry antes da execucao.
6. Remover os caminhos antigos quando a cobertura de registry estiver completa.
7. Criar o projeto `DbSqlLikeMem.Auto` e decidir se `AutoSqlDialect` sai do Core ou permanece nele.
