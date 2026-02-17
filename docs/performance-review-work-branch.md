# Revisão de performance da branch `work`

## Escopo da análise
Esta revisão focou nos fluxos que ficaram mais sensíveis após as mudanças de `IndexDef` e `ForeignDef` (composite FK), além dos pontos quentes de execução no caminho de `SELECT`, `INSERT` e `DELETE`.

## Principais pontos com impacto potencial

### 1) Validação de FK em `DELETE` faz varreduras repetidas
No fluxo de exclusão, para cada linha pai a ser removida, o código percorre todas as tabelas e todas as FKs relevantes; para cada referência da FK, chama `childTable.Any(...)`. Isso gera múltiplas passadas sobre a tabela filha.

- Local: `DbDeleteStrategy.ValidateForeignKeys`.
- Padrão atual: `References.All(... childTable.Any(...))`.
- Impacto: complexidade tende a crescer com `linhas_pai * tabelas_filhas * FKs * refs_por_fk * linhas_filha`.
- Risco adicional: em FK composta, o `All(Any(...))` pode validar colunas em linhas diferentes (custo + semântica fraca).

## 2) Lookup de índice materializa cópias profundas em toda consulta
A API de `IndexDef` retorna dicionários somente-leitura criando cópias (`ToDictionary + ReadOnlyDictionary`) em `Lookup`, `TryGetValue`, indexer e enumeração.

- Local: `IndexDef.Lookup`, `IndexDef.TryGetValue`, `IndexDef.this[string]`, `IndexDef.GetEnumerator`.
- Impacto: pressão de alocação e GC em consultas com alto volume de lookups, mesmo quando o resultado do índice é pequeno.

## 3) Acesso a metadados recria estruturas imutáveis com frequência
`TableMock.Columns` e `TableMock.Indexes` convertem o dicionário interno para `ImmutableDictionary` a cada acesso.

- Local: `TableMock.Columns`, `TableMock.Indexes`.
- Impacto: custo cumulativo em caminhos quentes (`GetColumn`, `UpdateIndexesWithRow`, montagem de linhas em `SELECT`).

## 4) Inserção com PK única ainda depende de varredura linear
`EnsureUniqueOnInsert` percorre todas as linhas para validar PK e ainda usa `Columns.First(...)` dentro de loop.

- Local: `TableMock.EnsureUniqueOnInsert`.
- Impacto: em carga de escrita, pode virar gargalo O(n) por inserção para PK (enquanto índices únicos já têm caminho melhor).

## 5) Rebuild de índice não limpa estrutura antes de reconstruir
`IndexDef.RebuildIndex()` percorre as linhas e adiciona em `_items`, mas não limpa `_items` no início.

- Local: `IndexDef.RebuildIndex`.
- Impacto: em cenários com rebuild frequente (ex.: `DELETE` chama `RebuildAllIndexes`), há risco de crescimento indevido de memória e degradação progressiva.

## Recomendações priorizadas

1. **Alta prioridade:** corrigir validação de FK composta em `DELETE` para fazer **uma única varredura por tabela filha/FK** (`Any(childRow => refs.All(...))`) e, idealmente, apoiar em índice quando existir.
2. **Alta prioridade:** revisar `RebuildIndex` para limpar `_items` antes da reconstrução.
3. **Média prioridade:** reduzir cópias em `Lookup` (ex.: caminho interno sem materialização para executor SQL).
4. **Média prioridade:** cachear mapa `pkIndex -> columnName` para evitar `Columns.First(...)` em loop.
5. **Média prioridade:** evitar recriar `ImmutableDictionary` em acessos frequentes (`Columns`/`Indexes`) — usar visão somente leitura estável ou cache invalidado por mutação.

## Quick wins de medição

- Adicionar microbenchmarks para:
  - `SELECT` por índice com 10k/100k linhas;
  - `DELETE` com 1 FK simples e 1 FK composta;
  - `INSERT` em tabela com PK composta e índice único.
- Comparar alocações (`Allocated MB/op`) e tempo (`Mean`, `P95`) antes/depois das correções.


## Pontos candidatos a paralelismo (e status)

1. **Rebuild de múltiplos índices da mesma tabela**
   - Status: **Aplicado** (`Parallel.ForEach`) quando `ThreadSafe=true` e há mais de um índice.
   - Ganho esperado: reduzir tempo de rebuild em `DELETE/UPDATE` com muitas estruturas de índice.

2. **Validação de FK em `DELETE` com tabelas filhas grandes**
   - Status: **Aplicado** (`AsParallel().Any(...)`) com threshold para evitar overhead em tabelas pequenas.
   - Ganho esperado: reduzir latência de scans de referência em FKs compostas.

3. **Lookup por índice para FK antes de scan completo**
   - Status: **Aplicado** em `SchemaMock` e `DbDeleteStrategy`.
   - Ganho esperado: em cenários com índice aderente ao FK, evita varredura de tabela filha.
