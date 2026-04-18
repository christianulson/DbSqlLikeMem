# Plano de Ação - Fidelidade

## Objetivo

Manter a fidelidade entre mock e container com prioridade para comportamento observável, tipos nativos, parser, estrutura de resultado e exceções.

## Regra central

- Se o banco real aceita, o mock deve aceitar.
- Se o banco real rejeita, o mock deve rejeitar.
- Se o mock precisar falhar melhor, a exceção pode ter mais contexto, mas sem mudar o gatilho principal.
- Regras por provider devem viver no `Dialect`, não espalhadas em `if`/`switch` nos testes.
- Nao normalizar input, output ou valores lidos do reader dentro do teste so para fazer mock e container parecerem iguais.
- Se a forma do valor precisar ser igual nos dois lados, tratar isso no core da aplicacao ou no dialect.

## Escopo prioritário

1. Parâmetros.
2. Tipos nativos por provider.
3. Formato do resultado.
4. Parser e sintaxe.
5. Funções e semântica.
6. Transações e savepoints.
7. Exceções e mensagens.

## Fase 1 - Inventário dos tipos nativos

Mapear, por provider, todos os tipos nativos relevantes que aparecem nos testes de fidelidade.

Cobertura mínima esperada por provider:

- `DbConnection`
- `DbCommand`
- `DbParameter`
- `DbTransaction`
- `DbDataReader`

Cobertura adicional quando o provider expuser tipos específicos fora do contrato base:

- tipos de conexão nativos
- tipos de comando nativos
- tipos de parâmetro nativos
- tipos de transação nativos
- tipos de reader nativos
- wrappers/interceptors quando fizerem parte do contrato observável

## Fase 2 - Expandir os testes de tipo

Adicionar contratos de tipo para cada provider nativo, não só os cenários atuais de `DbParameter`.

Direção sugerida:

- validar o runtime type do `DbMock`
- validar o runtime type da conexão
- validar o runtime type do comando criado a partir da conexão
- validar o runtime type do parâmetro criado a partir do comando
- validar o runtime type da transação aberta
- validar o runtime type do reader retornado por consulta
- validar que o teste nao reescreve tipo, forma ou nulabilidade dos valores para compensar diferencas do provider

## Fase 3 - Cobrir o shape completo do resultado

Substituir testes que comprimem o resultado para `COUNT(*)` quando o contrato for relacional.

Padrão:

- capturar `QueryResultSnapshot`
- comparar linhas, colunas, aliases e ordem
- normalizar apenas o que o provider real também normaliza

## Fase 4 - Parser e sintaxe

- Criar testes negativos quando o banco real não suporta a sintaxe.
- Evitar `skip` quando a falha for parte do contrato útil.
- Centralizar a regra de suporte no `Dialect`.

## Fase 5 - Funções e semântica

- Cobrir funções temporais, JSON, string, janela, agregação e joins/applies.
- Validar tipo e valor, não só texto formatado.
- Ajustar normalização por provider quando o retorno nativo diferir.

## Fase 6 - Transações

- Cobrir begin, commit, rollback, nested transactions, savepoint e release.
- Validar o mesmo gatilho de falha do provider real.

## Fase 7 - Exceções

- Manter o mesmo tipo principal de falha do provider real.
- Adicionar contexto somente quando não alterar o contrato.
- Garantir consistência entre mock e container.

## Definition of Done

- O teste de fidelidade usa o provider real como referência.
- O `Dialect` concentra as diferenças por provider.
- Os testes de tipo cobrem todos os tipos nativos relevantes do provider.
- Os testes de resultado usam snapshot quando a comparação é relacional.
- As falhas esperadas são validadas explicitamente, não escondidas por skip.
