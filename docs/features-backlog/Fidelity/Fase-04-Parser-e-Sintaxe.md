# Fase 4 - Parser e sintaxe

## Status

IN PROGRESS

## Percentual de entrega

75%

## O que foi feito

- Mapeado um primeiro bloco de sintaxe DML dependente do provider.
- Movidos os trechos SQL de `UPDATE/DELETE JOIN` para o `Dialect` base e para os dialects específicos de SQL Server e PostgreSQL.
- Mantido o fallback padrão do MySQL para o mesmo contrato, sem hardcode no teste base.
- Normalizado o handling de `CREATE TABLE AS SELECT` no pipeline de non-query, mantendo a execução centralizada no command handler.
- Eliminado o `skip` para `json_each` e `json_tree` e substituido por validacao negativa quando o provider nao suporta funcoes JSON tabulares.
- Confirmado o suporte positivo de SQLite para funcoes JSON tabulares no dialect especifico.
- Tornado `INSERT RETURNING` uma capability explicita no dialect e restringido o teste compartilhado aos providers que realmente suportam essa sintaxe.
- Expandido o parser e o modelo de sequence para `CYCLE`, `NO CYCLE`, `MINVALUE` e `MAXVALUE`, mantendo o estado no snapshot e na criacao da sequence.
- Adicionado suporte de parser e execucao para `ALTER SEQUENCE ... INCREMENT BY`, preservando o comportamento de `RESTART WITH` no mesmo caminho de sintaxe.
- Adicionado suporte de parser, estado e drop automatizado para `ALTER SEQUENCE OWNED BY NONE` e `ALTER SEQUENCE OWNED BY tabela.coluna`, preservando o vinculo da sequence com a tabela proprietaria.
- Adicionado suporte de parser e execucao para `CREATE SEQUENCE ... OWNED BY NONE` e `CREATE SEQUENCE ... OWNED BY tabela.coluna`, alinhando a criacao com o mesmo modelo de ownership.
- Adicionados testes de parser para `CREATE SEQUENCE ... OWNED BY` e `ALTER SEQUENCE ... OWNED BY NONE`, validando o novo contrato da sintaxe.

## Próximos passos

- Inventariar sintaxes rejeitadas por provider.
- Mover suportes e restrições para o dialect.
- Transformar `skip` em validação negativa quando fizer sentido.
