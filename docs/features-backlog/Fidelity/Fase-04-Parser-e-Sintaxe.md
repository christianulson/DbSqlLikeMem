# Fase 4 - Parser e sintaxe

## Status

IN PROGRESS

## Percentual de entrega

30%

## O que foi feito

- Mapeado um primeiro bloco de sintaxe DML dependente do provider.
- Movidos os trechos SQL de `UPDATE/DELETE JOIN` para o `Dialect` base e para os dialects específicos de SQL Server e PostgreSQL.
- Mantido o fallback padrão do MySQL para o mesmo contrato, sem hardcode no teste base.
- Eliminado o `skip` para `json_each` e `json_tree` e substituido por validacao negativa quando o provider nao suporta funcoes JSON tabulares.
- Confirmado o suporte positivo de SQLite para funcoes JSON tabulares no dialect especifico.
- Tornado `INSERT RETURNING` uma capability explicita no dialect e restringido o teste compartilhado aos providers que realmente suportam essa sintaxe.

## Próximos passos

- Inventariar sintaxes rejeitadas por provider.
- Mover suportes e restrições para o dialect.
- Transformar `skip` em validação negativa quando fizer sentido.
