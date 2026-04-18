# Fase 4 - Parser e sintaxe

## Status

IN PROGRESS

## Percentual de entrega

10%

## O que foi feito

- Mapeado um primeiro bloco de sintaxe DML dependente do provider.
- Movidos os trechos SQL de `UPDATE/DELETE JOIN` para o `Dialect` base e para os dialects específicos de SQL Server e PostgreSQL.
- Mantido o fallback padrão do MySQL para o mesmo contrato, sem hardcode no teste base.

## Próximos passos

- Inventariar sintaxes rejeitadas por provider.
- Mover suportes e restrições para o dialect.
- Transformar `skip` em validação negativa quando fizer sentido.
