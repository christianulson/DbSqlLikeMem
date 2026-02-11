# Matriz de compatibilidade SQL (feature x dialeto)

> Status consolidado para os dialetos principais: **MySQL / SQL Server / Oracle / Npgsql / DB2 / SQLite**.
> 
> Legenda: ✅ suportado, ⚠️ suportado parcialmente/condicional, ❌ não suportado.

## Matriz simplificada

| Feature SQL | MySQL | SQL Server | Oracle | Npgsql | DB2 | SQLite |
| --- | --- | --- | --- | --- | --- | --- |
| `WITH` / CTE básica | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| `WITH RECURSIVE` | ⚠️ (versão mínima do dialeto) | ❌ | ❌ | ✅ | ❌ | ✅ |
| `WITH ... AS MATERIALIZED` | ❌ | ❌ | ❌ | ✅ | ❌ | ⚠️ (`NOT MATERIALIZED` em cenários suportados) |
| `LIMIT/OFFSET` | ✅ | ❌ (`OFFSET/FETCH`) | ❌ (`FETCH FIRST/NEXT`) | ✅ | ❌ (`FETCH FIRST`) | ✅ |
| `OFFSET ... FETCH` | ❌ | ✅ (>= versão mínima) | ❌ | ❌ | ❌ | ❌ |
| `FETCH FIRST/NEXT` | ❌ | ❌ | ✅ | ❌ | ✅ | ❌ |
| `INSERT ... ON DUPLICATE KEY UPDATE` | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| `INSERT ... ON CONFLICT` | ❌ | ❌ | ❌ | ✅ | ❌ | ✅ |
| Table hints SQL Server `WITH (...)` | ❌ | ✅ | ❌ | ❌ | ❌ | ❌ |
| Index hints MySQL (`USE/IGNORE/FORCE INDEX`) | ✅ | ❌ | ❌ | ❌ | ❌ | ❌ |
| Operadores JSON `->` / `->>` | ⚠️ (dependente de parser/executor por cenário) | ❌ | ❌ | ✅ | ❌ | ✅ |

## Notas rápidas

- Esta matriz resume o comportamento esperado após os hardenings de parser/testes. Em caso de divergência, os testes por provider têm prioridade como fonte de verdade.
- Recursos marcados como ⚠️ indicam suporte com gate de versão do dialeto ou cobertura parcial.
- Para evoluções planejadas, consulte também o checklist de gaps conhecidos.

## Referências

- [Provedores e features](providers-and-features.md)
- [Checklist de known gaps](known-gaps-checklist.md)
