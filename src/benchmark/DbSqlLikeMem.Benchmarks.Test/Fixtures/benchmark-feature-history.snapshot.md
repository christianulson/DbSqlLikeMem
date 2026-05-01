# Benchmark Feature History

Registro das mudancas de categoria, status e identidade das features de benchmark.

## Politica

- Uma mudanca de categoria ou status deve entrar aqui antes de virar remocao formal.
- `Comparable` participa da matriz comparativa.
- `MockOnly` fica apenas na matriz app-specific.
- `Deprecated` continua rastreavel no catalogo e nos exportadores.
- `Removed` sai do catalogo ativo depois de registro historico.

## Historico

| Data | Escopo | Mudanca | Resultado |
| --- | --- | --- | --- |
| 2026-05-01 | Catalogo base | A separacao entre `Comparable` e `MockOnly` foi registrada como linha de base do historico de features. | A wiki e o validador passam a refletir explicitamente a divisao entre matriz comparativa e app-specific. |
| 2026-05-01 | Ciclo de vida | Os estados `Active`, `Deprecated` e `Removed` ficaram formalizados no catalogo de benchmark. | O historico passa a registrar mudancas sem perder o rastreio de features legadas. |
