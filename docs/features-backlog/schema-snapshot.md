# SchemaSnapshot

## Objetivo

Congelar e reaplicar o subset estrutural suportado do schema do `DbSqlLikeMem` sem reescrita manual de setup.

## Subset suportado

- `schemas`
- `tables`
- `columns`
- `primary keys`
- `indexes`
- `foreign keys`, incluindo alvo cross-schema
- `views` por `RawSql`
- `sequences`
- `procedure signatures`
- import/export por JSON e arquivo
- gate explícito de compatibilidade por `dialect/version`
- fingerprint estável
- comparação estruturada com relatório de drift

## Fora do escopo atual

- `check constraints`
- defaults computados por expressão
- geradores executáveis de colunas computadas
- corpos de `trigger`
- corpos de `procedure`
- definições de tabelas temporárias globais

## Fluxo recomendado

1. Exportar o snapshot com `SchemaSnapshot.Export(...)` ou `connection.ExportSchemaSnapshot()`.
2. Persistir em arquivo com `SaveToFile(...)` ou `ExportSchemaSnapshotToFile(...)`.
3. Consultar o subset suportado com `SchemaSnapshot.GetSupportProfile(...)` ou `connection.GetSchemaSnapshotSupportProfile()`.
4. Reaplicar com `Load(...)`/`ApplyTo(...)` ou `ImportSchemaSnapshot(...)`.
5. Validar drift com `GetFingerprint()`, `Matches(...)` ou `CompareTo(...)`.

## Regressão end-to-end

O contrato atual está coberto por regressões em SQLite para:

- round-trip JSON
- round-trip por arquivo
- multi-schema
- `foreign key` cross-schema
- realinhamento de `Database` na conexão
- gate estrito de compatibilidade
- replay snapshot-first
- fingerprint e relatório estruturado de drift
