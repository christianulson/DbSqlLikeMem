# DbSqlLikeMem.Sqlite

Com **`DbSqlLikeMem.Sqlite`**, você testa comportamentos inspirados em SQLite sem abrir arquivo físico nem depender de banco externo.

## Para quem é

- Times que desejam testes leves e rápidos
- Projetos que usam SQLite no desktop, mobile ou serviços
- Equipes que valorizam execução determinística no CI

## Instalação

```bash
dotnet add package DbSqlLikeMem.Sqlite
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.Sqlite;

var conn = new SqliteConnectionMock(new SqliteDbMock());
conn.Open();
```

## Contribuindo

Contribua com melhorias de compatibilidade, exemplos de uso e documentação para tornar o pacote ainda mais atrativo para novos mantenedores e usuários.
