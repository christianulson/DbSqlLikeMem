# DbSqlLikeMem.MySql

Teste seu código MySQL sem subir servidor: **`DbSqlLikeMem.MySql`** traz mocks de conexão/comando e comportamento SQL em memória para cenários de teste mais rápidos e baratos.

## Destaques

- Simulação focada no ecossistema MySQL
- APIs familiares de ADO.NET
- Fluxo compatível com Dapper
- Excelente para pipelines CI com execução paralela

## Instalação

```bash
dotnet add package DbSqlLikeMem.MySql
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.MySql;

var conn = new MySqlConnectionMock(new MySqlDbMock());
conn.Open();
```

## Faça parte da evolução

Se você encontrou um caso de SQL MySQL que ainda não está coberto, abra uma issue com o script de exemplo. PRs com testes são o melhor caminho para evoluirmos juntos.
