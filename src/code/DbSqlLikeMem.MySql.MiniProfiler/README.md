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

## Composicao com interceptacao

Se voce tambem quiser gravar eventos ADO.NET, simular latencia ou publicar diagnostics, mantenha o wrapper do MiniProfiler e aplique o pipeline de interceptacao na mesma cadeia de `DbConnection`.

```csharp
var profiled = new ProfiledDbConnection(
    new MySqlConnectionMock(new MySqlDbMock()),
    MiniProfiler.Current);

using var intercepted = profiled.WithInterception(options =>
{
    options.UseRecording();
    options.UseLogging(Console.WriteLine);
});
```

## Faça parte da evolução

Se você encontrou um caso de SQL MySQL que ainda não está coberto, abra uma issue com o script de exemplo. PRs com testes são o melhor caminho para evoluirmos juntos.
