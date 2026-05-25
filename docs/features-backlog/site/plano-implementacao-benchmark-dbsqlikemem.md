# Plano de Implementação — Site React + API C# para Comparativo DbSqlLikeMem vs Bancos Reais

## 1. Objetivo

Construir uma aplicação web onde o usuário possa:

1. Selecionar um banco, uma versão específica ou todos os bancos suportados.
2. Informar um script SQL de inicialização, por exemplo:
   - criação de tabelas;
   - criação de índices;
   - criação de sequences;
   - inserts de carga inicial;
   - constraints simples.
3. Informar uma query ou script de teste a ser executado.
4. Enviar o job para o servidor.
5. Receber um comparativo entre:
   - execução no DbSqlLikeMem;
   - execução em bancos reais rodando em containers temporários.
6. Visualizar:
   - tempo de execução por engine;
   - sucesso ou erro por banco;
   - compatibilidade da query por banco;
   - resultado tabular quando aplicável;
   - plano de execução do DbSqlLikeMem;
   - métricas de plano;
   - hints;
   - warnings;
   - recomendações de índice;
   - logs técnicos.

---

## 2. Decisão de Hospedagem

### 2.1 Melhor opção de menor custo

A melhor opção para este projeto é:

```text
Hetzner Cloud CX22
```

Configuração aproximada:

```text
2 vCPU
4 GB RAM
40 GB disco
20 TB tráfego
custo base aproximado: €3,79/mês sem VAT
```

### 2.2 Por que VPS, e não Render/Railway/Fly/Cloud Run?

Este projeto não é apenas um site com API.

O backend precisa:

1. receber SQL arbitrário controlado;
2. subir containers temporários de bancos reais;
3. aguardar readiness/healthcheck;
4. executar scripts;
5. coletar métricas;
6. destruir containers;
7. isolar execuções.

Esse requisito exige acesso ao Docker Engine do host.

Em plataformas como Cloud Run, Render, Railway e Fly.io, normalmente você consegue subir o seu app em container, mas não é o ambiente ideal para o app controlar um Docker host e criar containers filhos de bancos reais sob demanda.

Portanto, para custo baixo e controle operacional, use:

```text
VPS Linux + Docker Engine + Docker Compose + Nginx/Caddy
```

### 2.3 Alternativas

| Provedor | Indicação | Observação |
|---|---|---|
| Hetzner CX22 | melhor custo inicial | recomendado |
| Hetzner CX32 | melhor se usar SQL Server com frequência | mais folga de RAM |
| Contabo VPS | muito hardware por baixo custo | avaliar estabilidade e I/O |
| DigitalOcean Droplet | simples e confiável | mais caro para 4 GB RAM |
| Render/Railway | bom para apps simples | não recomendado como executor Docker |
| Cloud Run | ótimo para API stateless | não recomendado para orquestrar containers filhos |

### 2.4 Domínio `.org`

O domínio `.org` não muda a hospedagem.

Você terá:

```text
dominio.org  -> DNS A/AAAA -> IP da VPS
api.dominio.org -> DNS A/AAAA -> IP da VPS
```

O certificado HTTPS pode ser gratuito via Let's Encrypt.

---

## 3. Arquitetura Geral

### 3.1 Visão macro

```text
[Usuário]
   |
   v
[React + TypeScript]
   |
   v
[ASP.NET Core API]
   |
   +--> [DbSqlLikeMem Runner]
   |
   +--> [Compatibility Analyzer]
   |
   +--> [Benchmark Orchestrator]
            |
            +--> [Docker Engine]
                    |
                    +--> PostgreSQL container temporário
                    +--> MySQL container temporário
                    +--> SQL Server container temporário
                    +--> MariaDB container temporário
                    +--> Firebird container temporário
                    +--> DB2 container temporário opcional
                    +--> Oracle container temporário opcional
```

### 3.2 Componentes

```text
src/
  frontend/
    React + TypeScript + Vite

  backend/
    Benchmark.Api/
    Benchmark.Application/
    Benchmark.Domain/
    Benchmark.Infrastructure/
    Benchmark.Worker/

  deploy/
    docker-compose.yml
    nginx.conf
    caddy/Caddyfile
    scripts/
```

---

## 4. Escopo do MVP

### 4.1 MVP recomendado

Implementar primeiro:

```text
DbSqlLikeMem
SQLite
PostgreSQL
MySQL
SQL Server
```

### 4.2 Deixar para fase 2

```text
MariaDB
Firebird
Oracle
DB2
SQL Azure simulado
```

### 4.3 Motivo

SQL Server, PostgreSQL e MySQL já cobrem a maior parte dos cenários práticos.

Oracle e DB2 têm imagens mais pesadas, exigem mais RAM, podem ter termos/licenças específicos e aumentam bastante o custo operacional.

---

## 5. Funcionalidades do Produto

## 5.1 Tela principal

Campos:

```text
Título: DbSqlLikeMem Benchmark Lab

[ ] Executar em todos os bancos

Bancos:
[x] DbSqlLikeMem - SQL Server 2022
[x] DbSqlLikeMem - PostgreSQL 17
[x] PostgreSQL real 17
[x] MySQL real 8.4
[x] SQL Server real 2022

Script de inicialização:
<textarea>
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    active INT
);

CREATE INDEX ix_users_active ON users(active);

INSERT INTO users (id, name, active) VALUES
(1, 'Alice', 1),
(2, 'Bob', 0),
(3, 'Carol', 1);
</textarea>

Query de teste:
<textarea>
SELECT id, name
FROM users
WHERE active = 1
ORDER BY id;
</textarea>

[Executar comparativo]
```

## 5.2 Resultado resumido

Tabela:

| Banco | Versão | Tipo | Compatível | Status | Setup ms | Query ms | Linhas | Observação |
|---|---:|---|---|---|---:|---:|---:|---|
| DbSqlLikeMem SQL Server | 2022 | mock | sim | sucesso | 2 | 1 | 2 | plano disponível |
| PostgreSQL | 17 | real | sim | sucesso | 850 | 6 | 2 | container efêmero |
| MySQL | 8.4 | real | sim | sucesso | 920 | 4 | 2 | container efêmero |
| SQL Server | 2022 | real | sim | sucesso | 3500 | 12 | 2 | container pesado |

## 5.3 Resultado detalhado

Abas:

```text
Resumo
Resultados
Compatibilidade
Plano DbSqlLikeMem
Plan Warnings
Index Recommendations
Logs
JSON
```

---

## 6. Estrutura do Backend

### 6.1 Projetos

```bash
mkdir src/benchmark-lab
cd src/benchmark-lab

mkdir src
cd src

dotnet new sln -n BenchmarkLab

dotnet new webapi -n Benchmark.Api
dotnet new classlib -n Benchmark.Application
dotnet new classlib -n Benchmark.Domain
dotnet new classlib -n Benchmark.Infrastructure
dotnet new worker -n Benchmark.Worker

dotnet sln BenchmarkLab.sln add Benchmark.Api/Benchmark.Api.csproj
dotnet sln BenchmarkLab.sln add Benchmark.Application/Benchmark.Application.csproj
dotnet sln BenchmarkLab.sln add Benchmark.Domain/Benchmark.Domain.csproj
dotnet sln BenchmarkLab.sln add Benchmark.Infrastructure/Benchmark.Infrastructure.csproj
dotnet sln BenchmarkLab.sln add Benchmark.Worker/Benchmark.Worker.csproj

dotnet add Benchmark.Api/Benchmark.Api.csproj reference Benchmark.Application/Benchmark.Application.csproj
dotnet add Benchmark.Application/Benchmark.Application.csproj reference Benchmark.Domain/Benchmark.Domain.csproj
dotnet add Benchmark.Infrastructure/Benchmark.Infrastructure.csproj reference Benchmark.Application/Benchmark.Application.csproj
dotnet add Benchmark.Worker/Benchmark.Worker.csproj reference Benchmark.Application/Benchmark.Application.csproj
dotnet add Benchmark.Worker/Benchmark.Worker.csproj reference Benchmark.Infrastructure/Benchmark.Infrastructure.csproj
```

### 6.2 Pacotes recomendados

```bash
dotnet add Benchmark.Api package Swashbuckle.AspNetCore
dotnet add Benchmark.Api package FluentValidation.AspNetCore
dotnet add Benchmark.Api package Serilog.AspNetCore
dotnet add Benchmark.Api package Serilog.Sinks.Console

dotnet add Benchmark.Infrastructure package Dapper
dotnet add Benchmark.Infrastructure package Npgsql
dotnet add Benchmark.Infrastructure package MySqlConnector
dotnet add Benchmark.Infrastructure package Microsoft.Data.SqlClient
dotnet add Benchmark.Infrastructure package Docker.DotNet

dotnet add Benchmark.Infrastructure package DbSqlLikeMem.SqlServer
dotnet add Benchmark.Infrastructure package DbSqlLikeMem.Npgsql
dotnet add Benchmark.Infrastructure package DbSqlLikeMem.MySql
dotnet add Benchmark.Infrastructure package DbSqlLikeMem.Sqlite
```

Se for consumir o projeto local do DbSqlLikeMem em vez do NuGet:

```xml
<ItemGroup>
  <ProjectReference Include="../../DbSqlLikeMem/src/code/DbSqlLikeMem/DbSqlLikeMem.csproj" />
  <ProjectReference Include="../../DbSqlLikeMem/src/code/DbSqlLikeMem.SqlServer/DbSqlLikeMem.SqlServer.csproj" />
  <ProjectReference Include="../../DbSqlLikeMem/src/code/DbSqlLikeMem.Npgsql/DbSqlLikeMem.Npgsql.csproj" />
  <ProjectReference Include="../../DbSqlLikeMem/src/code/DbSqlLikeMem.MySql/DbSqlLikeMem.MySql.csproj" />
  <ProjectReference Include="../../DbSqlLikeMem/src/code/DbSqlLikeMem.Sqlite/DbSqlLikeMem.Sqlite.csproj" />
</ItemGroup>
```

---

## 7. Contratos da API

### 7.1 Request

```csharp
public sealed class BenchmarkRequest
{
    public required IReadOnlyList<BenchmarkTargetRequest> Targets { get; init; }
    public required string SetupSql { get; init; }
    public required string TestSql { get; init; }
    public BenchmarkOptions Options { get; init; } = new();
}

public sealed class BenchmarkTargetRequest
{
    public required string Provider { get; init; }
    public required string Version { get; init; }
    public required BenchmarkTargetKind Kind { get; init; }
}

public enum BenchmarkTargetKind
{
    DbSqlLikeMem,
    RealContainer
}

public sealed class BenchmarkOptions
{
    public int TimeoutSeconds { get; init; } = 60;
    public int MaxRowsToReturn { get; init; } = 100;
    public bool IncludeResultRows { get; init; } = true;
    public bool IncludeExecutionPlan { get; init; } = true;
    public bool DropContainerAfterRun { get; init; } = true;
}
```

### 7.2 Response

```csharp
public sealed class BenchmarkResponse
{
    public required string JobId { get; init; }
    public required DateTimeOffset StartedAt { get; init; }
    public required DateTimeOffset FinishedAt { get; init; }
    public required BenchmarkSummary Summary { get; init; }
    public required IReadOnlyList<BenchmarkTargetResult> Results { get; init; }
}

public sealed class BenchmarkSummary
{
    public int TotalTargets { get; init; }
    public int SuccessCount { get; init; }
    public int FailedCount { get; init; }
    public int CompatibleCount { get; init; }
    public int IncompatibleCount { get; init; }
    public string? FastestProvider { get; init; }
    public long? FastestElapsedMs { get; init; }
}

public sealed class BenchmarkTargetResult
{
    public required string Provider { get; init; }
    public required string Version { get; init; }
    public required BenchmarkTargetKind Kind { get; init; }
    public required CompatibilityStatus Compatibility { get; init; }
    public required ExecutionStatus Status { get; init; }

    public long SetupElapsedMs { get; init; }
    public long QueryElapsedMs { get; init; }
    public int RowCount { get; init; }

    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }

    public IReadOnlyList<IDictionary<string, object?>> Rows { get; init; } = [];

    public DbSqlLikeMemPlanDto? DbSqlLikeMemPlan { get; init; }
    public RealDatabasePlanDto? RealPlan { get; init; }

    public IReadOnlyList<string> Logs { get; init; } = [];
}

public enum CompatibilityStatus
{
    Compatible,
    CompatibleWithWarnings,
    Incompatible,
    NotTested
}

public enum ExecutionStatus
{
    Success,
    Failed,
    Timeout,
    Cancelled
}
```

### 7.3 Plano DbSqlLikeMem

```csharp
public sealed class DbSqlLikeMemPlanDto
{
    public string? RawText { get; init; }

    public int? EstimatedCost { get; init; }
    public int? InputTables { get; init; }
    public long? EstimatedRowsRead { get; init; }
    public int? ActualRows { get; init; }
    public double? SelectivityPct { get; init; }
    public double? RowsPerMs { get; init; }
    public long? ElapsedMs { get; init; }

    public IReadOnlyList<PlanWarningDto> Warnings { get; init; } = [];
    public IReadOnlyList<IndexRecommendationDto> IndexRecommendations { get; init; } = [];
    public string? PlanQualityGrade { get; init; }
    public int? PlanRiskScore { get; init; }
}

public sealed class PlanWarningDto
{
    public required string Code { get; init; }
    public required string Message { get; init; }
    public required string Severity { get; init; }
    public string? Reason { get; init; }
    public string? SuggestedAction { get; init; }
}

public sealed class IndexRecommendationDto
{
    public required string Table { get; init; }
    public required string SuggestedIndex { get; init; }
    public required string Reason { get; init; }
    public int Confidence { get; init; }
    public double EstimatedGainPct { get; init; }
}
```

---

## 8. Endpoints

### 8.1 Listar bancos suportados

```http
GET /api/providers
```

Resposta:

```json
{
  "providers": [
    {
      "id": "dbsqlikemem-sqlserver",
      "name": "DbSqlLikeMem SQL Server",
      "kind": "DbSqlLikeMem",
      "versions": ["7", "2000", "2005", "2008", "2012", "2014", "2016", "2017", "2019", "2022"]
    },
    {
      "id": "postgres-real",
      "name": "PostgreSQL",
      "kind": "RealContainer",
      "versions": ["15", "16", "17"]
    },
    {
      "id": "mysql-real",
      "name": "MySQL",
      "kind": "RealContainer",
      "versions": ["5.7", "8.0", "8.4"]
    },
    {
      "id": "sqlserver-real",
      "name": "SQL Server",
      "kind": "RealContainer",
      "versions": ["2019", "2022"]
    }
  ]
}
```

### 8.2 Executar benchmark síncrono no MVP

```http
POST /api/benchmarks/run
```

### 8.3 Criar job assíncrono na fase 2

```http
POST /api/benchmarks
GET /api/benchmarks/{jobId}
GET /api/benchmarks/{jobId}/events
```

Para MVP, implemente síncrono com timeout curto.

Para produção, implemente fila.

---

## 9. Runner do DbSqlLikeMem

### 9.1 Interface

```csharp
public interface IDbSqlLikeMemRunner
{
    Task<BenchmarkTargetResult> RunAsync(
        BenchmarkTargetRequest target,
        string setupSql,
        string testSql,
        BenchmarkOptions options,
        CancellationToken cancellationToken);
}
```

### 9.2 Factory de provider

```csharp
using System.Data.Common;
using DbSqlLikeMem.MySql;
using DbSqlLikeMem.Npgsql;
using DbSqlLikeMem.Sqlite;
using DbSqlLikeMem.SqlServer;

public static class DbSqlLikeMemConnectionFactory
{
    public static DbConnection Create(string provider, string version)
    {
        var normalized = provider.Trim().ToLowerInvariant();

        return normalized switch
        {
            "sqlserver" or "dbsqlikemem-sqlserver"
                => new SqlServerConnectionMock(new SqlServerDbMock(version: ParseIntVersion(version))),

            "postgres" or "postgresql" or "npgsql" or "dbsqlikemem-postgres"
                => new NpgsqlConnectionMock(new NpgsqlDbMock(version: ParseIntVersion(version))),

            "mysql" or "dbsqlikemem-mysql"
                => new MySqlConnectionMock(new MySqlDbMock(version: ParseIntVersion(version))),

            "sqlite" or "dbsqlikemem-sqlite"
                => new SqliteConnectionMock(new SqliteDbMock(version: ParseIntVersion(version))),

            _ => throw new NotSupportedException($"Provider DbSqlLikeMem não suportado: {provider}")
        };
    }

    private static int ParseIntVersion(string version)
    {
        return version.Replace(".", "", StringComparison.Ordinal).Trim() switch
        {
            "" => 0,
            var value when int.TryParse(value, out var parsed) => parsed,
            _ => throw new ArgumentException($"Versão inválida: {version}")
        };
    }
}
```

Ajuste os construtores de acordo com a assinatura exata da versão do pacote que você estiver usando.

### 9.3 Execução

```csharp
public sealed class DbSqlLikeMemRunner : IDbSqlLikeMemRunner
{
    public async Task<BenchmarkTargetResult> RunAsync(
        BenchmarkTargetRequest target,
        string setupSql,
        string testSql,
        BenchmarkOptions options,
        CancellationToken cancellationToken)
    {
        var setupWatch = System.Diagnostics.Stopwatch.StartNew();
        var queryWatch = new System.Diagnostics.Stopwatch();

        try
        {
            using var connection = DbSqlLikeMemConnectionFactory.Create(target.Provider, target.Version);
            await connection.OpenAsync(cancellationToken);

            using (var setupCommand = connection.CreateCommand())
            {
                setupCommand.CommandText = setupSql;
                setupCommand.CommandTimeout = options.TimeoutSeconds;
                await setupCommand.ExecuteNonQueryAsync(cancellationToken);
            }

            setupWatch.Stop();

            var rows = new List<IDictionary<string, object?>>();

            queryWatch.Start();

            using (var queryCommand = connection.CreateCommand())
            {
                queryCommand.CommandText = testSql;
                queryCommand.CommandTimeout = options.TimeoutSeconds;

                using var reader = await queryCommand.ExecuteReaderAsync(cancellationToken);

                while (await reader.ReadAsync(cancellationToken))
                {
                    if (rows.Count >= options.MaxRowsToReturn)
                        continue;

                    var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
                    for (var i = 0; i < reader.FieldCount; i++)
                        row[reader.GetName(i)] = await reader.IsDBNullAsync(i, cancellationToken) ? null : reader.GetValue(i);

                    rows.Add(row);
                }
            }

            queryWatch.Stop();

            var planText = TryGetLastExecutionPlan(connection);

            return new BenchmarkTargetResult
            {
                Provider = target.Provider,
                Version = target.Version,
                Kind = target.Kind,
                Compatibility = CompatibilityStatus.Compatible,
                Status = ExecutionStatus.Success,
                SetupElapsedMs = setupWatch.ElapsedMilliseconds,
                QueryElapsedMs = queryWatch.ElapsedMilliseconds,
                RowCount = rows.Count,
                Rows = options.IncludeResultRows ? rows : [],
                DbSqlLikeMemPlan = options.IncludeExecutionPlan
                    ? DbSqlLikeMemPlanParser.Parse(planText)
                    : null
            };
        }
        catch (OperationCanceledException)
        {
            return new BenchmarkTargetResult
            {
                Provider = target.Provider,
                Version = target.Version,
                Kind = target.Kind,
                Compatibility = CompatibilityStatus.NotTested,
                Status = ExecutionStatus.Timeout,
                SetupElapsedMs = setupWatch.ElapsedMilliseconds,
                QueryElapsedMs = queryWatch.ElapsedMilliseconds,
                ErrorCode = "TIMEOUT",
                ErrorMessage = "A execução excedeu o timeout configurado."
            };
        }
        catch (Exception ex)
        {
            return new BenchmarkTargetResult
            {
                Provider = target.Provider,
                Version = target.Version,
                Kind = target.Kind,
                Compatibility = CompatibilityStatus.Incompatible,
                Status = ExecutionStatus.Failed,
                SetupElapsedMs = setupWatch.ElapsedMilliseconds,
                QueryElapsedMs = queryWatch.ElapsedMilliseconds,
                ErrorCode = ex.GetType().Name,
                ErrorMessage = ex.Message
            };
        }
    }

    private static string? TryGetLastExecutionPlan(DbConnection connection)
    {
        var property = connection.GetType().GetProperty("LastExecutionPlan");
        return property?.GetValue(connection)?.ToString();
    }
}
```

---

## 10. Parser simples do plano DbSqlLikeMem

No MVP, parseie o texto do plano com regex simples.

```csharp
using System.Globalization;
using System.Text.RegularExpressions;

public static class DbSqlLikeMemPlanParser
{
    public static DbSqlLikeMemPlanDto? Parse(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
            return null;

        return new DbSqlLikeMemPlanDto
        {
            RawText = raw,
            EstimatedCost = ReadInt(raw, "Estimated Cost"),
            InputTables = ReadInt(raw, "Input Tables"),
            EstimatedRowsRead = ReadLong(raw, "Estimated Rows Read"),
            ActualRows = ReadInt(raw, "Actual Rows"),
            SelectivityPct = ReadDouble(raw, "Selectivity"),
            RowsPerMs = ReadDouble(raw, "Rows/ms"),
            ElapsedMs = ReadLong(raw, "Elapsed")
        };
    }

    private static int? ReadInt(string raw, string label)
        => int.TryParse(ReadValue(raw, label), NumberStyles.Any, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static long? ReadLong(string raw, string label)
        => long.TryParse(ReadValue(raw, label), NumberStyles.Any, CultureInfo.InvariantCulture, out var value)
            ? value
            : null;

    private static double? ReadDouble(string raw, string label)
        => double.TryParse(
                ReadValue(raw, label)?.Replace("%", "", StringComparison.Ordinal),
                NumberStyles.Any,
                CultureInfo.InvariantCulture,
                out var value)
            ? value
            : null;

    private static string? ReadValue(string raw, string label)
    {
        var pattern = $@"{Regex.Escape(label)}\s*:\s*(?<value>[^\r\n]+)";
        var match = Regex.Match(raw, pattern, RegexOptions.IgnoreCase);
        return match.Success ? match.Groups["value"].Value.Trim() : null;
    }
}
```

Depois, evolua para expor o plano de forma estruturada diretamente no DbSqlLikeMem, se fizer sentido.

---

## 11. Runner de Banco Real em Container

### 11.1 Interface

```csharp
public interface IRealDatabaseContainerRunner
{
    Task<BenchmarkTargetResult> RunAsync(
        BenchmarkTargetRequest target,
        string setupSql,
        string testSql,
        BenchmarkOptions options,
        CancellationToken cancellationToken);
}
```

### 11.2 Estratégia

Para cada execução:

1. gerar um `jobId`;
2. criar nome único de container;
3. criar rede Docker isolada ou usar rede padrão controlada;
4. subir container do banco;
5. aguardar readiness;
6. abrir conexão;
7. executar setup;
8. executar teste;
9. coletar resultado;
10. remover container.

### 11.3 Imagens recomendadas

```text
PostgreSQL 17: postgres:17-alpine
PostgreSQL 16: postgres:16-alpine
MySQL 8.4: mysql:8.4
MySQL 8.0: mysql:8.0
SQL Server 2022: mcr.microsoft.com/mssql/server:2022-latest
SQL Server 2019: mcr.microsoft.com/mssql/server:2019-latest
```

### 11.4 Portas

Evite expor as portas publicamente.

Use portas dinâmicas ou rede Docker interna.

Exemplo:

```text
PostgreSQL interno: 5432
MySQL interno: 3306
SQL Server interno: 1433
```

No MVP, se a API rodar no host e conectar via porta mapeada:

```text
127.0.0.1:randomPort -> container:defaultPort
```

Nunca exponha esses bancos em `0.0.0.0`.

---

## 12. Docker.DotNet — Exemplo base

### 12.1 Cliente Docker

```csharp
using Docker.DotNet;

public static class DockerClientFactory
{
    public static DockerClient Create()
    {
        return new DockerClientConfiguration(
            new Uri("unix:///var/run/docker.sock"))
            .CreateClient();
    }
}
```

### 12.2 Subir PostgreSQL

```csharp
using Docker.DotNet;
using Docker.DotNet.Models;

public sealed class DockerDatabaseContainerService
{
    private readonly DockerClient _docker;

    public DockerDatabaseContainerService(DockerClient docker)
    {
        _docker = docker;
    }

    public async Task<string> StartPostgresAsync(string jobId, string version, CancellationToken ct)
    {
        var containerName = $"bench-postgres-{version}-{jobId}".ToLowerInvariant();

        var response = await _docker.Containers.CreateContainerAsync(new CreateContainerParameters
        {
            Image = $"postgres:{version}-alpine",
            Name = containerName,
            Env =
            [
                "POSTGRES_USER=bench",
                "POSTGRES_PASSWORD=bench_password",
                "POSTGRES_DB=benchdb"
            ],
            HostConfig = new HostConfig
            {
                AutoRemove = false,
                PortBindings = new Dictionary<string, IList<PortBinding>>
                {
                    ["5432/tcp"] =
                    [
                        new PortBinding
                        {
                            HostIp = "127.0.0.1",
                            HostPort = ""
                        }
                    ]
                },
                Memory = 512L * 1024 * 1024,
                NanoCPUs = 1_000_000_000
            }
        }, ct);

        await _docker.Containers.StartContainerAsync(response.ID, new ContainerStartParameters(), ct);

        return response.ID;
    }

    public async Task RemoveAsync(string containerId, CancellationToken ct)
    {
        try
        {
            await _docker.Containers.StopContainerAsync(containerId, new ContainerStopParameters
            {
                WaitBeforeKillSeconds = 5
            }, ct);
        }
        catch
        {
            // Ignore stop errors.
        }

        try
        {
            await _docker.Containers.RemoveContainerAsync(containerId, new ContainerRemoveParameters
            {
                Force = true,
                RemoveVolumes = true
            }, ct);
        }
        catch
        {
            // Ignore cleanup errors.
        }
    }
}
```

### 12.3 Readiness

```csharp
public static async Task WaitUntilReadyAsync(
    Func<Task> probe,
    TimeSpan timeout,
    CancellationToken ct)
{
    var startedAt = DateTimeOffset.UtcNow;
    Exception? last = null;

    while (DateTimeOffset.UtcNow - startedAt < timeout)
    {
        ct.ThrowIfCancellationRequested();

        try
        {
            await probe();
            return;
        }
        catch (Exception ex)
        {
            last = ex;
            await Task.Delay(TimeSpan.FromMilliseconds(500), ct);
        }
    }

    throw new TimeoutException("Container não ficou pronto a tempo.", last);
}
```

---

## 13. Execução por Banco Real

### 13.1 PostgreSQL

Connection string:

```text
Host=127.0.0.1;Port={port};Database=benchdb;Username=bench;Password=bench_password;Timeout=15;Command Timeout=60
```

Execução:

```csharp
using Npgsql;

public async Task ExecutePostgresAsync(string connectionString, string setupSql, string testSql, CancellationToken ct)
{
    await using var connection = new NpgsqlConnection(connectionString);
    await connection.OpenAsync(ct);

    await using (var setup = new NpgsqlCommand(setupSql, connection))
        await setup.ExecuteNonQueryAsync(ct);

    await using var query = new NpgsqlCommand(testSql, connection);
    await using var reader = await query.ExecuteReaderAsync(ct);

    while (await reader.ReadAsync(ct))
    {
        // map rows
    }
}
```

Plano real opcional:

```sql
EXPLAIN (FORMAT JSON, ANALYZE, BUFFERS) <query>
```

### 13.2 MySQL

Connection string:

```text
Server=127.0.0.1;Port={port};Database=benchdb;Uid=bench;Pwd=bench_password;Connection Timeout=15;Default Command Timeout=60
```

Plano real opcional:

```sql
EXPLAIN FORMAT=JSON <query>
```

### 13.3 SQL Server

Variáveis:

```text
ACCEPT_EULA=Y
MSSQL_SA_PASSWORD=Your_strong_password123!
MSSQL_PID=Developer
```

Connection string:

```text
Server=127.0.0.1,{port};Database=tempdb;User Id=sa;Password=Your_strong_password123!;TrustServerCertificate=True;Connection Timeout=15;Command Timeout=60
```

Plano real opcional:

```sql
SET STATISTICS IO ON;
SET STATISTICS TIME ON;
```

Ou:

```sql
SET SHOWPLAN_XML ON;
<query>
SET SHOWPLAN_XML OFF;
```

No MVP, colete apenas tempo e status. Adicione plano real depois.

---

## 14. Compatibilidade da Query

### 14.1 Estratégia MVP

A compatibilidade deve ser determinada por execução real:

```text
Se setup + query executaram com sucesso:
    Compatible

Se setup executou, mas query falhou por sintaxe/função:
    Incompatible

Se executou, mas houve warning/diferença conhecida:
    CompatibleWithWarnings
```

### 14.2 Normalização de erros

Crie um classificador:

```csharp
public sealed class SqlCompatibilityClassifier
{
    public CompatibilityStatus Classify(Exception ex)
    {
        var message = ex.Message.ToLowerInvariant();

        if (message.Contains("syntax", StringComparison.OrdinalIgnoreCase))
            return CompatibilityStatus.Incompatible;

        if (message.Contains("does not exist", StringComparison.OrdinalIgnoreCase))
            return CompatibilityStatus.Incompatible;

        if (message.Contains("unknown function", StringComparison.OrdinalIgnoreCase))
            return CompatibilityStatus.Incompatible;

        if (message.Contains("timeout", StringComparison.OrdinalIgnoreCase))
            return CompatibilityStatus.NotTested;

        return CompatibilityStatus.Incompatible;
    }
}
```

### 14.3 Estratégia fase 2

Adicionar analisador prévio por dialeto:

```text
LIMIT/OFFSET
TOP
FETCH NEXT
RETURNING
OUTPUT
ON DUPLICATE KEY UPDATE
ON CONFLICT
MERGE
FOR JSON
JSON_TABLE
json_each/json_tree
PIVOT/UNPIVOT
Sequences
Identity
Temporary tables
```

---

## 15. Segurança

Este é o ponto mais crítico do projeto.

Você está permitindo que usuários executem SQL.

### 15.1 Restrições obrigatórias

No MVP:

```text
não permitir autenticação pública sem limite;
não permitir execução anônima ilimitada;
não expor containers de banco para a internet;
limitar timeout;
limitar memória;
limitar CPU;
limitar tamanho do script;
limitar tamanho do resultado;
limitar concorrência;
remover containers após execução;
não montar volumes do host nos bancos;
não passar Docker socket para containers de usuário;
não executar containers privilegiados;
não rodar API como root se possível.
```

### 15.2 Limites recomendados

```json
{
  "maxSetupSqlBytes": 200000,
  "maxTestSqlBytes": 50000,
  "maxRowsToReturn": 100,
  "maxConcurrentJobs": 1,
  "jobTimeoutSeconds": 90,
  "databaseStartupTimeoutSeconds": 45,
  "containerMemoryMb": {
    "postgres": 512,
    "mysql": 768,
    "sqlserver": 2048
  }
}
```

### 15.3 Bloqueios de SQL administrativo

Bloquear ou tratar com muito cuidado:

```sql
CREATE USER
ALTER USER
DROP USER
GRANT
REVOKE
BACKUP
RESTORE
COPY ... PROGRAM
LOAD DATA LOCAL INFILE
xp_cmdshell
sp_configure
CREATE EXTENSION
CREATE SERVER
CREATE FOREIGN TABLE
```

### 15.4 Isolamento

Para cada job:

```text
container novo
database novo
sem volume persistente
sem bind mount
rede controlada
porta mapeada apenas em 127.0.0.1
remoção forçada no finally
```

### 15.5 Cleanup de emergência

Criar job periódico:

```bash
docker ps -a --filter "name=bench-" --format "{{.ID}}" | xargs -r docker rm -f
docker volume prune -f
docker network prune -f
```

---

## 16. Fila de Jobs

### 16.1 MVP

Comece sem Redis.

Use fila em memória:

```csharp
Channel<BenchmarkJob>
```

Limite:

```text
1 job por vez
```

### 16.2 Fase 2

Adicionar Redis:

```text
API recebe job
API grava job no banco
API publica job na fila
Worker consome
Frontend acompanha por polling ou SSE
```

Opções:

```text
Hangfire + Redis
Quartz.NET
BackgroundService + Channel
MassTransit + RabbitMQ
```

Para menor custo:

```text
BackgroundService + SQLite local
```

---

## 17. Persistência

### 17.1 MVP

SQLite local:

```text
app.db
```

Tabelas:

```sql
CREATE TABLE benchmark_jobs (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    started_at TEXT NULL,
    finished_at TEXT NULL,
    status TEXT NOT NULL,
    setup_sql TEXT NOT NULL,
    test_sql TEXT NOT NULL,
    request_json TEXT NOT NULL,
    response_json TEXT NULL
);

CREATE TABLE benchmark_results (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    provider TEXT NOT NULL,
    version TEXT NOT NULL,
    kind TEXT NOT NULL,
    status TEXT NOT NULL,
    compatibility TEXT NOT NULL,
    setup_elapsed_ms INTEGER NOT NULL,
    query_elapsed_ms INTEGER NOT NULL,
    row_count INTEGER NOT NULL,
    error_code TEXT NULL,
    error_message TEXT NULL,
    plan_text TEXT NULL,
    result_json TEXT NULL,
    FOREIGN KEY(job_id) REFERENCES benchmark_jobs(id)
);
```

### 17.2 Fase 2

Migrar para PostgreSQL se necessário.

---

## 18. Frontend React

### 18.1 Criar projeto

```bash
npm create vite@latest frontend -- --template react-ts
cd frontend
npm install
npm install axios @tanstack/react-query react-hook-form zod @hookform/resolvers
npm install lucide-react
```

Opcional:

```bash
npm install monaco-editor @monaco-editor/react
```

### 18.2 Estrutura

```text
frontend/src/
  api/
    client.ts
    benchmarkApi.ts

  components/
    ProviderSelector.tsx
    SqlEditor.tsx
    ResultSummaryTable.tsx
    ExecutionPlanPanel.tsx
    CompatibilityPanel.tsx
    LogsPanel.tsx

  pages/
    BenchmarkPage.tsx

  types/
    benchmark.ts

  App.tsx
```

### 18.3 Tipos

```ts
export type BenchmarkTargetKind = "DbSqlLikeMem" | "RealContainer";

export type CompatibilityStatus =
  | "Compatible"
  | "CompatibleWithWarnings"
  | "Incompatible"
  | "NotTested";

export type ExecutionStatus =
  | "Success"
  | "Failed"
  | "Timeout"
  | "Cancelled";

export interface BenchmarkTargetRequest {
  provider: string;
  version: string;
  kind: BenchmarkTargetKind;
}

export interface BenchmarkRequest {
  targets: BenchmarkTargetRequest[];
  setupSql: string;
  testSql: string;
  options: {
    timeoutSeconds: number;
    maxRowsToReturn: number;
    includeResultRows: boolean;
    includeExecutionPlan: boolean;
    dropContainerAfterRun: boolean;
  };
}

export interface BenchmarkTargetResult {
  provider: string;
  version: string;
  kind: BenchmarkTargetKind;
  compatibility: CompatibilityStatus;
  status: ExecutionStatus;
  setupElapsedMs: number;
  queryElapsedMs: number;
  rowCount: number;
  errorCode?: string;
  errorMessage?: string;
  rows: Record<string, unknown>[];
  dbSqlLikeMemPlan?: DbSqlLikeMemPlan;
  logs: string[];
}

export interface DbSqlLikeMemPlan {
  rawText?: string;
  estimatedCost?: number;
  inputTables?: number;
  estimatedRowsRead?: number;
  actualRows?: number;
  selectivityPct?: number;
  rowsPerMs?: number;
  elapsedMs?: number;
  warnings: PlanWarning[];
  indexRecommendations: IndexRecommendation[];
}

export interface PlanWarning {
  code: string;
  message: string;
  severity: string;
  reason?: string;
  suggestedAction?: string;
}

export interface IndexRecommendation {
  table: string;
  suggestedIndex: string;
  reason: string;
  confidence: number;
  estimatedGainPct: number;
}
```

### 18.4 API client

```ts
import axios from "axios";

export const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL ?? "/api",
  timeout: 120_000,
});
```

### 18.5 Chamada

```ts
import { api } from "./client";
import type { BenchmarkRequest } from "../types/benchmark";

export async function runBenchmark(request: BenchmarkRequest) {
  const response = await api.post("/benchmarks/run", request);
  return response.data;
}
```

### 18.6 Componentes essenciais

#### ProviderSelector

```tsx
type Props = {
  value: string[];
  onChange: (value: string[]) => void;
};

export function ProviderSelector({ value, onChange }: Props) {
  const providers = [
    "dbsqlikemem-sqlserver:2022",
    "dbsqlikemem-postgres:17",
    "dbsqlikemem-mysql:84",
    "postgres-real:17",
    "mysql-real:8.4",
    "sqlserver-real:2022",
  ];

  return (
    <div>
      <h2>Bancos</h2>
      {providers.map((provider) => (
        <label key={provider} style={{ display: "block" }}>
          <input
            type="checkbox"
            checked={value.includes(provider)}
            onChange={(event) => {
              if (event.target.checked) {
                onChange([...value, provider]);
              } else {
                onChange(value.filter((x) => x !== provider));
              }
            }}
          />
          {provider}
        </label>
      ))}
    </div>
  );
}
```

#### SqlEditor simples

```tsx
type Props = {
  label: string;
  value: string;
  onChange: (value: string) => void;
};

export function SqlEditor({ label, value, onChange }: Props) {
  return (
    <label style={{ display: "block", marginBottom: 16 }}>
      <strong>{label}</strong>
      <textarea
        value={value}
        onChange={(event) => onChange(event.target.value)}
        spellCheck={false}
        style={{
          display: "block",
          width: "100%",
          minHeight: 220,
          fontFamily: "monospace",
          marginTop: 8,
        }}
      />
    </label>
  );
}
```

---

## 19. Dockerfile do Backend

```dockerfile
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY src/BenchmarkLab.sln ./
COPY src/Benchmark.Api/Benchmark.Api.csproj Benchmark.Api/
COPY src/Benchmark.Application/Benchmark.Application.csproj Benchmark.Application/
COPY src/Benchmark.Domain/Benchmark.Domain.csproj Benchmark.Domain/
COPY src/Benchmark.Infrastructure/Benchmark.Infrastructure.csproj Benchmark.Infrastructure/

RUN dotnet restore BenchmarkLab.sln

COPY src/ ./
RUN dotnet publish Benchmark.Api/Benchmark.Api.csproj -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime
WORKDIR /app

ENV ASPNETCORE_URLS=http://+:8080

COPY --from=build /app/publish .

EXPOSE 8080

ENTRYPOINT ["dotnet", "Benchmark.Api.dll"]
```

---

## 20. Dockerfile do Frontend

```dockerfile
FROM node:22-alpine AS build
WORKDIR /app

COPY frontend/package*.json ./
RUN npm ci

COPY frontend/ ./
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
COPY deploy/nginx.frontend.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

---

## 21. Nginx do Frontend

```nginx
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    location /api/ {
        proxy_pass http://api:8080/api/;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 120s;
    }

    location / {
        try_files $uri /index.html;
    }
}
```

---

## 22. Docker Compose da Aplicação

```yaml
services:
  api:
    build:
      context: .
      dockerfile: deploy/Dockerfile.api
    container_name: benchmark-api
    restart: unless-stopped
    environment:
      ASPNETCORE_ENVIRONMENT: Production
      BENCHMARK_MAX_CONCURRENT_JOBS: "1"
      BENCHMARK_JOB_TIMEOUT_SECONDS: "90"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
      - benchmark-data:/app/data
    networks:
      - benchmark-net

  frontend:
    build:
      context: .
      dockerfile: deploy/Dockerfile.frontend
    container_name: benchmark-frontend
    restart: unless-stopped
    depends_on:
      - api
    networks:
      - benchmark-net

  caddy:
    image: caddy:2
    container_name: benchmark-caddy
    restart: unless-stopped
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./deploy/Caddyfile:/etc/caddy/Caddyfile:ro
      - caddy-data:/data
      - caddy-config:/config
    depends_on:
      - frontend
    networks:
      - benchmark-net

networks:
  benchmark-net:
    name: benchmark-net

volumes:
  benchmark-data:
  caddy-data:
  caddy-config:
```

Atenção:

```text
/var/run/docker.sock:/var/run/docker.sock
```

Isso dá poder elevado para a API controlar Docker no host.

Mitigações mínimas:

1. não expor API administrativa sem autenticação;
2. limitar endpoints;
3. validar tudo;
4. considerar rodar o worker em serviço separado;
5. aplicar firewall;
6. não permitir comandos livres de shell;
7. remover containers sempre;
8. nunca montar volumes arbitrários enviados pelo usuário.

---

## 23. Caddyfile

```caddyfile
seudominio.org {
    reverse_proxy frontend:80
}

www.seudominio.org {
    redir https://seudominio.org{uri}
}
```

Para API separada:

```caddyfile
api.seudominio.org {
    reverse_proxy api:8080
}

seudominio.org {
    reverse_proxy frontend:80
}
```

---

## 24. Configuração da VPS

### 24.1 Criar VPS

Sistema recomendado:

```text
Ubuntu 24.04 LTS
```

Tamanho inicial:

```text
CX22
```

Se SQL Server real ficar lento ou instável:

```text
CX32
```

### 24.2 Acessar servidor

```bash
ssh root@IP_DA_VPS
```

### 24.3 Criar usuário

```bash
adduser app
usermod -aG sudo app
usermod -aG docker app
```

Se o grupo docker ainda não existir, ele será criado após instalação do Docker.

### 24.4 Atualizar servidor

```bash
apt update
apt upgrade -y
apt install -y ca-certificates curl gnupg git ufw htop unzip
```

### 24.5 Instalar Docker

```bash
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg

chmod a+r /etc/apt/keyrings/docker.gpg

. /etc/os-release

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  ${VERSION_CODENAME} stable" \
  > /etc/apt/sources.list.d/docker.list

apt update
apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

systemctl enable docker
systemctl start docker
```

### 24.6 Firewall

```bash
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH
ufw allow 80/tcp
ufw allow 443/tcp
ufw enable
ufw status
```

Não abra portas dos bancos.

---

## 25. Deploy

### 25.1 Clonar projeto

```bash
mkdir -p /opt/benchmark-lab
cd /opt/benchmark-lab

git clone https://github.com/seu-usuario/seu-repo.git .
```

### 25.2 Criar env

```bash
cat > .env << 'EOF'
ASPNETCORE_ENVIRONMENT=Production
BENCHMARK_MAX_CONCURRENT_JOBS=1
BENCHMARK_JOB_TIMEOUT_SECONDS=90
EOF
```

### 25.3 Build e subida

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

### 25.4 Logs

```bash
docker compose -f deploy/docker-compose.yml logs -f
```

### 25.5 Healthcheck

```bash
curl -I https://seudominio.org
curl https://seudominio.org/api/health
```

---

## 26. DNS

No registrador do domínio `.org`:

```text
A     @      IP_DA_VPS
A     www    IP_DA_VPS
A     api    IP_DA_VPS
```

Se usar IPv6:

```text
AAAA  @      IPV6_DA_VPS
AAAA  www    IPV6_DA_VPS
AAAA  api    IPV6_DA_VPS
```

---

## 27. Observabilidade

### 27.1 Logs

Use Serilog:

```bash
dotnet add Benchmark.Api package Serilog.AspNetCore
dotnet add Benchmark.Api package Serilog.Sinks.File
```

Config:

```json
{
  "Serilog": {
    "MinimumLevel": "Information",
    "WriteTo": [
      { "Name": "Console" },
      {
        "Name": "File",
        "Args": {
          "path": "data/logs/app-.log",
          "rollingInterval": "Day",
          "retainedFileCountLimit": 7
        }
      }
    ]
  }
}
```

### 27.2 Métricas importantes

Coletar por job:

```text
job_id
provider
version
container_start_ms
container_ready_ms
setup_ms
query_ms
cleanup_ms
rows
status
error_type
error_message
memory_limit
cpu_limit
```

### 27.3 Painel simples

No MVP, a própria UI pode mostrar:

```text
últimos 20 jobs
taxa de erro
tempo médio por banco
bancos mais lentos
queries incompatíveis
```

---

## 28. Testes

### 28.1 Testes unitários

```bash
dotnet test
```

Testar:

```text
Provider parsing
Request validation
SQL blocking rules
Plan parser
Compatibility classifier
Result mapper
```

### 28.2 Testes de integração

Rodar localmente com Docker:

```bash
docker info
dotnet test --filter Category=Integration
```

### 28.3 Testes end-to-end

```bash
npm install -D playwright
npx playwright install
```

Cenários:

```text
abrir página
selecionar DbSqlLikeMem + PostgreSQL
preencher setup
preencher query
executar
validar tabela de resultados
abrir aba de plano
validar métricas
```

---

## 29. Roadmap de Implementação

## Fase 0 — Preparação

Checklist:

```text
[ ] Criar repositório
[ ] Criar solução .NET
[ ] Criar frontend React
[ ] Criar Docker Compose local
[ ] Definir providers iniciais
[ ] Definir contrato JSON
```

## Fase 1 — DbSqlLikeMem local

```text
[ ] Criar endpoint POST /api/benchmarks/run
[ ] Implementar DbSqlLikeMemRunner
[ ] Executar setup SQL
[ ] Executar query SQL
[ ] Capturar LastExecutionPlan
[ ] Retornar métricas básicas
[ ] Mostrar resultado no React
```

Critério de pronto:

```text
Usuário executa setup + query contra DbSqlLikeMem e vê plano de execução.
```

## Fase 2 — PostgreSQL container

```text
[ ] Instalar Docker.DotNet
[ ] Subir postgres:17-alpine sob demanda
[ ] Aguardar readiness
[ ] Executar setup
[ ] Executar query
[ ] Coletar tempo
[ ] Remover container
[ ] Mostrar comparação DbSqlLikeMem vs PostgreSQL
```

Critério de pronto:

```text
A mesma query roda no mock e no PostgreSQL real.
```

## Fase 3 — MySQL container

```text
[ ] Adicionar mysql:8.4
[ ] Criar connection string
[ ] Aguardar readiness
[ ] Executar setup/query
[ ] Classificar compatibilidade
```

Critério de pronto:

```text
Query compatível mostra sucesso; query incompatível mostra erro classificado.
```

## Fase 4 — SQL Server container

```text
[ ] Adicionar SQL Server 2022
[ ] Configurar senha forte
[ ] Ajustar memória mínima do container
[ ] Aguardar readiness mais longo
[ ] Executar setup/query
```

Critério de pronto:

```text
SQL Server roda em container e retorna comparativo.
```

## Fase 5 — UI completa

```text
[ ] Multi-select de providers
[ ] Seleção por versão
[ ] Textarea setup
[ ] Textarea query
[ ] Tabela comparativa
[ ] Abas de detalhe
[ ] Exportar JSON
```

## Fase 6 — Deploy

```text
[ ] Criar VPS
[ ] Instalar Docker
[ ] Configurar firewall
[ ] Subir Docker Compose
[ ] Configurar domínio
[ ] Ativar HTTPS
[ ] Testar execução real
```

## Fase 7 — Hardening

```text
[ ] Autenticação
[ ] Rate limit
[ ] Fila
[ ] Histórico persistente
[ ] Cleanup automático
[ ] Limites por usuário/IP
[ ] Auditoria
```

---

## 30. Backlog Técnico

### Backend

```text
[ ] BenchmarkRequestValidator
[ ] ProviderCatalogService
[ ] DbSqlLikeMemRunner
[ ] RealDatabaseContainerRunner
[ ] DockerContainerService
[ ] PostgreSqlContainerStrategy
[ ] MySqlContainerStrategy
[ ] SqlServerContainerStrategy
[ ] CompatibilityClassifier
[ ] SqlSafetyValidator
[ ] ResultSetMapper
[ ] ExecutionPlanParser
[ ] BenchmarkResultAggregator
[ ] JobRepository
[ ] CleanupHostedService
```

### Frontend

```text
[ ] BenchmarkPage
[ ] ProviderSelector
[ ] VersionSelector
[ ] SqlSetupEditor
[ ] SqlTestEditor
[ ] RunButton
[ ] ResultSummaryTable
[ ] ProviderResultCard
[ ] ExecutionPlanPanel
[ ] CompatibilityPanel
[ ] LogsPanel
[ ] JsonExportButton
```

### Infra

```text
[ ] Dockerfile API
[ ] Dockerfile frontend
[ ] docker-compose.yml
[ ] Caddyfile
[ ] ufw setup
[ ] backup do SQLite local
[ ] script de cleanup Docker
[ ] script de deploy
```

---

## 31. Comandos úteis

### Ver containers temporários

```bash
docker ps -a --filter "name=bench-"
```

### Remover containers presos

```bash
docker ps -a --filter "name=bench-" --format "{{.ID}}" | xargs -r docker rm -f
```

### Ver uso de recursos

```bash
docker stats
```

### Limpar volumes não usados

```bash
docker volume prune -f
```

### Ver logs da API

```bash
docker logs -f benchmark-api
```

### Reiniciar aplicação

```bash
docker compose -f deploy/docker-compose.yml restart
```

---

## 32. Exemplo de caso de teste manual

Setup:

```sql
CREATE TABLE users (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    active INT NOT NULL
);

CREATE INDEX ix_users_active ON users(active);

INSERT INTO users (id, name, active) VALUES
(1, 'Alice', 1),
(2, 'Bob', 0),
(3, 'Carol', 1);
```

Query:

```sql
SELECT id, name
FROM users
WHERE active = 1
ORDER BY id;
```

Resultado esperado:

```text
DbSqlLikeMem: sucesso, 2 linhas, plano disponível
PostgreSQL: sucesso, 2 linhas
MySQL: sucesso, 2 linhas
SQL Server: sucesso, 2 linhas
```

---

## 33. Exemplo de incompatibilidade

Query:

```sql
SELECT TOP 10 *
FROM users;
```

Esperado:

```text
SQL Server: compatível
PostgreSQL: incompatível
MySQL: incompatível
DbSqlLikeMem SQL Server: compatível
DbSqlLikeMem PostgreSQL: incompatível ou erro de dialeto
```

Query:

```sql
SELECT *
FROM users
LIMIT 10;
```

Esperado:

```text
PostgreSQL: compatível
MySQL: compatível
SQL Server: incompatível
```

---

## 34. Critérios para considerar a aplicação funcionando

A aplicação estará pronta no MVP quando:

```text
[ ] O domínio .org abre o frontend em HTTPS
[ ] A API responde /api/health
[ ] O usuário seleciona DbSqlLikeMem e PostgreSQL real
[ ] O usuário cola setup SQL
[ ] O usuário cola query SQL
[ ] O servidor executa o setup no DbSqlLikeMem
[ ] O servidor executa a query no DbSqlLikeMem
[ ] O servidor captura LastExecutionPlan
[ ] O servidor sobe container PostgreSQL
[ ] O servidor executa setup/query no PostgreSQL
[ ] O servidor remove o container PostgreSQL
[ ] A UI mostra tempos comparativos
[ ] A UI mostra compatibilidade por banco
[ ] A UI mostra plano DbSqlLikeMem
[ ] A UI mostra erro quando a query não é compatível
```

---

## 35. Ordem recomendada de implementação

Não comece pelo deploy.

Comece nesta ordem:

```text
1. Backend executando DbSqlLikeMem local.
2. Endpoint POST /api/benchmarks/run.
3. Frontend simples chamando endpoint.
4. PostgreSQL real via Docker.DotNet.
5. Comparativo lado a lado.
6. MySQL real.
7. SQL Server real.
8. Segurança e limites.
9. Deploy no VPS.
10. Domínio e HTTPS.
```

---

## 36. Decisão final

Para colocar em produção com menor custo e com o requisito de subir containers sob demanda:

```text
Hospedagem: Hetzner Cloud CX22
Sistema: Ubuntu 24.04 LTS
Runtime: Docker Engine + Docker Compose
Proxy/HTTPS: Caddy
Backend: ASP.NET Core 8
Frontend: React + TypeScript + Vite
Execução de bancos reais: containers temporários
MVP: DbSqlLikeMem + PostgreSQL + MySQL + SQL Server
```

Se a execução com SQL Server ficar pesada, subir para:

```text
Hetzner CX32
```

ou separar:

```text
VPS 1: frontend + API
VPS 2: workers de benchmark
```

---

## 37. Próxima fase sugerida

Depois do MVP funcional, a próxima fatia deve sair do modo "executar comparativo" e entrar em "operar comparativos com histórico".

Prioridade prática:

1. Persistir jobs e resultados com schema estável.
2. Permitir reexecução de um job salvo.
3. Permitir cancelamento e timeout por job.
4. Adicionar presets de entrada para SQL, query e bancos.
5. Registrar baseline simples para comparar execuções.
6. Adicionar autenticação básica para administração.
7. Expor exportação em JSON e CSV.

### 37.1 Modelo mínimo de persistência

Guardar o job em duas tabelas simples:

```text
BenchmarkJob
BenchmarkJobResult
```

Campos mínimos do job:

- `JobId`
- `CreatedAtUtc`
- `CreatedBy`
- `Status`
- `Profile`
- `SelectedProviders`
- `InputHash`
- `InputPayload`

Campos mínimos do resultado:

- `JobId`
- `ProviderId`
- `Engine`
- `Version`
- `Kind`
- `Success`
- `ExecutionMs`
- `Rows`
- `Compatibility`
- `PlanJson`
- `ErrorText`

### 37.2 Contrato de reexecução

Reexecutar um job deve significar:

1. reaproveitar o mesmo payload de entrada;
2. manter o mesmo `InputHash`;
3. criar um novo grupo de resultados com referência ao job anterior;
4. registrar a versão do schema de resultado usada na rodada.

### 37.3 Baseline inicial

O baseline desta fase pode começar simples:

- comparar por `BenchmarkStableId` quando existir;
- comparar por `ProviderId` e `Engine`;
- guardar apenas a última execução válida por perfil;
- marcar diferença pequena como variação normal.

### 37.4 Controle de acesso inicial

Na primeira versão, o acesso administrativo pode ser básico:

- uma conta admin única;
- login por senha forte;
- endpoints de escrita protegidos;
- leitura pública apenas para resultados já publicados.

### 37.5 API mínima da fase seguinte

Expor um conjunto pequeno de endpoints deixa a evolução previsivel:

- `POST /api/benchmarks/jobs` para criar um job;
- `GET /api/benchmarks/jobs/{jobId}` para consultar status e resumo;
- `GET /api/benchmarks/jobs/{jobId}/results` para ler resultados;
- `POST /api/benchmarks/jobs/{jobId}/rerun` para repetir a execucao;
- `POST /api/benchmarks/jobs/{jobId}/cancel` para abortar uma execucao em andamento.

### 37.6 Exportacao inicial

A exportacao pode comecar simples e util:

- JSON para integracao e debug;
- CSV para leitura rapida e planilhas;
- um arquivo por job salvo;
- um resumo agregado por perfil.

### 37.7 Fila simples

Mesmo sem worker distribuido, o fluxo deve assumir fila:

- um job em execucao por vez no MVP;
- estado `Queued`, `Running`, `Succeeded`, `Failed`, `Cancelled`;
- timeout por job como regra de seguranca;
- reexecucao sempre cria uma nova execucao filha do job original.

### 37.8 Observabilidade minima

Cada execucao precisa deixar rastros simples e uteis:

- `JobId` em todos os logs da rodada;
- transicoes de estado registradas com timestamp;
- tempo total do job e tempo por provider;
- motivo de falha ou cancelamento;
- caminho do arquivo exportado, quando houver.

### 37.9 Testes da fase

Validar primeiro o fluxo de orquestracao, sem depender de banco real:

- teste de validacao do request;
- teste de criacao e leitura do job;
- teste de reexecucao com `InputHash` estavel;
- teste de exportacao JSON;
- teste de exportacao CSV;
- teste de cancelamento por timeout.

### 37.10 Criterio de conclusao

A fase 37 fica concluida quando:

- existe um job persistido com resultados associados;
- a reexecucao reaproveita o mesmo payload;
- a API devolve status, resumo e resultados;
- a exportacao gera JSON e CSV;
- os logs permitem rastrear uma execucao do inicio ao fim.

Ponto de parada desta fase:

- primeiro fechar o modelo de persistência de job;
- depois ligar histórico, baseline e reexecução sobre o mesmo identificador;
- por fim, adicionar controle de acesso e exportação.

---

## 38. Referências externas para decisão de infra

- Hetzner CX plans: https://www.hetzner.com/pressroom/new-cx-plans/
- Google Cloud Run container contract: https://cloud.google.com/run/docs/container-contract
- DigitalOcean Droplet pricing: https://www.digitalocean.com/pricing/droplets
- Railway pricing: https://docs.railway.com/pricing
- Docker Engine install Ubuntu: https://docs.docker.com/engine/install/ubuntu/
- Caddy documentation: https://caddyserver.com/docs/
- Let's Encrypt: https://letsencrypt.org/

---

## 39. Referências internas do DbSqlLikeMem usadas no plano

Use como base no repositório:

```text
README.md
docs/getting-started.md
docs/Wiki/Providers-and-Compatibility.md
docs/old/providers-and-features.md
src/code/DbSqlLikeMem/Query/ExecutionPlan/SqlExecutionPlanFormatter.cs
src/code/DbSqlLikeMem.MySql.Test/ExecutionPlanTests.cs
```

Pontos relevantes:

```text
- Providers suportados: MySQL, MariaDB, Firebird, SQL Server, SQL Azure, Oracle, PostgreSQL, SQLite e DB2.
- Versões simuladas por provider.
- Factory de provider em runtime.
- LastExecutionPlan.
- LastExecutionPlans.
- Métricas: EstimatedCost, InputTables, EstimatedRowsRead, ActualRows, SelectivityPct, RowsPerMs, ElapsedMs.
- IndexRecommendations.
- PlanWarnings.
- PlanRiskScore.
- PlanQualityGrade.
- PlanSeverityHint.
```

---

## 40. Próximo bloco de execução

Depois da fase de histórico e exportação, a implementação deve entrar no núcleo operacional do produto.

### 40.1 Ordem prática

1. Definir o schema v1 de `BenchmarkJob` e `BenchmarkJobResult`.
2. Implementar a persistência mínima da fila.
3. Criar o runner sequencial para um job por vez.
4. Ligar o runner ao executor DbSqlLikeMem local.
5. Ligar o runner ao executor de banco real em container.
6. Publicar JSON e CSV da execução.
7. Adicionar smoke tests para request, fila e exportação.

### 40.2 Critério de parada

Pare esta fase quando:

- o job puder ser criado, executado e consultado;
- o resultado puder ser reexecutado com o mesmo `InputHash`;
- o exportador puder gerar a mesma saida em JSON e CSV;
- os logs puderem identificar cada job de ponta a ponta.

---

## 41. Esqueleto inicial da implementação

Antes de integrar com bancos reais, a base do produto deve ficar previsível e simples de testar.

### 41.1 Estrutura mínima

- `Api` para aceitar requests e expor status;
- `Application` para orquestrar jobs, regras e validações;
- `Domain` para tipos de job, resultado, compatibilidade e status;
- `Infrastructure` para persistencia, exportacao e acesso a containers;
- `Worker` opcional para execução assíncrona depois do MVP.

### 41.2 Primeiro recorte técnico

Implementar nesta ordem:

1. contratos do request e response;
2. entidade de job e resultado;
3. repositório em memória ou banco leve;
4. runner sequencial local;
5. exportação JSON;
6. exportação CSV;
7. testes do fluxo mínimo.

### 41.3 Critério de parada

Pare aqui quando:

- existir um endpoint que aceite a execução e devolva `JobId`;
- o job puder ser armazenado e consultado;
- a exportação funcionar sem banco real;
- o desenho estiver pronto para plugar PostgreSQL, MySQL e SQL Server.

---

## 42. Modelo de domínio inicial

Com a estrutura base definida, o próximo passo é fixar os tipos que atravessam API, Application e Persistência.

### 42.1 Tipos centrais

- `BenchmarkJob` para representar a execução solicitada;
- `BenchmarkJobResult` para cada resultado por provider;
- `BenchmarkExecutionRequest` para entrada da API;
- `BenchmarkExecutionResponse` para saída resumida;
- `BenchmarkCompatibilityStatus` para classificar compatibilidade;
- `BenchmarkJobStatus` para acompanhar o ciclo de vida da fila.

### 42.2 Campos que não podem faltar

- identificador estável do job;
- data de criação;
- perfil de execução;
- lista de providers selecionados;
- hash da entrada;
- payload original;
- status atual;
- versão do schema;
- estado de compatibilidade;
- erro ou observação de falha.

### 42.3 Regra de evolução

Se um campo impactar exportação, histórico ou reexecução, ele deve ser definido aqui antes de virar detalhe de infra.

### 42.4 Critério de parada

Pare esta etapa quando:

- os tipos principais estiverem descritos e estáveis;
- a API e o worker puderem compartilhar o mesmo contrato;
- o exportador puder serializar o mesmo modelo sem adaptação extra.

---

## 43. Persistência inicial

Com o modelo definido, o próximo passo é fixar como os dados serão gravados e lidos.

### 43.1 Contratos de persistência

- repositório de job para criar e consultar execuções;
- repositório de resultado para listar os resultados por `JobId`;
- unidade de trabalho simples para gravar job e resultados juntos;
- serialização do payload original sem perda de campos úteis.

### 43.2 Regras de gravação

- um `JobId` deve identificar uma execução completa;
- o resultado deve sempre referenciar o `JobId` pai;
- a persistência deve registrar o estado final da execução;
- reexecução deve gerar nova entrada sem apagar o histórico anterior.

### 43.3 Ordem de implementação

1. persistência em memória para validar fluxo;
2. persistência em banco relacional leve;
3. migração do runner para usar o repositório;
4. leitura por status e por `JobId`;
5. limpeza e retenção do histórico.

### 43.4 Critério de parada

Pare esta etapa quando:

- o job e seus resultados puderem ser salvos juntos;
- a API conseguir buscar uma execução por `JobId`;
- a reexecução não sobrescrever o histórico;
- a persistência suportar a exportação planejada.

---

## 44. Exportação inicial

Depois de persistir os dados, o próximo passo é padronizar a saída que alimenta UI, download e auditoria.

### 44.1 Formatos mínimos

- JSON como formato principal de integração;
- CSV como formato simples de leitura;
- resumo compacto para a resposta da API;
- artefato completo por job para depuração.

### 44.2 Conteúdo exportado

- identificação do job;
- timestamp de início e fim;
- lista de providers executados;
- status de cada resultado;
- tempo de execução por provider;
- compatibilidade;
- texto de erro, quando houver;
- referência ao arquivo gerado.

### 44.3 Ordem de implementação

1. exportador JSON primeiro;
2. exportador CSV em seguida;
3. serialização compartilhada entre API e arquivo;
4. validação de campos obrigatórios;
5. teste de round-trip do formato.

### 44.4 Critério de parada

Pare esta etapa quando:

- um job salvo puder ser exportado sem transformação manual;
- JSON e CSV representarem o mesmo conteúdo lógico;
- a API reutilizar os mesmos modelos do exportador;
- o resultado ficar pronto para consumo pela UI.

---

## 45. Integração mínima da API

Com persistência e exportação definidos, o próximo passo é ligar o fluxo ponta a ponta.

### 45.1 Responsabilidades da API

- validar o request de execução;
- criar o job na fila;
- disparar o runner adequado;
- devolver `JobId` e status;
- expor leitura de resultado;
- expor reexecução e cancelamento.

### 45.2 Responsabilidades do runner

- executar um job por vez no MVP;
- aplicar o payload no DbSqlLikeMem;
- opcionalmente aplicar o payload no banco real;
- gravar resultados e logs;
- chamar o exportador ao final.

### 45.3 Ordem de integração

1. endpoint de criação do job;
2. endpoint de consulta do job;
3. execução local do DbSqlLikeMem;
4. exportação automática ao finalizar;
5. leitura do resultado exportado;
6. cancelamento simples.

### 45.4 Critério de parada

Pare esta etapa quando:

- a API conseguir iniciar um job real;
- o runner conseguir concluir e persistir a execução;
- a leitura do status refletir o estado atual;
- a exportação for acionada sem etapa manual.

---

## 46. Execução em banco real

Depois da integração local, o próximo passo é ligar o mesmo fluxo aos containers temporários dos bancos reais.

### 46.1 Responsabilidades do executor real

- subir o container do provider selecionado;
- aguardar readiness;
- aplicar o script de inicialização;
- executar a query ou o comando de teste;
- coletar tempo, linhas e erro;
- destruir o container ao final.

### 46.2 Ordem de implementação

1. PostgreSQL container;
2. MySQL container;
3. SQL Server container;
4. SQLite sem container, quando aplicável;
5. Oracle e Db2 como fase posterior.

### 46.3 Critério de parada

Pare esta etapa quando:

- o mesmo job puder rodar em DbSqlLikeMem e em um banco real;
- o executor real registrar sucesso, falha ou incompatibilidade;
- o cleanup do container acontecer automaticamente;
- o resultado real entrar no mesmo contrato da exportação.

---

## 47. Compatibilidade e normalização

Depois que o fluxo de execução estiver ligado, a próxima tarefa é padronizar como os resultados são classificados.

### 47.1 Estados de compatibilidade

- `Compatible` quando o provider executa a entrada sem ressalvas;
- `CompatibleWithWarnings` quando a execução funciona, mas com observação;
- `Incompatible` quando a query ou o script não pode ser executado;
- `NotSupported` quando a capacidade não existe no provider;
- `MockOnly` quando a feature só roda na camada simulada.

### 47.2 Regras de normalização

- erros iguais devem virar a mesma categoria;
- falha de container não deve virar incompatibilidade de query;
- ausência de suporte não deve virar falha inesperada;
- resultado parcial deve registrar o motivo de forma estruturada.

### 47.3 Ordem de implementação

1. normalizar status internos do runner;
2. mapear exceções do provider para compatibilidade;
3. mapear timeout e cancelamento para estados próprios;
4. expor a classificação na API e na exportação;
5. validar a legenda na UI depois.

### 47.4 Critério de parada

Pare esta etapa quando:

- a mesma causa sempre cair no mesmo estado;
- o consumidor da API conseguir diferenciar falha, incompatibilidade e não suporte;
- a exportação preservar a classificação sem perda de sentido.

---

## 48. Visualização do resultado

Depois de normalizar os estados, o próximo passo é mostrar o resultado de um jeito legível e consistente.

### 48.1 Painel mínimo

- resumo do job;
- lista de providers executados;
- status por provider;
- tempo por execução;
- erro ou observação quando existir;
- link para JSON e CSV exportados.

### 48.2 Ordem de implementação

1. tela de resumo do job;
2. tabela de resultados por provider;
3. detalhes do erro ou incompatibilidade;
4. link para exportações;
5. destaque visual do status.

### 48.3 Critério de parada

Pare esta etapa quando:

- o usuário conseguir entender o resultado sem abrir os dados brutos;
- o estado de cada provider ficar evidente;
- a UI reutilizar a mesma classificação da API e da exportação.

---

## 49. Histórico e comparação simples

Depois da visualização, o próximo passo é guardar o que foi executado para permitir comparação rápida entre rodadas.

### 49.1 O que guardar

- data da execução;
- `JobId`;
- perfil usado;
- providers selecionados;
- tempo por provider;
- compatibilidade;
- referência ao exportador;
- resumo de erro ou observação.

### 49.2 Ordem de implementação

1. listar jobs recentes;
2. abrir detalhes de um job salvo;
3. comparar duas execuções pelo `JobId`;
4. destacar diferença de tempo e status;
5. marcar a execução mais recente como padrão da tela.

### 49.3 Critério de parada

Pare esta etapa quando:

- a UI conseguir mostrar execuções anteriores;
- duas execuções puderem ser comparadas lado a lado;
- a comparação usar os mesmos estados da API e da exportação.

---

## 50. Segurança e limites básicos

Antes de liberar uso mais amplo, a aplicação precisa de proteções mínimas no ponto de entrada.

### 50.1 Regras mínimas

- autenticação administrativa simples;
- limite de tamanho para SQL e payload;
- timeout por execução;
- bloqueio de comandos administrativos perigosos;
- rate limit por usuário ou por IP.

### 50.2 Ordem de implementação

1. autenticação básica;
2. validação de tamanho do payload;
3. timeout por job;
4. lista de comandos proibidos;
5. rate limit simples;
6. retorno padronizado para requests rejeitados.

### 50.3 Critério de parada

Pare esta etapa quando:

- requests grandes ou perigosos forem bloqueados antes da execução;
- o acesso administrativo estiver protegido;
- o usuário receber motivo claro quando a execução for rejeitada.

---

## 51. Deploy e operação mínima

Depois de segurança e limites, o próximo passo é colocar a aplicação para rodar de forma previsível.

### 51.1 Componentes de produção

- frontend servido por Nginx ou Caddy;
- API ASP.NET Core;
- worker opcional para execução assíncrona;
- banco de dados da aplicação;
- containers temporários dos providers reais.

### 51.2 Ordem de implantação

1. subir a aplicação localmente em ambiente Docker;
2. validar frontend e API juntos;
3. validar execução do job;
4. validar cleanup dos containers reais;
5. publicar em VPS;
6. validar logs e healthcheck.

### 51.3 Critério de parada

Pare esta etapa quando:

- a aplicação subir em um ambiente reproduzível;
- o frontend conversar com a API;
- o job puder executar em ambiente próximo do real;
- o healthcheck indicar estado válido.

---

## 52. Observabilidade mínima

Depois do deploy, o próximo passo é conseguir entender rapidamente o que aconteceu em uma execução.

### 52.1 Sinais mínimos

- healthcheck da API;
- logs do runner;
- logs do job;
- tempo total por execução;
- motivo de falha ou cancelamento;
- status do container real, quando houver.

### 52.2 Ordem de implementação

1. healthcheck simples;
2. logs estruturados por `JobId`;
3. métricas básicas de duração;
4. logs de erro e cancelamento;
5. painel mínimo de diagnóstico.

### 52.3 Critério de parada

Pare esta etapa quando:

- a falha puder ser rastreada por `JobId`;
- o tempo de execução puder ser visto sem depurar;
- o status do job e do container estiver claro para suporte.

---

## 53. Validação automatizada

Depois de observar o sistema, o próximo passo é garantir que o fluxo mínimo não quebre sem ser percebido.

### 53.1 Cobertura mínima

- teste do request da API;
- teste de persistência do job;
- teste do runner sequencial;
- teste de exportação JSON;
- teste de exportação CSV;
- teste de execução em banco real simulado;
- teste de normalização de compatibilidade.

### 53.2 Ordem de implementação

1. testes de domínio;
2. testes de aplicação;
3. testes de exportação;
4. testes de integração do runner;
5. smoke test do fluxo completo.

### 53.3 Critério de parada

Pare esta etapa quando:

- o fluxo mínimo estiver coberto por testes reproduzíveis;
- regressões básicas forem detectadas antes do deploy;
- a API, a persistência e a exportação estiverem alinhadas com o mesmo contrato.

---

## 54. Release e rollback

Depois da validação, o próximo passo é conseguir publicar com segurança e voltar atrás quando necessário.

### 54.1 Regras de release

- cada release deve ter versão clara;
- o schema de dados deve ter migração compatível;
- a exportação deve manter retrocompatibilidade sempre que possível;
- uma release nova não deve quebrar jobs antigos sem aviso.

### 54.2 Ordem de implementação

1. versionar a aplicação;
2. versionar o schema de dados;
3. publicar release em ambiente de staging;
4. validar o fluxo completo;
5. promover para produção;
6. manter rollback documentado.

### 54.3 Critério de parada

Pare esta etapa quando:

- a aplicação puder ser publicada com versão rastreável;
- o rollback puder ser executado sem improviso;
- jobs antigos continuarem legíveis depois da atualização.

---

## 55. Governança de mudanças

Depois de release e rollback, o próximo passo é registrar mudanças de forma que a evolução continue rastreável.

### 55.1 O que registrar

- mudança de schema;
- mudança de contrato da API;
- mudança de formato de exportação;
- mudança de status ou compatibilidade;
- mudança de comportamento de execução;
- motivo da mudança e impacto esperado.

### 55.2 Ordem de implementação

1. criar um changelog técnico;
2. registrar mudanças de schema e contrato;
3. registrar mudanças de exportação;
4. registrar mudanças de compatibilidade;
5. manter referência cruzada com a versão publicada.

### 55.3 Critério de parada

Pare esta etapa quando:

- toda mudança relevante tiver registro curto e rastreável;
- uma versão antiga puder ser comparada com uma nova sem ambiguidade;
- o histórico do produto explicar por que uma mudança ocorreu.

---

## 56. Depreciação e limpeza

Depois de registrar mudanças, o próximo passo é definir como remover ou aposentar partes antigas sem perder rastreabilidade.

### 56.1 Regras básicas

- nada relevante deve ser removido sem aviso;
- itens antigos devem passar por estado `Deprecated` antes de sumir;
- o histórico deve continuar consultável depois da remoção;
- a exportação e a API devem indicar quando algo foi depreciado.

### 56.2 Ordem de implementação

1. marcar itens antigos como depreciados;
2. documentar o substituto ou o motivo da remoção;
3. atualizar testes e exportadores afetados;
4. remover apenas depois de registrar o histórico;
5. limpar referências obsoletas quando não houver mais uso.

### 56.3 Critério de parada

Pare esta etapa quando:

- itens antigos puderem ser aposentados sem ambiguidade;
- o histórico continuar interpretável;
- a aplicação não confundir recurso ativo com recurso depreciado.

---

## 57. Encerramento do ciclo inicial

Antes de iniciar uma nova rodada de expansão, o MVP precisa fechar o primeiro ciclo com um estado estável e verificável.

### 57.1 O que deve estar pronto

- execução local do DbSqlLikeMem;
- execução em banco real via container;
- persistência de job e resultado;
- exportação JSON e CSV;
- classificação de compatibilidade;
- visualização básica do resultado;
- observabilidade mínima;
- testes automatizados do fluxo principal.

### 57.2 Ordem de revisão final

1. revisar contrato da API;
2. revisar schema de persistência;
3. revisar exportadores;
4. revisar classificação de status;
5. revisar UI e logs;
6. revisar testes de regressão.

### 57.3 Critério de fechamento

Considere este ciclo fechado quando:

- um job puder ser criado, executado, persistido, exportado e visualizado;
- o mesmo fluxo funcionar para DbSqlLikeMem e para banco real;
- o histórico e o rollback estiverem documentados;
- o próximo passo puder começar sem reabrir o desenho base.

---

## 58. Próxima fase de evolução

Depois do ciclo inicial fechado, a próxima etapa deve reduzir atrito operacional e preparar uso mais amplo.

### 58.1 Prioridades da fase 2

- fila persistente com worker separado;
- reexecução assíncrona;
- cache de imagens e containers;
- rate limit por usuário e por banco;
- histórico por perfil e por versão;
- comparação entre execuções recentes.

### 58.2 Ordem de implementação

1. separar o worker da API;
2. persistir fila em banco real ou Redis;
3. mover reexecução para processamento assíncrono;
4. adicionar histórico consultável;
5. reforçar rate limit e timeouts;
6. revisar exportação e painel com dados históricos.

### 58.3 Critério de parada

Pare esta etapa quando:

- a API puder enfileirar e acompanhar jobs sem bloquear a request;
- o worker puder processar execuções em paralelo controlado;
- o histórico puder ser filtrado por período, provider e perfil;
- o uso operacional ficar mais estável do que no ciclo inicial.

---

## 59. Próxima fase de escala

Depois da fase 2 estabilizada, a aplicação pode crescer sem mudar o núcleo funcional.

### 59.1 Prioridades da fase 3

- separar frontend e API em deploys independentes;
- dedicar worker para execuções pesadas;
- adicionar múltiplos nós de execução quando necessário;
- ampliar providers suportados por perfil;
- melhorar caching e reutilização de imagens;
- reforçar observabilidade e retenção histórica.

### 59.2 Ordem de implementação

1. separar frontend do backend;
2. isolar worker em processo ou serviço próprio;
3. habilitar execução distribuída controlada;
4. ajustar retenção de histórico e exportações;
5. revisar custos e limites de infraestrutura;
6. ampliar suporte para novos providers e perfis.

### 59.3 Critério de parada

Pare esta etapa quando:

- a aplicação puder escalar sem acoplamento entre UI e processamento;
- o worker puder ser expandido sem mexer na API principal;
- novos providers puderem entrar sem refatorar o fluxo base.

---

## 60. Operação contínua

Depois da escala, o foco passa a ser manter a aplicação estável e previsível no dia a dia.

### 60.1 Prioridades da operação

- monitorar custo por job e por provider;
- revisar filas, timeouts e cancelamentos;
- acompanhar falhas recorrentes por provider;
- ajustar retenção de histórico e exports;
- manter documentação e contratos alinhados;
- revisar capacidade antes de ampliar carga.

### 60.2 Ordem de manutenção

1. revisar métricas de uso e custo;
2. tratar falhas recorrentes;
3. reduzir ruído de logs e alertas;
4. ajustar tempos limite e quotas;
5. revisar exportações e relatórios;
6. atualizar documentação operacional.

### 60.3 Critério de parada

Pare esta etapa quando:

- o uso diário estiver previsível;
- falhas recorrentes tiverem tratamento claro;
- o sistema puder operar sem ajustes manuais frequentes.

---

## 61. Revisão periódica

Com a operação estabilizada, o próximo passo é manter o plano vivo e atualizado sem perder rastreabilidade.

### 61.1 O que revisar

- backlog técnico;
- contratos da API;
- schema de persistência;
- exportações e relatórios;
- histórico de mudanças;
- custos e uso por provider.

### 61.2 Ordem de revisão

1. revisar métricas e incidentes;
2. revisar pendências do backlog;
3. revisar contratos e compatibilidade;
4. revisar documentação pública;
5. registrar decisões e pendências novas.

### 61.3 Critério de parada

Pare esta etapa quando:

- o plano estiver alinhado com a implementação atual;
- pendências antigas estiverem marcadas com estado claro;
- novas mudanças entrarem com histórico e prioridade definidos.

---

## 62. Expansão futura

Depois da revisão periódica, o que vier a seguir deve ser tratado como expansão de longo prazo, não como parte do núcleo mínimo.

### 62.1 Possíveis frentes

- suporte a mais providers e versões;
- concorrência maior no worker;
- relatórios avançados por período e provider;
- integração com alertas externos;
- dashboards operacionais mais completos;
- automação de baseline e regressão.

### 62.2 Ordem sugerida

1. ampliar providers e perfis;
2. melhorar concorrência e throughput;
3. automatizar baselines e comparações;
4. integrar alertas e notificações;
5. evoluir dashboards e relatórios;
6. revisar custo e complexidade antes de cada novo salto.

### 62.3 Critério de parada

Pare esta etapa quando:

- a expansão estiver claramente separada do núcleo mínimo;
- cada nova frente tiver impacto e custo conhecidos;
- o plano continuar fácil de seguir sem misturar manutenção com pesquisa.

---

## 63. Encerramento e próximo ciclo

Quando o núcleo mínimo estiver estável e as fases de evolução estiverem bem separadas, o plano pode ser considerado pronto para um novo ciclo de revisão.

### 63.1 Resultado esperado

- o MVP operando de ponta a ponta;
- o histórico e a exportação funcionando com consistência;
- o plano de evolução separado por fases;
- a manutenção documentada de forma simples;
- a expansão futura sem misturar com o núcleo mínimo.

### 63.2 Próxima revisão recomendada

1. revisar o backlog técnico aberto;
2. revisar o estado real da implementação;
3. retirar itens já concluídos do plano ativo;
4. registrar novas dependências ou riscos;
5. abrir um novo ciclo apenas quando o anterior estiver estável.

### 63.3 Critério de encerramento

Considere o plano fechado para este ciclo quando:

- o conteúdo refletir a implementação atual;
- o próximo passo estiver claramente classificado como manutenção, evolução ou expansão;
- o backlog puder ser reaberto sem perder o contexto anterior.

---

## 64. Próximo ciclo recomendado

Quando o plano atual estiver fechado, o próximo ciclo deve começar pequeno e com foco em estabilidade.

### 64.1 Primeiros itens

- revisar o backlog aberto e remover itens já concluídos;
- confirmar o estado real da API, worker e exportação;
- priorizar automatização de regressão e baseline;
- separar demandas de manutenção das demandas de expansão;
- registrar dependências novas antes de iniciar qualquer refactor.

### 64.2 Ordem sugerida

1. limpar pendências antigas;
2. atualizar o backlog com o estado real;
3. definir o próximo alvo de estabilidade;
4. só então abrir expansão de capacidade ou de providers.

### 64.3 Critério de início

Comece este ciclo quando:

- o plano anterior estiver consistente com a implementação atual;
- houver um backlog enxuto e classificado;
- a próxima meta estiver clara e pequena o suficiente para ser executada sem reabrir o desenho base.

---

## 65. Limpeza inicial do backlog

O primeiro passo do novo ciclo é deixar apenas o que ainda precisa de trabalho ativo.

### 65.1 O que limpar

- itens já concluídos no plano;
- duplicatas entre backlog e roadmap;
- pendências sem dono claro;
- anotações antigas sem impacto prático;
- referências que já mudaram de fase.

### 65.2 Ordem de limpeza

1. marcar concluídos;
2. mover itens históricos para referência;
3. remover duplicatas;
4. agrupar pendências por fase;
5. deixar só o trabalho ativo visível no topo.

### 65.3 Critério de parada

Pare esta etapa quando:

- o backlog mostrar apenas trabalho ativo ou claramente histórico;
- não houver item duplicado competindo com o mesmo objetivo;
- a próxima execução puder começar sem dúvida sobre prioridade.

---

## 66. Priorização do backlog ativo

Com o backlog limpo, o próximo passo é ordenar o trabalho por impacto e dependência.

### 66.1 Critérios de prioridade

- desbloqueio do fluxo principal;
- redução de risco operacional;
- melhora de observabilidade;
- redução de acoplamento;
- impacto direto em usuários ou manutenção;
- dependências de outras tarefas.

### 66.2 Ordem de priorização

1. itens que destravam o fluxo principal;
2. itens que reduzem risco ou regressão;
3. itens que melhoram diagnóstico e suporte;
4. itens que simplificam manutenção;
5. itens de expansão futura.

### 66.3 Critério de parada

Pare esta etapa quando:

- o backlog ativo estiver em ordem de execução;
- cada item tiver motivo de prioridade claro;
- a próxima tarefa puder ser escolhida sem ambiguidade.

---

## 67. Seleção do próximo item

Depois de priorizar o backlog, o próximo passo é escolher um item pequeno e de alto impacto para execução imediata.

### 67.1 Regra de escolha

- escolher o item com maior desbloqueio e menor risco;
- evitar itens que exijam desenho novo antes de entregar valor;
- preferir algo que possa ser concluído em um ciclo curto;
- manter dependências explícitas antes de começar.

### 67.2 Ordem de escolha

1. item que destrava o fluxo principal;
2. item que reduz regressão ou risco;
3. item que melhora diagnóstico;
4. item que simplifica manutenção;
5. item de expansão apenas se estiver pronto para entrar.

### 67.3 Critério de parada

Pare esta etapa quando:

- um único próximo item estiver claramente selecionado;
- o motivo da escolha estiver documentado;
- a execução seguinte puder começar sem reavaliar o backlog inteiro.

---

## 68. Execução do item selecionado

Com o próximo item definido, o foco agora é entregar uma pequena melhora completa, não abrir uma nova linha de trabalho.

### 68.1 Como executar

- manter o escopo curto;
- alterar apenas o que estiver ligado ao item escolhido;
- preservar contratos já definidos;
- atualizar o plano somente se a execução mudar o estado real do backlog.

### 68.2 Ordem de execução

1. implementar a menor alteração útil;
2. validar o impacto no backlog e no plano;
3. registrar o novo estado;
4. parar quando o item estiver concluído ou claramente bloqueado.

### 68.3 Critério de parada

Pare esta etapa quando:

- o item escolhido estiver concluído;
- o bloqueio, se existir, estiver documentado com causa concreta;
- o próximo passo puder ser definido sem ambiguidade.

---

## 69. Validação do item executado

Depois de executar um item, o próximo passo é confirmar que ele realmente trouxe o efeito esperado.

### 69.1 O que validar

- contrato não quebrou;
- backlog refletiu o novo estado;
- documentação ficou alinhada;
- nenhum item lateral foi alterado sem necessidade;
- o próximo passo segue claro.

### 69.2 Ordem de validação

1. conferir o efeito direto da mudança;
2. conferir o impacto no plano e no backlog;
3. registrar qualquer desvio ou pendência;
4. preparar o próximo item do ciclo.

### 69.3 Critério de parada

Pare esta etapa quando:

- a mudança estiver confirmada como útil;
- o backlog e o plano estiverem consistentes;
- não houver dúvida sobre o próximo item.

---

## 70. Preparação da próxima rodada

Depois de validar o item executado, o próximo passo é deixar a próxima rodada pronta para começar sem retrabalho.

### 70.1 O que preparar

- próximo item já selecionado;
- dependências já mapeadas;
- riscos conhecidos e anotados;
- impacto no backlog revisado;
- critério de conclusão atualizado.

### 70.2 Ordem de preparação

1. confirmar o item seguinte;
2. listar dependências mínimas;
3. registrar riscos e bloqueios;
4. definir a menor entrega útil;
5. iniciar a próxima rodada apenas com escopo fechado.

### 70.3 Critério de parada

Pare esta etapa quando:

- a próxima rodada estiver pronta para começar;
- o escopo estiver pequeno e claro;
- o caminho de execução não exigir replanejamento amplo.

---

## 71. Acompanhamento da rodada

Depois de preparar a próxima rodada, o foco passa a ser acompanhar a execução sem perder rastreabilidade.

### 71.1 O que acompanhar

- estado do item em execução;
- bloqueios que surgirem durante a rodada;
- mudanças laterais no backlog;
- impacto no plano e na documentação;
- necessidade de replanejamento.

### 71.2 Ordem de acompanhamento

1. registrar início da rodada;
2. acompanhar progresso e bloqueios;
3. ajustar apenas o que for necessário;
4. registrar conclusão ou interrupção;
5. atualizar o próximo passo.

### 71.3 Critério de parada

Pare esta etapa quando:

- a rodada tiver estado claro;
- bloqueios estiverem documentados;
- o próximo passo estiver pronto para ser escolhido ou iniciado.

---

## 72. Fechamento da rodada

Quando a rodada termina, o próximo passo é consolidar o resultado antes de abrir nova tarefa.

### 72.1 O que fechar

- conclusão do item;
- pendências remanescentes;
- ajustes de documentação;
- impactos no backlog;
- decisão sobre o próximo passo.

### 72.2 Ordem de fechamento

1. registrar o resultado final;
2. apontar pendências ou bloqueios;
3. atualizar backlog e plano;
4. definir se a próxima ação é manutenção, evolução ou expansão;
5. encerrar a rodada.

### 72.3 Critério de parada

Pare esta etapa quando:

- o resultado estiver consolidado;
- as pendências estiverem explícitas;
- a próxima ação estiver classificada sem dúvida.

---

## 73. Retrospectiva curta

Depois de fechar a rodada, vale registrar o que ajudou, o que atrapalhou e o que deve mudar na próxima execução.

### 73.1 O que registrar

- o que entrou no escopo e funcionou bem;
- o que exigiu ajuste de rota;
- o que ficou bloqueado e por quê;
- o que deve ser evitado na próxima rodada;
- o que merece repetição por ter dado certo.

### 73.2 Ordem da retrospectiva

1. listar pontos positivos;
2. listar pontos de fricção;
3. apontar bloqueios e causas;
4. registrar melhorias para o próximo ciclo;
5. atualizar o plano se necessário.

### 73.3 Critério de parada

Pare esta etapa quando:

- o aprendizado da rodada estiver registrado;
- a próxima rodada puder começar com menos incerteza;
- o plano refletir o que realmente aconteceu.

---

## 74. Atualização do plano

Depois da retrospectiva, o próximo passo é aplicar o aprendizado ao documento para que o plano continue fiel ao estado real.

### 74.1 O que atualizar

- seções concluídas;
- seções bloqueadas;
- prioridades do backlog;
- dependências descobertas;
- fases que mudaram de ordem;
- pendências que ganharam novo contexto.

### 74.2 Ordem de atualização

1. refletir o estado real no plano;
2. ajustar o backlog vinculado;
3. mover itens concluídos para histórico;
4. reclassificar prioridades se necessário;
5. registrar a revisão feita.

### 74.3 Critério de parada

Pare esta etapa quando:

- o documento representar o estado atual sem distorção;
- o backlog estiver sincronizado com o plano;
- a próxima rodada puder iniciar sem dúvida sobre o cenário atual.

---

## 75. Histórico consolidado da rodada

Depois de atualizar o plano, o próximo passo é consolidar o que foi aprendido em um resumo estável.

### 75.1 O que registrar

- item executado ou validado;
- resultado final da rodada;
- bloqueios encontrados;
- mudanças no plano e no backlog;
- decisão sobre o próximo ciclo.

### 75.2 Ordem de consolidação

1. resumir a rodada em poucas linhas;
2. registrar o item e o resultado;
3. apontar bloqueios e pendências;
4. guardar a decisão do próximo passo;
5. fechar a rodada no histórico.

### 75.3 Critério de parada

Pare esta etapa quando:

- o histórico da rodada estiver claro e curto;
- o resultado puder ser entendido sem ler todo o detalhamento;
- o próximo ciclo tiver base suficiente para começar.

---

## 76. Resumo executivo do ciclo

Quando a rodada e o histórico estiverem fechados, vale manter um resumo curto para leitura rápida.

### 76.1 O que resumir

- estado do núcleo mínimo;
- fase atual do plano;
- último item executado;
- próximos dois passos;
- pendências abertas que realmente importam.

### 76.2 Ordem do resumo

1. registrar o estado atual;
2. apontar o item mais recente;
3. listar o próximo passo e o seguinte;
4. destacar bloqueios críticos;
5. guardar o resumo em formato fácil de revisar.

### 76.3 Critério de parada

Pare esta etapa quando:

- o ciclo puder ser entendido em poucos segundos;
- o resumo apontar claramente a próxima ação;
- não houver necessidade de reler toda a trilha para decidir o que fazer.

---

## 77. Revisão de curto prazo

Depois do resumo executivo, o próximo passo é verificar se a próxima ação ainda é a correta antes de executar.

### 77.1 O que revisar

- se a prioridade continua válida;
- se o próximo item ainda é pequeno o bastante;
- se surgiram bloqueios novos;
- se o contexto mudou desde a última rodada;
- se o resumo precisa ser ajustado.

### 77.2 Ordem da revisão

1. ler o resumo executivo;
2. comparar com o estado atual do backlog;
3. confirmar se a próxima ação continua válida;
4. registrar qualquer mudança de prioridade;
5. seguir para execução ou replanejamento.

### 77.3 Critério de parada

Pare esta etapa quando:

- a próxima ação estiver confirmada ou substituída;
- a prioridade atual estiver clara;
- não houver dúvida sobre o que fazer em seguida.

---

## 78. Replanejamento rápido

Quando a revisão mostrar mudança de contexto, o próximo passo é ajustar a rota sem reconstruir todo o plano.

### 78.1 O que ajustar

- prioridade do item seguinte;
- dependências que mudaram;
- bloqueios novos;
- escopo da próxima entrega;
- registro de impacto no backlog.

### 78.2 Ordem do replanejamento

1. identificar o que mudou;
2. ajustar a próxima ação;
3. registrar o motivo da alteração;
4. atualizar backlog e plano;
5. voltar para execução com escopo novo.

### 78.3 Critério de parada

Pare esta etapa quando:

- o próximo passo estiver reconfigurado;
- o motivo da mudança estiver documentado;
- o plano voltar a ficar executável sem ambiguidade.

---

## 79. Retorno à execução

Depois do replanejamento, o próximo passo é voltar a executar sem perder o contexto novo.

### 79.1 O que executar

- a próxima ação já reconfigurada;
- o menor escopo útil possível;
- as dependências já confirmadas;
- as mudanças documentadas na etapa anterior.

### 79.2 Ordem de execução

1. retomar o item ajustado;
2. validar o impacto do novo contexto;
3. registrar qualquer novo bloqueio;
4. manter o plano sincronizado com a execução;
5. parar ao concluir ou bloquear.

### 79.3 Critério de parada

Pare esta etapa quando:

- a nova rota estiver em andamento;
- o efeito do replanejamento estiver visível;
- o próximo passo estiver claro novamente.

---

## 80. Checagem pós-retomada

Depois de voltar para a execução, vale confirmar rapidamente se a nova rota continua válida.

### 80.1 O que checar

- se o item retomado ainda é o mais importante;
- se as dependências continuam válidas;
- se surgiu algum bloqueio novo;
- se o escopo ainda está pequeno o suficiente;
- se o backlog continua sincronizado.

### 80.2 Ordem da checagem

1. verificar o item retomado;
2. confirmar dependências;
3. registrar novos riscos;
4. ajustar o plano apenas se necessário;
5. seguir para conclusão ou nova decisão.

### 80.3 Critério de parada

Pare esta etapa quando:

- a rota retomada estiver confirmada;
- não houver nova divergência de contexto;
- a execução puder seguir sem nova reavaliação ampla.

---

## 81. Fechamento operacional da rodada

Depois da checagem pós-retomada, o próximo passo é encerrar a rodada com um estado simples de interpretar.

### 81.1 O que fechar

- resultado do item retomado;
- bloqueios remanescentes;
- ajuste de backlog se houver;
- atualização do resumo executivo;
- decisão sobre a próxima ação.

### 81.2 Ordem de fechamento

1. registrar o resultado final da rodada;
2. consolidar bloqueios e pendências;
3. ajustar backlog e resumo;
4. confirmar a próxima ação;
5. encerrar a rodada.

### 81.3 Critério de parada

Pare esta etapa quando:

- a rodada estiver encerrada de forma clara;
- o próximo passo estiver explícito;
- o plano puder ser lido sem ambiguidade sobre o estado atual.

---

## 82. Preparação do próximo ciclo

Depois de encerrar a rodada, o último passo é deixar o próximo ciclo pronto sem misturar com o estado anterior.

### 82.1 O que preparar

- resumo final da rodada;
- itens que ficam para o próximo ciclo;
- dependências novas descobertas;
- prioridade inicial do próximo ciclo;
- registro do estado em que o plano foi deixado.

### 82.2 Ordem de preparação

1. guardar o resumo final;
2. separar o que fica para depois;
3. registrar dependências e riscos;
4. definir a prioridade de arranque;
5. iniciar o próximo ciclo apenas com contexto limpo.

### 82.3 Critério de parada

Pare esta etapa quando:

- o próximo ciclo estiver pronto para começar;
- o contexto anterior tiver sido consolidado;
- não houver dúvida sobre o ponto de partida.

---

## 83. Arquivo mestre de referência

Depois de preparar o próximo ciclo, vale manter este documento como ponto central de consulta e alinhamento.

### 83.1 O que manter aqui

- resumo da estratégia;
- fases do ciclo atual;
- decisões consolidadas;
- histórico curto das últimas rodadas;
- ponte para backlog e roadmap relacionados.

### 83.2 Ordem de manutenção

1. manter o documento limpo e legível;
2. apontar claramente o que está ativo e o que é histórico;
3. evitar duplicar conteúdo que já vive melhor em backlog;
4. atualizar apenas o que muda o estado real;
5. usar este arquivo como referência de navegação.

### 83.3 Critério de parada

Pare esta etapa quando:

- o arquivo puder ser usado como visão central do plano;
- o backlog e o roadmap complementarem, sem duplicar, o que está aqui;
- a manutenção do documento ficar simples de revisar.

---

## 84. Ponto de entrada do próximo ciclo

Quando este ciclo terminar, a próxima rodada deve começar a partir de um ponto explícito e pequeno.

### 84.1 O que marcar

- próximo item selecionado;
- dependências mínimas;
- riscos já conhecidos;
- estado do backlog;
- fase do plano em que a nova rodada começa.

### 84.2 Ordem de marcação

1. registrar o próximo item;
2. apontar o contexto mínimo;
3. confirmar a fase de entrada;
4. revisar dependências;
5. iniciar apenas quando o escopo estiver claro.

### 84.3 Critério de parada

Pare esta etapa quando:

- a próxima rodada tiver um ponto inicial inequívoco;
- o contexto necessário estiver descrito;
- não houver dúvida sobre onde retomar.

---

## 85. Início controlado

Depois de definir o ponto de entrada, a próxima rodada deve começar com escopo reduzido e objetivo único.

### 85.1 O que iniciar

- item pequeno e de alto impacto;
- dependências já validadas;
- contexto já resumido;
- estado do backlog alinhado;
- regra de parada clara desde o início.

### 85.2 Ordem de início

1. confirmar o item escolhido;
2. ler o contexto resumido;
3. checar dependências;
4. executar a menor entrega útil;
5. parar assim que o objetivo for alcançado ou bloqueado.

### 85.3 Critério de parada

Pare esta etapa quando:

- a nova rodada tiver começado com escopo pequeno;
- o objetivo estiver claro e verificável;
- a execução puder seguir sem ampliar o plano.

---

## 86. Primeiro checkpoint

Depois do início controlado, o próximo passo é registrar um checkpoint simples para confirmar que a rodada segue no caminho certo.

### 86.1 O que conferir

- se o item escolhido continua correto;
- se o escopo segue pequeno;
- se surgiram bloqueios novos;
- se a prioridade mudou;
- se o próximo checkpoint já pode ser previsto.

### 86.2 Ordem do checkpoint

1. revisar o andamento inicial;
2. confirmar o item em execução;
3. registrar riscos ou desvios;
4. ajustar apenas o necessário;
5. seguir para a próxima etapa ou encerrar.

### 86.3 Critério de parada

Pare esta etapa quando:

- o início da rodada estiver validado;
- o andamento estiver coerente com o plano;
- o próximo ponto de controle estiver claro.

---

## 87. Checkpoint de meio de rodada

Depois do primeiro checkpoint, o próximo passo é validar se a execução continua saudável no meio do caminho.

### 87.1 O que validar

- progresso real versus esperado;
- bloqueios acumulados;
- necessidade de ajuste de escopo;
- impacto no backlog;
- necessidade de replanejamento parcial.

### 87.2 Ordem do checkpoint

1. comparar progresso com o objetivo;
2. identificar desvios relevantes;
3. decidir se segue, ajusta ou para;
4. registrar a decisão;
5. voltar à execução ou fechar a rodada.

### 87.3 Critério de parada

Pare esta etapa quando:

- a saúde da rodada estiver clara;
- o desvio estiver tratado ou aceito;
- o próximo passo estiver decidido sem ambiguidade.

---

## 88. Fechamento parcial da trilha

Depois dos checkpoints, o próximo passo é consolidar o que já foi validado sem encerrar o plano inteiro.

### 88.1 O que consolidar

- o que foi concluído na rodada;
- o que ficou bloqueado;
- o que mudou no plano;
- o que já pode ser reusado no próximo ciclo;
- o que precisa permanecer em observação.

### 88.2 Ordem de consolidação

1. registrar o estado parcial;
2. apontar o que já está estável;
3. listar o que ainda pede atenção;
4. guardar o aprendizado útil;
5. seguir para o próximo ciclo ou nova rodada.

### 88.3 Critério de parada

Pare esta etapa quando:

- o estado parcial estiver claro;
- o que já foi validado puder ser reaproveitado;
- a próxima etapa não exigir rediscutir o que já ficou estável.

---

## 89. Próxima decisão

Depois do fechamento parcial, o próximo passo é decidir com clareza qual direção continua valendo para a rodada seguinte.

### 89.1 O que decidir

- se a rodada continua na mesma direção;
- se o escopo precisa ser reduzido;
- se alguma dependência nova mudou a prioridade;
- se o próximo ciclo deve recomeçar ou seguir;
- se existe bloqueio que pede replanejamento maior.

### 89.2 Ordem da decisão

1. revisar o estado parcial;
2. comparar com o objetivo atual;
3. confirmar continuidade ou ajuste;
4. registrar a decisão;
5. seguir para execução ou novo planejamento.

### 89.3 Critério de parada

Pare esta etapa quando:

- a direção seguinte estiver definida;
- a prioridade estiver clara;
- não houver dúvida sobre a próxima ação.

---

## 90. Execução da próxima decisão

Depois de decidir a direção, o próximo passo é iniciar a ação escolhida sem reabrir o debate.

### 90.1 O que executar

- a opção decidida na etapa anterior;
- o menor escopo suficiente para gerar progresso;
- as dependências já confirmadas;
- o ajuste de backlog necessário;
- a regra de parada associada ao item.

### 90.2 Ordem de execução

1. iniciar a ação escolhida;
2. manter o escopo reduzido;
3. registrar bloqueios novos;
4. atualizar o estado do plano se necessário;
5. parar ao concluir ou bloquear.

### 90.3 Critério de parada

Pare esta etapa quando:

- a próxima decisão estiver em execução;
- o progresso estiver visível;
- o próximo checkpoint puder ser definido sem ambiguidade.

---

## 91. Confirmação de progresso

Depois de iniciar a execução, o próximo passo é confirmar se a mudança está realmente avançando.

### 91.1 O que confirmar

- se o item continua no caminho certo;
- se o escopo permanece pequeno;
- se o progresso é mensurável;
- se surgiram novos bloqueios;
- se o próximo checkpoint continua válido.

### 91.2 Ordem de confirmação

1. medir o avanço atual;
2. comparar com a meta do item;
3. registrar bloqueios e desvios;
4. ajustar apenas o necessário;
5. seguir para conclusão ou novo checkpoint.

### 91.3 Critério de parada

Pare esta etapa quando:

- o progresso estiver confirmado;
- os desvios estiverem claros;
- o próximo passo estiver pronto para ser validado.

---

## 92. Ajuste fino da execução

Depois de confirmar o progresso, o próximo passo é fazer apenas o ajuste mínimo necessário para manter a rodada saudável.

### 92.1 O que ajustar

- pequenos desvios de escopo;
- dependências que ainda não fecharam;
- bloqueios leves;
- ordem das tarefas restantes;
- detalhes de documentação que ficaram pendentes.

### 92.2 Ordem do ajuste

1. identificar o ajuste mínimo;
2. aplicar sem ampliar o escopo;
3. registrar o impacto;
4. manter o plano sincronizado;
5. seguir para o próximo checkpoint.

### 92.3 Critério de parada

Pare esta etapa quando:

- o ajuste mínimo tiver sido aplicado;
- o escopo continuar controlado;
- a rodada seguir estável para o próximo passo.

---

## 93. Checkpoint final da rodada

Depois do ajuste fino, o próximo passo é registrar um checkpoint final para encerrar a rodada sem perder rastreabilidade.

### 93.1 O que conferir

- se o objetivo da rodada foi atingido;
- se o ajuste mínimo foi suficiente;
- se restaram bloqueios relevantes;
- se o backlog ficou coerente;
- se o próximo ciclo já pode ser iniciado.

### 93.2 Ordem do checkpoint

1. revisar o estado final;
2. confirmar o que foi entregue;
3. listar pendências restantes;
4. registrar a decisão de encerramento;
5. liberar a próxima fase ou ciclo.

### 93.3 Critério de parada

Pare esta etapa quando:

- a rodada estiver formalmente encerrada;
- o estado final estiver claro;
- o próximo ciclo puder ser iniciado sem ambiguidade.

---

## 94. Retomada controlada

Depois do checkpoint final, o próximo passo é reiniciar a execução com contexto limpo e escopo reduzido.

### 94.1 O que retomar

- item pequeno e bem definido;
- dependências já confirmadas;
- prioridade válida para o ciclo novo;
- backlog sincronizado com o estado final;
- resumo executivo atualizado.

### 94.2 Ordem de retomada

1. confirmar o contexto limpo;
2. escolher o próximo item;
3. validar dependências e riscos;
4. iniciar o novo ciclo com escopo pequeno;
5. manter checkpoints curtos desde o início.

### 94.3 Critério de parada

Pare esta etapa quando:

- a retomada estiver em andamento;
- o contexto anterior não estiver mais interferindo;
- o novo ciclo tiver ponto de controle claro.

---

## 95. Confirmação do novo ciclo

Depois da retomada controlada, o próximo passo é confirmar que o ciclo novo realmente começou com base limpa.

### 95.1 O que confirmar

- item ativo escolhido;
- escopo pequeno;
- backlog atualizado;
- checkpoints previstos;
- ausência de conflito com o ciclo anterior.

### 95.2 Ordem da confirmação

1. checar o item ativo;
2. validar o escopo;
3. confirmar o estado do backlog;
4. registrar o primeiro checkpoint;
5. seguir para execução curta.

### 95.3 Critério de parada

Pare esta etapa quando:

- o novo ciclo estiver validado;
- o escopo permanecer reduzido;
- o próximo checkpoint estiver pronto para uso.

---

## 96. Execução curta do novo ciclo

Depois de confirmar o ciclo novo, o próximo passo é executar uma tarefa pequena e bem delimitada.

### 96.1 O que executar

- item ativo e prioritário;
- escopo mínimo viável;
- dependências já validadas;
- checkpoints curtos;
- registro claro de conclusão ou bloqueio.

### 96.2 Ordem de execução

1. iniciar a tarefa escolhida;
2. manter o escopo curto;
3. registrar progresso e bloqueios;
4. atualizar o backlog se necessário;
5. parar assim que a entrega útil estiver pronta.

### 96.3 Critério de parada

Pare esta etapa quando:

- a tarefa pequena estiver concluída ou bloqueada;
- o progresso estiver rastreável;
- o próximo checkpoint puder ser acionado sem dúvida.

---

## 97. Checkpoint da execução curta

Depois de iniciar a tarefa pequena, o próximo passo é confirmar rapidamente se a execução segue no trilho certo.

### 97.1 O que confirmar

- se a tarefa continua pequena;
- se o escopo segue controlado;
- se surgiram bloqueios novos;
- se o item ainda vale a pena;
- se o próximo checkpoint já pode ser previsto.

### 97.2 Ordem do checkpoint

1. revisar o andamento da tarefa;
2. comparar com o objetivo inicial;
3. registrar desvios e bloqueios;
4. ajustar somente o necessário;
5. seguir para conclusão ou nova decisão.

### 97.3 Critério de parada

Pare esta etapa quando:

- o andamento da tarefa estiver validado;
- o desvio estiver claro ou ausente;
- o próximo passo estiver evidente.

---

## 98. Consolidação do checkpoint

Depois de validar a execução curta, o próximo passo é consolidar o que ficou confirmado para não perder o contexto.

### 98.1 O que consolidar

- estado do item;
- bloqueios remanescentes;
- decisões tomadas no checkpoint;
- necessidade de ajuste no backlog;
- próximo passo sugerido.

### 98.2 Ordem de consolidação

1. registrar o estado validado;
2. apontar bloqueios ou desvios;
3. atualizar o backlog se necessário;
4. guardar a decisão do próximo passo;
5. seguir para fechamento ou nova execução.

### 98.3 Critério de parada

Pare esta etapa quando:

- o checkpoint estiver consolidado;
- o estado do item estiver claro;
- o próximo passo puder ser seguido sem nova revisão ampla.

---

## 99. Encaminhamento da próxima ação

Depois de consolidar o checkpoint, o próximo passo é encaminhar a ação seguinte sem perder a linha do ciclo.

### 99.1 O que encaminhar

- item seguinte;
- dependências restantes;
- riscos conhecidos;
- resumo do estado atual;
- critério de conclusão da próxima ação.

### 99.2 Ordem de encaminhamento

1. registrar o item seguinte;
2. apontar o contexto mínimo;
3. confirmar dependências e riscos;
4. atualizar o backlog e o resumo;
5. seguir para execução ou encerramento.

### 99.3 Critério de parada

Pare esta etapa quando:

- a próxima ação estiver encaminhada;
- o contexto necessário estiver registrado;
- o ciclo puder seguir sem dúvida sobre o rumo.
