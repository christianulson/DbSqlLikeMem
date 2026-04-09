# DbSqlLikeMem

**EN:** Core package of the **DbSqlLikeMem** ecosystem: an in-memory SQL-like engine that helps you test data access code in C# with speed, repeatability, and confidence.
**PT-BR:** Pacote base do ecossistema **DbSqlLikeMem**: um motor SQL-like em memória para testar acesso a dados em C# com velocidade, previsibilidade e confiança.

## What this package provides | O que este pacote entrega

- **EN:** In-memory database structures (schema, tables, columns, indexes).
  **PT-BR:** Estruturas de banco em memória (schema, tabelas, colunas, índices).
- **EN:** SQL parser and executor for common test scenarios (DDL and DML subsets).
  **PT-BR:** Parser e executor SQL para cenários comuns de teste (subconjuntos de DDL e DML).
- **EN:** Data seeding helpers and fluent builders for setup.
  **PT-BR:** Helpers de seed e builders fluentes para setup.
- **EN:** Schema-level sequence registration plus optional identity override helpers for deterministic scenarios.
  **PT-BR:** Registro de sequences em nível de schema e helpers opcionais de sobrescrita de identity para cenários determinísticos.
- **EN:** ADO.NET-friendly behavior used by provider packages.
  **PT-BR:** Comportamento compatível com ADO.NET usado pelos pacotes de provedor.
- **EN:** Mock execution plans with lightweight runtime metrics and per-connection history.
  **PT-BR:** Planos de execução mock com métricas de runtime simplificadas e histórico por conexão.

## Typical use cases | Quando usar

Use `DbSqlLikeMem` when you want to / Use `DbSqlLikeMem` quando quiser:

- **EN:** Reduce test costs that currently rely on a real database server.
  **PT-BR:** Reduzir custo de testes que hoje dependem de banco real.
- **EN:** Build reproducible QA scenarios with deterministic data setup.
  **PT-BR:** Criar cenários de QA reproduzíveis com setup determinístico.
- **EN:** Validate query and transformation logic with fast feedback loops.
  **PT-BR:** Validar regras de query e transformação de dados com ciclo rápido.
- **EN:** Inspect query behavior in tests with simplified execution-plan metrics.
  **PT-BR:** Investigar comportamento de queries em testes com métricas simplificadas de plano.

## Installation | Instalação

```bash
dotnet add package DbSqlLikeMem
```

## Quick example | Exemplo rápido

```csharp
var db = new DbMock();
var users = db.AddTable("Users");
users.AddColumn("Id", DbType.Int32, false);
users.AddColumn("Name", DbType.String, false);
users.AddPrimaryKeyIndexes("Id");
```

## Next step: choose a provider package | Próximo passo: escolha um pacote de provedor

**EN:** Add at least one provider package to emulate your database dialect in tests (`DbSqlLikeMem.MySql`, `DbSqlLikeMem.SqlServer`, `DbSqlLikeMem.SqlAzure`, `DbSqlLikeMem.Oracle`, `DbSqlLikeMem.Npgsql`, `DbSqlLikeMem.Sqlite`, or `DbSqlLikeMem.Db2`).
**PT-BR:** Adicione ao menos um pacote de provedor para simular o dialeto do seu banco nos testes (`DbSqlLikeMem.MySql`, `DbSqlLikeMem.SqlServer`, `DbSqlLikeMem.SqlAzure`, `DbSqlLikeMem.Oracle`, `DbSqlLikeMem.Npgsql`, `DbSqlLikeMem.Sqlite` ou `DbSqlLikeMem.Db2`).

## One-call test factory | Factory de uma chamada para testes

**EN:** You can create `DbMock` + `IDbConnection` in a single call:
**PT-BR:** Você pode criar `DbMock` + `IDbConnection` com uma chamada única:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateSqliteWithTables(
    d => d.AddTable("Users",
        [new Col("Id", DataTypeDef.Int32()), new Col("Name", DataTypeDef.String())],
        [new Dictionary<int, object?> { [0] = 1, [1] = "Ana" }]));
```

**EN:** There are provider shortcuts as well: `CreateOracleWithTables`, `CreateSqlServerWithTables`, `CreateSqlAzureWithTables`, `CreateMySqlWithTables`, `CreateSqliteWithTables`, `CreateDb2WithTables`, and `CreateNpgsqlWithTables`.
**PT-BR:** Também existem atalhos por banco: `CreateOracleWithTables`, `CreateSqlServerWithTables`, `CreateSqlAzureWithTables`, `CreateMySqlWithTables`, `CreateSqliteWithTables`, `CreateDb2WithTables` e `CreateNpgsqlWithTables`.

**EN:** If preferred, use the generic string-based entry point:
**PT-BR:** Se preferir, use a entrada genérica por string:

```csharp
var (db, conn) = DbMockConnectionFactory.CreateWithTables("SqlServer", d => { /* mapeamentos */ });
```

> **EN:** Tip: the factory resolves connection types via reflection, so it works best when the provider package is already referenced and loaded by your test project.
> **PT-BR:** Dica: a factory resolve os tipos de conexão via reflexão, então funciona melhor quando o pacote de provedor já está referenciado e carregado no projeto de teste.

## Interception pipeline | Pipeline de interceptacao

**EN:** The core package now includes an initial ADO.NET interception wrapper so you can compose behaviors around any `DbConnection`.
**PT-BR:** O pacote base agora inclui um wrapper inicial de interceptacao ADO.NET para compor comportamentos em torno de qualquer `DbConnection`.

```csharp
using var intercepted = connection.WithInterceptors(
    new DelegatingDbConnectionInterceptor
    {
        OnCommandCreated = (_, command) =>
        {
            // Example: tag or rewrite SQL before execution.
            command.CommandText = $"/* test */ {command.CommandText}";
        },
        OnCommandExecuting = context =>
        {
            Console.WriteLine($"{context.ExecutionKind}: {context.Command.CommandText}");
        }
    });
```

**EN:** Current scope: connection lifecycle, transaction begin/commit/rollback, command creation, and sync/async interception for `ExecuteNonQuery`, `ExecuteScalar`, and `ExecuteReader`.
**PT-BR:** Escopo atual: ciclo de vida da conexao, inicio/commit/rollback de transacao, criacao de comando e interceptacao sync/async de `ExecuteNonQuery`, `ExecuteScalar` e `ExecuteReader`.

**EN:** SQL parsing keeps an AST cache by default; set `DBSQLLIKEMEM_AST_CACHE_SIZE=0` to disable it or raise the value to keep more parsed statements hot between calls.
**PT-BR:** O parsing SQL mantém um cache de AST por padrao; defina `DBSQLLIKEMEM_AST_CACHE_SIZE=0` para desabilita-lo ou aumente o valor para manter mais statements parseados quentes entre chamadas.

**EN:** Structured command logs can now include both `performance=` and `performanceDelta=` when the wrapped connection exposes runtime metrics.
**PT-BR:** Os logs estruturados de comando agora podem incluir tanto `performance=` quanto `performanceDelta=` quando a conexao encapsulada expõe metricas de runtime.

**EN:** If you only need in-memory tracing of what happened, use `RecordingDbConnectionInterceptor` and inspect `Events`.
**PT-BR:** Se voce precisa apenas de tracing em memoria do que aconteceu, use `RecordingDbConnectionInterceptor` e inspecione `Events`.

```csharp
var recorder = new RecordingDbConnectionInterceptor();
using var interceptedMock = cnn.Intercept(recorder);
```

**EN:** `RecordingDbConnectionInterceptor.GetFormattedEvents()` uses the same normalized text formatter consumed by the logging interceptor.
**PT-BR:** `RecordingDbConnectionInterceptor.GetFormattedEvents()` usa o mesmo formatter de texto normalizado consumido pelo interceptor de logging.

**EN:** The runtime factory also has intercepted entry points when you want `DbMock + DbConnection` ready in one call.
**PT-BR:** A factory de runtime tambem possui entradas interceptadas quando voce quer `DbMock + DbConnection` prontos em uma chamada.

```csharp
var (db, interceptedConnection) = DbMockConnectionFactory.CreateSqliteWithTablesIntercepted(
    new RecordingDbConnectionInterceptor(),
    d => { /* tables */ });
```

**EN:** The intercepted `DbMockConnectionFactory` path is now exercised across the provider factory contract tests as the baseline route for providers that do not expose dedicated ORM connection factories.
**PT-BR:** O caminho interceptado da `DbMockConnectionFactory` agora tambem e exercitado nos testes contratuais da factory por provedor como rota base para providers que nao expõem factories ORM dedicadas.

**EN:** `DbMockConnectionFactory.Create*WithTablesIntercepted(...)` now accepts either explicit interceptors or `DbInterceptionOptions`, matching the rest of the interception API.
**PT-BR:** `DbMockConnectionFactory.Create*WithTablesIntercepted(...)` agora aceita tanto interceptors explicitos quanto `DbInterceptionOptions`, alinhando-se ao restante da API de interceptacao.

**EN:** If you want to compose multiple built-in interceptors without manual wiring, use `WithInterception(...)` with `DbInterceptionOptions`.
**PT-BR:** Se voce quiser compor varios interceptors nativos sem wiring manual, use `WithInterception(...)` com `DbInterceptionOptions`.

```csharp
using var intercepted = connection.WithInterception(options =>
{
    options.EnableRecording = true;
    options.LogAction = Console.WriteLine;
    options.FaultInjection = new FaultInjectionDbConnectionInterceptor
    {
        Latency = TimeSpan.FromMilliseconds(25)
    };
});
```

**EN:** `DbInterceptionOptions` also exposes fluent helpers such as `UseRecording(...)`, `UseLogging(...)`, `UseTextWriter(...)`, `UseFaultInjection(...)`, and `AddInterceptor(...)`.
**PT-BR:** `DbInterceptionOptions` tambem expoe helpers fluentes como `UseRecording(...)`, `UseLogging(...)`, `UseTextWriter(...)`, `UseFaultInjection(...)` e `AddInterceptor(...)`.

**EN:** The static `DbInterceptionPipeline.Wrap(...)` entry point also accepts inline option configuration, keeping parity with the extension-based API.
**PT-BR:** A entrada estatica `DbInterceptionPipeline.Wrap(...)` tambem aceita configuracao inline de opcoes, mantendo paridade com a API baseada em extensions.

**EN:** If your application already creates connections through a delegate or provider factory, `WithInterceptionFactory(...)` creates an `IDbInterceptionConnectionFactory` that returns wrapped or already-opened connections, either from explicit interceptors, a prebuilt `DbInterceptionOptions`, or inline option configuration.
**PT-BR:** Se a sua aplicacao ja cria conexoes por delegate ou provider factory, `WithInterceptionFactory(...)` cria uma `IDbInterceptionConnectionFactory` que retorna conexoes encapsuladas ou ja abertas, seja a partir de interceptors explicitos, de um `DbInterceptionOptions` pronto ou de configuracao inline de opcoes.

**EN:** The first provider-specific adoption path already exists for `Sqlite`, `SqlServer`, `Npgsql`, `MySql`, `Oracle`, and `Db2` EF Core/LinqToDB factories, which now accept interceptors or interception options directly.
**PT-BR:** O primeiro caminho de adocao especifico por provedor ja existe para as factories `Sqlite`, `SqlServer`, `Npgsql`, `MySql`, `Oracle` e `Db2` de EF Core/LinqToDB, que agora aceitam interceptors ou opcoes de interceptacao diretamente.

**EN:** For dependency-injection based setups, register interceptors in `IServiceCollection` and apply them later with `WithRegisteredInterceptors(serviceProvider)`.
**PT-BR:** Para setups baseados em injecao de dependencia, registre os interceptors no `IServiceCollection` e aplique-os depois com `WithRegisteredInterceptors(serviceProvider)`.

**EN:** When you need to inspect recorded events later, provide your own `RecordingDbConnectionInterceptor` instance in `DbInterceptionOptions` so the same object can be reused or resolved from DI.
**PT-BR:** Quando voce precisar inspecionar os eventos gravados depois, forneca sua propria instancia de `RecordingDbConnectionInterceptor` em `DbInterceptionOptions` para que o mesmo objeto possa ser reutilizado ou resolvido via DI.

**EN:** For common host/test setups there are also higher-level DI helpers such as `AddDbInterceptionRecording(...)`, `AddDbInterceptionLogging(...)`, `AddDbInterceptionLogger(...)`, and `AddDbInterceptionTextWriter(...)`.
**PT-BR:** Para setups comuns de host/teste tambem existem helpers de DI em alto nivel, como `AddDbInterceptionRecording(...)`, `AddDbInterceptionLogging(...)`, `AddDbInterceptionLogger(...)` e `AddDbInterceptionTextWriter(...)`.

**EN:** When those built-in interceptors depend on services already registered in the host, `AddDbInterception((serviceProvider, options) => ...)` can compose the pipeline from the same container and still be consumed later through `WithRegisteredInterceptors(serviceProvider)`.
**PT-BR:** Quando esses interceptors nativos dependem de servicos ja registrados no host, `AddDbInterception((serviceProvider, options) => ...)` pode compor o pipeline a partir do mesmo container e ainda ser consumido depois por `WithRegisteredInterceptors(serviceProvider)`.

**EN:** DI can also register `IDbInterceptionConnectionFactory` directly through `AddDbInterceptionConnectionFactory(...)` when your host builds connections from delegates, including overloads that either resolve the registered `DbConnectionInterceptor` chain from the same `IServiceProvider` or compose `DbInterceptionOptions` from services already in the container.
**PT-BR:** O DI tambem pode registrar `IDbInterceptionConnectionFactory` diretamente por `AddDbInterceptionConnectionFactory(...)` quando o host constroi conexoes a partir de delegates, incluindo sobrecargas que tanto resolvem a cadeia registrada de `DbConnectionInterceptor` a partir do mesmo `IServiceProvider` quanto compoem `DbInterceptionOptions` a partir de servicos ja presentes no container.

**EN:** For resilience tests, `FaultInjectionDbConnectionInterceptor` can inject fixed latency or fail connection, command, and transaction paths deterministically.
**PT-BR:** Para testes de resiliencia, `FaultInjectionDbConnectionInterceptor` pode injetar latencia fixa ou falhar de forma deterministica os caminhos de conexao, comando e transacao.

**EN:** For simple structured diagnostics, `LoggingDbConnectionInterceptor` writes normalized event lines through any `Action<string>`.
**PT-BR:** Para diagnostico estruturado simples, `LoggingDbConnectionInterceptor` escreve linhas de evento normalizadas por qualquer `Action<string>`.

**EN:** `TextWriterDbConnectionInterceptor` is a lightweight bridge for `Console.Out`, `StringWriter`, or file-backed writers.
**PT-BR:** `TextWriterDbConnectionInterceptor` e uma ponte leve para `Console.Out`, `StringWriter` ou writers baseados em arquivo.

**EN:** If your application already uses the standard .NET logging stack, `ILoggerDbConnectionInterceptor` and `AddDbInterceptionLogger(...)` bridge the same formatted events to `ILogger`.
**PT-BR:** Se a sua aplicacao ja usa a pilha padrao de logging do .NET, `ILoggerDbConnectionInterceptor` e `AddDbInterceptionLogger(...)` fazem a ponte dos mesmos eventos formatados para `ILogger`.

**EN:** If you want runtime-native observability hooks without extra dependencies, `DiagnosticListenerDbConnectionInterceptor` publishes typed payloads through `DiagnosticListener`.
**PT-BR:** Se voce quiser hooks de observabilidade nativos do runtime sem dependencias extras, `DiagnosticListenerDbConnectionInterceptor` publica payloads tipados por `DiagnosticListener`.

**EN:** On modern target frameworks, `ActivitySourceDbConnectionInterceptor` also publishes activities for connection, command, and transaction flows.
**PT-BR:** Em target frameworks modernos, `ActivitySourceDbConnectionInterceptor` tambem publica activities para fluxos de conexao, comando e transacao.

**EN:** The interception wrappers stay on the standard `DbConnection` surface, so consumer libraries such as Dapper continue to work on top of intercepted mock or provider-backed connections.
**PT-BR:** Os wrappers de interceptacao permanecem na superficie padrao de `DbConnection`, entao bibliotecas consumidoras como Dapper continuam funcionando sobre conexoes mockadas ou de provider real ja interceptadas.

```csharp
var recorder = new RecordingDbConnectionInterceptor();
using var connection = new SqliteConnectionMock(new SqliteDbMock()).Intercept(recorder);
connection.Open();

var rows = connection.Query<(int Id, string Name)>(
    "select id, name from users where id = @id",
    new { id = 1 });
```

**EN:** If you already profile ADO.NET calls with MiniProfiler or another wrapper-based tool, place the interception pipeline on the same `DbConnection` chain instead of replacing the profiling wrapper.
**PT-BR:** Se voce ja perfila chamadas ADO.NET com MiniProfiler ou outra ferramenta baseada em wrappers, coloque o pipeline de interceptacao na mesma cadeia de `DbConnection` em vez de substituir o wrapper de profiling.

```csharp
var profiled = new ProfiledDbConnection(innerConnection, MiniProfiler.Current);
using var intercepted = profiled.WithInterception(options =>
{
    options.UseRecording();
    options.UseLogging(Console.WriteLine);
});
```

## Sequence quick reference | Referência rápida de sequence

- **EN:** SQL Server uses `NEXT VALUE FOR schema.seq_name`.
  **PT-BR:** SQL Server usa `NEXT VALUE FOR schema.seq_name`.
- **EN:** PostgreSQL uses `nextval`, `currval`, `setval`, and `lastval`.
  **PT-BR:** PostgreSQL usa `nextval`, `currval`, `setval` e `lastval`.
- **EN:** Oracle uses `schema.seq_name.NEXTVAL` and `schema.seq_name.CURRVAL`.
  **PT-BR:** Oracle usa `schema.seq_name.NEXTVAL` e `schema.seq_name.CURRVAL`.
- **EN:** DB2 uses `NEXT VALUE FOR schema.seq_name` and `PREVIOUS VALUE FOR schema.seq_name`.
  **PT-BR:** DB2 usa `NEXT VALUE FOR schema.seq_name` e `PREVIOUS VALUE FOR schema.seq_name`.

**EN:** See the full examples in [Getting Started](../../docs/getting-started.md).
**PT-BR:** Veja os exemplos completos em [Getting Started](../../docs/getting-started.md).

## Scope and expectations | Escopo e expectativas

- **EN:** This package is intended for tests and local validation, not as a production database replacement.
  **PT-BR:** Este pacote é voltado para testes e validação local, não para substituir banco em produção.
- **EN:** SQL support is incremental and dialect-aware through provider-specific packages.
  **PT-BR:** O suporte SQL é incremental e sensível a dialeto por meio de pacotes específicos por provedor.
- **EN:** Unsupported SQL constructs should fail fast with clear exceptions.
  **PT-BR:** Construções SQL não suportadas devem falhar rapidamente com exceções claras.
- **EN:** Execution-plan metrics are diagnostic and relative; do not use mock timings as production performance benchmarks.
  **PT-BR:** As métricas de plano de execução são diagnósticas e relativas; não use tempos do mock como benchmark de performance de produção.

## Learn more | Saiba mais

- **EN:** Full docs and guides: [Repository README](../../README.md) and [Getting Started](../../docs/getting-started.md).
  **PT-BR:** Documentação completa e guias: [README do repositório](../../README.md) e [Guia de início](../../docs/getting-started.md).

## Contributing | Contribuindo

Contributions are very welcome / Contribuições são muito bem-vindas 💙

- **EN:** Open issues with real SQL samples and expected behavior.
  **PT-BR:** Abra issues com exemplos reais de SQL e comportamento esperado.
- **EN:** Submit PRs with focused tests and clear intent.
  **PT-BR:** Envie PRs com testes objetivos e intenção clara.
- **EN:** Help improve docs and examples for new users.
  **PT-BR:** Ajude a melhorar documentação e exemplos para novos usuários.
