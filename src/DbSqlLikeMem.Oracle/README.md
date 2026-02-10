# DbSqlLikeMem.Oracle

Com **`DbSqlLikeMem.Oracle`**, você pode testar fluxos de acesso Oracle de forma confiável e sem dependência de ambiente externo.

## Benefícios

- Simulação em memória para cenários Oracle
- Facilidade para testar regras de acesso a dados
- Redução do tempo de execução em pipelines
- Melhor experiência para TDD/BDD em projetos com Oracle

## Instalação

```bash
dotnet add package DbSqlLikeMem.Oracle
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.Oracle;

var conn = new OracleConnectionMock(new OracleDbMock());
conn.Open();
```

## Comunidade

Queremos construir o melhor mock Oracle para .NET, juntos. Traga exemplos, gaps de compatibilidade e PRs com testes de regressão.
