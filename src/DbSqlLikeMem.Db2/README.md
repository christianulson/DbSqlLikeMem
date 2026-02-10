# DbSqlLikeMem.Db2

**`DbSqlLikeMem.Db2`** leva o ecossistema DbSqlLikeMem ao universo IBM DB2, ajudando sua equipe a validar regras de acesso a dados em memória.

## Diferenciais

- Simulação voltada ao provider DB2
- Execução de testes mais rápida e previsível
- Excelente para validar comportamento de consultas sem banco real

## Instalação

```bash
dotnet add package DbSqlLikeMem.Db2
```

## Exemplo rápido

```csharp
using DbSqlLikeMem.Db2;

var conn = new Db2ConnectionMock(new Db2DbMock());
conn.Open();
```

## Evolução colaborativa

A comunidade é fundamental para ampliar cobertura DB2. Compartilhe scripts SQL reais, abra issues e envie PRs com testes para acelerar a evolução do pacote.
