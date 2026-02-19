# AI Playbook — Usar DbSqlLikeMem em projeto externo (testes de repositório/integração)

Este guia é para quando o usuário tem um projeto **XPTO** e pede para a IA criar testes de repositório (ou “integração leve”) usando o DbSqlLikeMem para validar queries sem banco real.

> Objetivo principal: dar **contexto rápido e acionável** para a IA, sem ela precisar varrer o repositório inteiro.

## 1) Contexto mínimo que a IA precisa (cheat sheet)

- O DbSqlLikeMem simula providers SQL em memória para testes.
- Providers disponíveis: **MySQL, SQL Server, Oracle, PostgreSQL (Npgsql), SQLite, DB2**.
- Uso típico no projeto do usuário:
  1. adicionar pacote NuGet do provider;
  2. criar `ConnectionMock` + `DbMock`;
  3. semear dados;
  4. executar repositório/query;
  5. validar resultado.

## 2) Quando usar este playbook

Use quando o pedido for algo como:

- “Crie testes de repositório para minhas queries”.
- “Quero testes de integração sem subir banco”.
- “Adapte meus testes Dapper/ADO.NET com mock de conexão por provider”.

## 3) Prompt pronto para o usuário pedir à IA (copy/paste)

```text
Você está me ajudando no meu projeto XPTO.

Quero criar testes de repositório (integração leve) usando DbSqlLikeMem para validar minhas queries sem banco real.

Requisitos:
1) Identificar meu provider principal (ex.: SqlServer, Npgsql, MySql etc.).
2) Configurar pacote DbSqlLikeMem.<Provider> no projeto de testes.
3) Criar testes cobrindo:
   - cenário feliz de consulta;
   - filtro com WHERE;
   - ordenação/paginação básica;
   - pelo menos 1 caso de borda (resultado vazio ou parâmetro inválido).
4) Usar padrão Arrange/Act/Assert e nomes de teste claros.
5) No final, listar exatamente quais arquivos foram criados/alterados.

Restrições:
- Não alterar regra de negócio de produção.
- Limitar mudanças ao projeto de testes e setup necessário.
- Se faltar informação (provider/tabelas), propor defaults e seguir com TODOs explícitos.
```

## 4) Mapa rápido: provider -> pacote NuGet

| Provider do projeto XPTO | Pacote para testes |
| --- | --- |
| MySQL | `DbSqlLikeMem.MySql` |
| SQL Server | `DbSqlLikeMem.SqlServer` |
| Oracle | `DbSqlLikeMem.Oracle` |
| PostgreSQL (Npgsql) | `DbSqlLikeMem.Npgsql` |
| SQLite | `DbSqlLikeMem.Sqlite` |
| DB2 | `DbSqlLikeMem.Db2` |

## 5) Fluxo recomendado para a IA (rápido e sem varredura grande)

1. **Ler apenas** `README.md` + `docs/getting-started.md` para contexto base.
2. Identificar provider alvo do usuário.
3. Abrir apenas o README do provider em `src/DbSqlLikeMem.<Provider>/README.md`.
4. Implementar teste no projeto XPTO com o menor setup possível.
5. Executar testes somente do projeto alterado.

## 6) Template de implementação no projeto do usuário

```csharp
[Fact]
public void Repository_Should_Return_Expected_Items_By_Filter()
{
    // Arrange
    var db = new SqlServerDbMock();
    db.CreateSchema("dbo");
    db.CreateTable("dbo", "Users")
      .WithColumn("Id", typeof(int))
      .WithColumn("Name", typeof(string));

    db.Insert("dbo", "Users", new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Ana" });
    db.Insert("dbo", "Users", new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bruno" });

    using var conn = new SqlServerConnectionMock(db);
    var repository = new UserRepository(conn);

    // Act
    var result = repository.FindByName("Ana");

    // Assert
    result.Should().ContainSingle();
    result[0].Name.Should().Be("Ana");
}
```

> Observação: adapte as classes `SqlServer*` para o provider real do projeto XPTO.

## 7) Checklist de qualidade (antes de finalizar)

- [ ] Projeto de teste compila.
- [ ] Pelo menos 3 cenários de query cobertos.
- [ ] Não houve alteração de código de produção sem necessidade.
- [ ] Testes executados e resultado reportado.
- [ ] Lista de arquivos alterados incluída na resposta final.

## 8) Arquivos do DbSqlLikeMem que ajudam a IA a se localizar rápido

- `README.md` (visão geral e links).
- `docs/getting-started.md` (instalação e setup inicial).
- `docs/providers-and-features.md` (diferenças por provider).
- `src/DbSqlLikeMem.<Provider>/README.md` (detalhes do provider escolhido).

Se a ideia for, no próximo passo eu também posso preparar um **“AI_CONTEXT.md” ultracurto** na raiz com no máximo 1 página para reduzir ainda mais o tempo de descoberta em prompts automáticos.
