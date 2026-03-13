# Contributing to DbSqlLikeMem

Thank you for contributing to **DbSqlLikeMem**.

**EN:** This document explains the preferred workflow for proposing changes, writing code, updating documentation, and submitting pull requests.  
**PT-BR:** Este documento explica o fluxo preferido para propor mudanças, escrever código, atualizar documentação e enviar pull requests.

---

## 1. Scope of contributions | Escopo das contribuições

**EN:** Contributions are welcome in the following areas:  
**PT-BR:** Contribuições são bem-vindas nas seguintes áreas:

- **EN:** SQL compatibility improvements by provider and version.  
  **PT-BR:** Melhorias de compatibilidade SQL por provedor e versão.
- **EN:** Parser and executor behavior.  
  **PT-BR:** Comportamento do parser e do executor.
- **EN:** ADO.NET mock fidelity.  
  **PT-BR:** Fidelidade dos mocks de ADO.NET.
- **EN:** Test coverage and regression protection.  
  **PT-BR:** Cobertura de testes e proteção contra regressões.
- **EN:** Documentation, examples, and developer tooling.  
  **PT-BR:** Documentação, exemplos e ferramental para desenvolvedores.
- **EN:** Diagnostics, performance, and maintainability improvements.  
  **PT-BR:** Melhorias de diagnóstico, performance e manutenibilidade.

---

## 2. Before you start | Antes de começar

**EN:** Before implementing a larger change, prefer opening an issue or starting a discussion so the approach can be aligned early.  
**PT-BR:** Antes de implementar uma mudança maior, prefira abrir uma issue ou iniciar uma discussão para alinhar a abordagem desde o início.

**EN:** Small fixes such as typo corrections, documentation adjustments, and isolated tests can usually be submitted directly as a pull request.  
**PT-BR:** Correções pequenas, como ajustes de texto, documentação e testes isolados, normalmente podem ser enviadas diretamente como pull request.

---

## 3. Development principles | Princípios de desenvolvimento

**EN:** Please keep contributions aligned with the project goals:  
**PT-BR:** Mantenha as contribuições alinhadas com os objetivos do projeto:

- **EN:** Focus on deterministic test behavior.  
  **PT-BR:** Foque em comportamento determinístico para testes.
- **EN:** Respect dialect-specific behavior whenever possible.  
  **PT-BR:** Respeite o comportamento específico de cada dialeto sempre que possível.
- **EN:** Prefer incremental, test-driven changes over large unvalidated refactors.  
  **PT-BR:** Prefira mudanças incrementais e orientadas por testes em vez de grandes refatorações sem validação.
- **EN:** Fail clearly when a construct is unsupported instead of silently diverging.  
  **PT-BR:** Gere falhas claras quando uma construção não for suportada, em vez de divergir silenciosamente.
- **EN:** Keep public-facing documentation bilingual when applicable.  
  **PT-BR:** Mantenha a documentação pública bilíngue quando aplicável.

---

## 4. Repository setup | Setup do repositório

```bash
git clone https://github.com/christianulson/DbSqlLikeMem.git
cd DbSqlLikeMem
```

**EN:** Use the .NET SDKs/runtimes required by the projects you are changing. The repository currently documents production targets around `net462`, `netstandard2.0`, and `net8.0`, while test/test-tools projects may also use `net6.0`.  
**PT-BR:** Use os SDKs/runtimes .NET exigidos pelos projetos que você estiver alterando. O repositório atualmente documenta alvos de produção em torno de `net462`, `netstandard2.0` e `net8.0`, enquanto projetos de teste/test-tools também podem usar `net6.0`.

**EN:** When in doubt, validate your changes with the solution-level test command first.  
**PT-BR:** Em caso de dúvida, valide suas mudanças primeiro com o comando de teste da solução.

---

## 5. Branching and commits | Branches e commits

**EN:** Create a dedicated branch for each contribution.  
**PT-BR:** Crie uma branch dedicada para cada contribuição.

Suggested examples:

- `feature/mysql-json-functions`
- `fix/sqlserver-identity-seed`
- `docs/getting-started-clarification`
- `test/npgsql-upsert-coverage`

**EN:** Prefer small, focused commits with messages that explain the intent of the change.  
**PT-BR:** Prefira commits pequenos e focados, com mensagens que expliquem a intenção da mudança.

Recommended style:

```text
feat(mysql): add initial support for ...
fix(sqlserver): correct affected rows for ...
docs(readme): clarify provider setup
test(npgsql): add regression coverage for ...
```

---

## 6. Coding expectations | Expectativas de código

**EN:** When contributing code:  
**PT-BR:** Ao contribuir com código:

- **EN:** Keep naming and structure consistent with the existing provider/core organization.  
  **PT-BR:** Mantenha nomes e estrutura consistentes com a organização atual de provider/core.
- **EN:** Isolate provider-specific behavior instead of introducing cross-dialect coupling unnecessarily.  
  **PT-BR:** Isole comportamentos específicos de provider em vez de introduzir acoplamento entre dialetos sem necessidade.
- **EN:** Avoid broad refactors unrelated to the problem being solved.  
  **PT-BR:** Evite refatorações amplas não relacionadas ao problema em questão.
- **EN:** Add or update tests together with behavior changes.  
  **PT-BR:** Adicione ou atualize testes junto com mudanças de comportamento.
- **EN:** Preserve backward compatibility when possible, especially in public APIs and common flows.  
  **PT-BR:** Preserve compatibilidade retroativa sempre que possível, especialmente em APIs públicas e fluxos comuns.

---

## 7. Documentation standard | Padrão de documentação

**EN:** Public-facing documentation should follow the repository convention of **English first** and **Portuguese second**.  
**PT-BR:** A documentação voltada ao público deve seguir a convenção do repositório de **inglês primeiro** e **português em seguida**.

Recommended XML documentation pattern:

```csharp
/// <summary>
/// English description.
/// Descrição em português.
/// </summary>
```

**EN:** When overriding or implementing members that already have adequate documentation, prefer:  
**PT-BR:** Ao sobrescrever ou implementar membros que já possuem documentação adequada, prefira:

```csharp
/// <inheritdoc/>
```

**EN:** Keep examples executable whenever practical.  
**PT-BR:** Mantenha exemplos executáveis sempre que for prático.

---

## 8. Testing requirements | Requisitos de teste

Run the full test suite when your change has broad impact:

```bash
dotnet test src/DbSqlLikeMem.slnx
```

Run a single test project when iterating on a targeted area:

```bash
dotnet test src/DbSqlLikeMem.SqlServer.Test/DbSqlLikeMem.SqlServer.Test.csproj
dotnet test src/DbSqlLikeMem.SqlAzure.Test/DbSqlLikeMem.SqlAzure.Test.csproj
```

**EN:** Every behavior change should include validation through automated tests, manual verification notes, or both.  
**PT-BR:** Toda mudança de comportamento deve incluir validação por testes automatizados, anotações de verificação manual, ou ambos.

**EN:** For dialect-sensitive features, prefer adding regression tests that make the expected behavior explicit.  
**PT-BR:** Para recursos sensíveis a dialeto, prefira adicionar testes de regressão que deixem explícito o comportamento esperado.

**EN:** If you update scripts, snapshots, or compatibility matrices, regenerate the related artifacts when required.  
**PT-BR:** Se você atualizar scripts, snapshots ou matrizes de compatibilidade, regenere os artefatos relacionados quando necessário.

---

## 9. Pull request checklist | Checklist de pull request

Before opening a PR, confirm the following:

- [ ] **EN:** The change is scoped and focused.  
      **PT-BR:** A mudança está bem delimitada e focada.
- [ ] **EN:** Code follows the existing repository structure and naming patterns.  
      **PT-BR:** O código segue a estrutura e os padrões de nomenclatura do repositório.
- [ ] **EN:** Relevant tests were added or updated.  
      **PT-BR:** Testes relevantes foram adicionados ou atualizados.
- [ ] **EN:** Documentation was updated when behavior or usage changed.  
      **PT-BR:** A documentação foi atualizada quando houve mudança de comportamento ou uso.
- [ ] **EN:** Public-facing docs/comments follow the bilingual convention where applicable.  
      **PT-BR:** Documentos/comentários públicos seguem a convenção bilíngue quando aplicável.
- [ ] **EN:** The branch is rebased or merged cleanly against the target branch.  
      **PT-BR:** A branch está atualizada e sem conflitos com a branch de destino.

**EN:** In the PR description, include:  
**PT-BR:** Na descrição do PR, inclua:

1. **EN:** What changed.  
   **PT-BR:** O que mudou.
2. **EN:** Why the change is needed.  
   **PT-BR:** Por que a mudança é necessária.
3. **EN:** Which providers/dialects are affected.  
   **PT-BR:** Quais providers/dialetos são afetados.
4. **EN:** How the change was validated.  
   **PT-BR:** Como a mudança foi validada.
5. **EN:** Any known limitations or follow-up work.  
   **PT-BR:** Limitações conhecidas ou trabalhos futuros.

---

## 10. Areas where help is especially valuable | Áreas em que ajuda é especialmente valiosa

**EN:** High-impact contribution areas include:  
**PT-BR:** Áreas de contribuição de alto impacto incluem:

- **EN:** Expanding SQL compatibility by dialect/version.  
  **PT-BR:** Expandir compatibilidade SQL por dialeto/versão.
- **EN:** Improving parser/executor coverage.  
  **PT-BR:** Melhorar cobertura do parser/executor.
- **EN:** Adding realistic provider-specific scenarios.  
  **PT-BR:** Adicionar cenários realistas específicos por provider.
- **EN:** Strengthening transaction, concurrency, and regression tests.  
  **PT-BR:** Fortalecer testes de transação, concorrência e regressão.
- **EN:** Improving docs, onboarding, and examples.  
  **PT-BR:** Melhorar documentação, onboarding e exemplos.

---

## 11. Reporting issues | Reportando issues

**EN:** When opening an issue, include enough detail so the problem can be reproduced and discussed efficiently.  
**PT-BR:** Ao abrir uma issue, inclua detalhes suficientes para que o problema possa ser reproduzido e discutido com eficiência.

Recommended information:

- **EN:** Provider/dialect and simulated version  
  **PT-BR:** Provider/dialeto e versão simulada
- **EN:** Minimal reproducible example  
  **PT-BR:** Exemplo mínimo reproduzível
- **EN:** Expected behavior  
  **PT-BR:** Comportamento esperado
- **EN:** Actual behavior  
  **PT-BR:** Comportamento atual
- **EN:** Stack trace or failing assertion, if available  
  **PT-BR:** Stack trace ou asserção com falha, se houver

---

## 12. Final notes | Observações finais

**EN:** By contributing, you agree that your contributions will be licensed under the same license used by this repository. See [LICENSE](LICENSE).  
**PT-BR:** Ao contribuir, você concorda que suas contribuições serão licenciadas sob a mesma licença usada neste repositório. Veja [LICENSE](LICENSE).

**EN:** Thank you for helping improve DbSqlLikeMem.  
**PT-BR:** Obrigado por ajudar a melhorar o DbSqlLikeMem.
