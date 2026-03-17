# Security Policy

## Supported Versions | Versões suportadas

**EN:** Security fixes are generally applied only to the **latest released version** of DbSqlLikeMem.  
Older versions may not receive security updates.

**PT-BR:** Correções de segurança geralmente são aplicadas apenas à **versão mais recente** do DbSqlLikeMem.  
Versões antigas podem não receber atualizações de segurança.

| Version | Supported |
| ------- | ----------|
| Latest release | :white_check_mark: |
| Previous minor versions | :warning: Limited |
| Older versions | :x: |

**EN:** If you are using an older version, upgrading to the latest release is strongly recommended.  
**PT-BR:** Se você estiver usando uma versão antiga, é altamente recomendado atualizar para a versão mais recente.

---

# Reporting a Vulnerability | Reportando uma vulnerabilidade

**EN:** If you discover a security vulnerability in DbSqlLikeMem, please report it responsibly.

**PT-BR:** Se você descobrir uma vulnerabilidade de segurança no DbSqlLikeMem, por favor reporte de forma responsável.

## How to report | Como reportar

**EN:** Please open a **private security report** through one of the following channels:

- GitHub Security Advisory (preferred)
- GitHub Issue marked as **security**
- Direct contact if necessary

If possible, include:

- Description of the vulnerability
- Steps to reproduce
- Minimal reproducible example
- Potential impact
- Suggested fix (optional)

**PT-BR:** Abra um **relato de segurança privado** por um dos canais abaixo:

- GitHub Security Advisory (preferencial)
- GitHub Issue marcada como **security**
- Contato direto se necessário

Se possível inclua:

- Descrição da vulnerabilidade
- Passos para reprodução
- Exemplo mínimo reproduzível
- Impacto potencial
- Sugestão de correção (opcional)

---

# Response process | Processo de resposta

**EN:** After a vulnerability report is received:

1. The report will be reviewed.
2. The issue will be reproduced and validated.
3. A fix will be developed if the vulnerability is confirmed.
4. A patched release will be published when appropriate.

Typical timeline:

- Initial response: **within a few days**
- Investigation and validation: **depends on complexity**
- Patch release: **as soon as a safe fix is available**

**PT-BR:** Após receber um relatório de vulnerabilidade:

1. O relatório será analisado.
2. O problema será reproduzido e validado.
3. Uma correção será desenvolvida caso a vulnerabilidade seja confirmada.
4. Uma nova release corrigida será publicada quando apropriado.

Prazo típico:

- Primeira resposta: **em poucos dias**
- Investigação e validação: **depende da complexidade**
- Release com correção: **assim que uma solução segura estiver disponível**

---

# Scope | Escopo

**EN:** DbSqlLikeMem is an **in-memory testing tool** that emulates SQL and ADO.NET behavior.  
It is **not intended for production database use**.

Security reports are most relevant when they involve:

- Unexpected code execution
- Dependency vulnerabilities
- Unsafe parsing or evaluation behavior
- Data exposure in test environments

**PT-BR:** DbSqlLikeMem é uma **ferramenta de testes em memória** que emula comportamento SQL e ADO.NET.  
Ela **não é destinada ao uso como banco de dados em produção**.

Relatórios de segurança são mais relevantes quando envolvem:

- Execução inesperada de código
- Vulnerabilidades em dependências
- Comportamento inseguro de parsing ou execução
- Exposição de dados em ambientes de teste

---

# Responsible disclosure | Divulgação responsável

**EN:** Please avoid publicly disclosing security vulnerabilities before a fix has been released.

**PT-BR:** Por favor evite divulgar publicamente vulnerabilidades de segurança antes que uma correção tenha sido publicada.

We appreciate responsible disclosure and collaboration from the community.
