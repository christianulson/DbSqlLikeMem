# GitHub Wiki for DbSqlLikeMem

This directory documents the GitHub Wiki workflow now that the mirrored pages live in `docs/Wiki` and follow a multilingual page-pair convention.

## Documentation strategy

1. Keep canonical deep documentation in the repository under `docs/`.
2. Keep the GitHub Wiki focused on end-user developer guidance.
3. Use English as the canonical wiki language.
4. Add one mirrored page per additional language using a locale suffix.

## Current language convention

- English page: `Home.md`
- Portuguese (Brazil) mirror: `Home.pt-BR.md`
- Every wiki page should expose reciprocal language links at the top.
- The shared `_Sidebar.md` should list both language trees.

Future languages should follow the same naming style, for example `Getting-Started.es.md` or `Getting-Started.fr-FR.md`.

## Current wiki structure

### Core navigation

- `Home`
- `Getting-Started`
- `Provider-Selection`
- `Providers-and-Compatibility`
- `ADO.NET-and-Dapper`
- `DbConnection-Interception`
- `Diagnostics-and-Execution-Plans`
- `Limitations-and-Known-Gaps`

### Ecosystem navigation

- `Extensions-and-Ecosystem`
- `ORM-and-Test-Integrations`
- `MiniProfiler-Integration`
- `Visual-Studio-Extension`
- `VS-Code-Extension`
- `Maintainer-Resources`

### Provider pages

- `Provider-MySQL`
- `Provider-SQL-Server`
- `Provider-SQL-Azure`
- `Provider-PostgreSQL-Npgsql`
- `Provider-Oracle`
- `Provider-SQLite`
- `Provider-DB2`

Each page above currently has an English file and a `pt-BR` mirror.

## Publishing to GitHub Wiki

### Option A: Copy and paste in GitHub

1. Open the repository **Wiki** tab.
2. Create or update the English pages from `docs/Wiki/*.md`.
3. Create or update the Portuguese mirrors from `docs/Wiki/*.pt-BR.md`.
4. Copy `_Sidebar.md` so both language trees appear in the GitHub Wiki sidebar.

### Option B: Sync through the separate wiki repository

```bash
# Wiki URL usually ends with .wiki.git
git clone https://github.com/<org>/<repo>.wiki.git
cd <repo>.wiki

# copy the mirrored wiki files to the wiki repo root
cp -r ../DbSqlLikeMem/docs/Wiki/* .

git add .
git commit -m "docs(wiki): refresh multilingual wiki"
git push
```

## Editorial rules for wiki pages

- Do not mix English and Portuguese in the same wiki page.
- Keep user-facing pages focused on installation, provider choice, usage, diagnostics, ecosystem, and known limitations.
- Keep release and publishing details out of the main navigation and point maintainers to `Maintainer-Resources`.
- Use repository docs and the feature backlog as source material, but rewrite them for end users instead of copying backlog wording.

## Related repository docs

- [Docs index](../README.md)
- [Getting started](../getting-started.md)
- [Publishing](../publishing.md)
- [Feature backlog](../features-backlog/index.md)

---

# Wiki do GitHub para o DbSqlLikeMem

Este diretório documenta o fluxo da GitHub Wiki agora que as páginas espelhadas vivem em `docs/Wiki` e seguem uma convenção multilíngue por par de páginas.

## Estratégia de documentação

1. Manter a documentação canônica e mais profunda dentro de `docs/`.
2. Manter a GitHub Wiki focada na orientação ao desenvolvedor usuário final.
3. Usar inglês como idioma canônico da wiki.
4. Adicionar um arquivo espelho por idioma com sufixo de locale.

## Convenção atual de idiomas

- Página em inglês: `Home.md`
- Espelho em português do Brasil: `Home.pt-BR.md`
- Toda página da wiki deve expor links recíprocos de idioma no topo.
- O `_Sidebar.md` compartilhado deve listar as duas árvores de navegação.

Idiomas futuros devem seguir o mesmo padrão de nome, por exemplo `Getting-Started.es.md` ou `Getting-Started.fr-FR.md`.

## Publicação na GitHub Wiki

### Opção A: copiar e colar no GitHub

1. Abra a aba **Wiki** do repositório.
2. Crie ou atualize as páginas em inglês a partir de `docs/Wiki/*.md`.
3. Crie ou atualize os espelhos em português a partir de `docs/Wiki/*.pt-BR.md`.
4. Copie `_Sidebar.md` para que as duas árvores de idioma apareçam na navegação lateral.

### Opção B: sincronizar pelo repositório separado da wiki

```bash
# A URL da wiki normalmente termina com .wiki.git
git clone https://github.com/<org>/<repo>.wiki.git
cd <repo>.wiki

# copie os arquivos espelhados para a raiz do repo da wiki
cp -r ../DbSqlLikeMem/docs/Wiki/* .

git add .
git commit -m "docs(wiki): atualizar wiki multilíngue"
git push
```

## Regras editoriais para as páginas da wiki

- Não misturar inglês e português na mesma página.
- Manter as páginas de usuário final focadas em instalação, escolha de provider, uso, diagnóstico, ecossistema e limitações conhecidas.
- Deixar detalhes de release e publicação fora da navegação principal e apontar mantenedores para `Maintainer-Resources`.
- Usar a documentação do repositório e o backlog funcional como fonte, mas reescrever para o público final em vez de copiar a linguagem do backlog.