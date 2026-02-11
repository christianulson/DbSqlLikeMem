# GitHub Wiki for DbSqlLikeMem

This directory contains a ready-to-publish structure for the repository Wiki.

## Recommended strategy

1. Keep canonical documentation in the repository (`docs/`).
2. Mirror key pages to the GitHub Wiki for quick navigation.
3. Add bidirectional links between README and Wiki pages.

## Suggested page structure

The files under `docs/wiki/pages` are prepared to be used directly as Wiki pages:

- `Home.md`
- `Getting-Started.md`
- `Providers-and-Compatibility.md`
- `Publishing.md`

## How to publish to Wiki (manual)

### Option A: Copy and paste in GitHub

1. In the GitHub repository, open the **Wiki** tab.
2. Create the `Home` page and paste `docs/wiki/pages/Home.md`.
3. Create the remaining pages with the same names (without `.md`).

### Option B: Clone the Wiki repository and version it locally

```bash
# Wiki URL usually ends with .wiki.git
git clone https://github.com/<org>/<repo>.wiki.git
cd <repo>.wiki

# copy files from this folder to the wiki repo root
cp -r ../DbSqlLikeMem/docs/wiki/pages/* .

git add .
git commit -m "docs(wiki): initial wiki structure"
git push
```

## Notes

- GitHub Wiki uses a separate Git repository (`<repo>.wiki.git`).
- The sidebar menu can be controlled via `_Sidebar.md` (optional).
- You can automate sync from `docs/wiki/pages` to the wiki repo using GitHub Actions.

## Related links

- [Local documentation](../README.md)
- [Getting started](../getting-started.md)
- [Providers and compatibility](../providers-and-features.md)
- [Publishing](../publishing.md)

---

# Wiki do GitHub para o DbSqlLikeMem (Português)

Este diretório prepara uma estrutura pronta para ser publicada como Wiki do repositório.

## Estratégia recomendada

1. Manter a documentação canônica no repositório (`docs/`).
2. Espelhar as páginas principais na Wiki do GitHub para navegação rápida.
3. Incluir links bidirecionais entre README e Wiki.

## Como publicar na Wiki (manual)

### Opção A: copiar e colar no GitHub

1. No repositório do GitHub, abra a aba **Wiki**.
2. Crie a página `Home` e cole `docs/wiki/pages/Home.md`.
3. Crie as demais páginas com os mesmos nomes (sem `.md`).

### Opção B: clonar o repositório da wiki e versionar localmente

```bash
# URL da wiki normalmente termina com .wiki.git
git clone https://github.com/<org>/<repo>.wiki.git
cd <repo>.wiki

# copie os arquivos desta pasta para a raiz do repo da wiki
cp -r ../DbSqlLikeMem/docs/wiki/pages/* .

git add .
git commit -m "docs(wiki): estrutura inicial da wiki"
git push
```
