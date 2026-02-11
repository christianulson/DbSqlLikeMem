# Wiki do GitHub para o DbSqlLikeMem

Este diretório prepara uma estrutura pronta para ser publicada como Wiki do repositório.

## Estratégia recomendada

1. Manter a documentação canônica no repositório (`docs/`).
2. Espelhar as páginas principais na Wiki do GitHub para navegação rápida.
3. Incluir links bidirecionais entre README e Wiki.

## Estrutura sugerida de páginas

Os arquivos em `docs/wiki/pages` já foram preparados para uso como páginas da wiki:

- `Home.md`
- `Getting-Started.md`
- `Providers-and-Compatibility.md`
- `Publishing.md`

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

## Observações

- O GitHub Wiki usa um repositório Git separado (`<repo>.wiki.git`).
- O menu lateral pode ser controlado via página `_Sidebar.md` (opcional).
- Se quiser, você pode automatizar sync da pasta `docs/wiki/pages` com o repo da wiki via GitHub Actions.

## Links relacionados

- [Documentação local](../README.md)
- [Começando rápido](../getting-started.md)
- [Provedores e compatibilidade](../providers-and-features.md)
- [Publicação](../publishing.md)
