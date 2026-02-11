# DbSqlLikeMem VS Code Extension (MVP)

Extensão equivalente ao fluxo desenhado para o Visual Studio Extension Core, adaptada para VS Code.

## O que já faz

- Sidebar própria **DbSqlLikeMem** na Activity Bar.
- Cadastro de conexões (persistidas no `globalState` da extensão).
- TreeView por:
  - Tipo de banco
  - Database
  - Tipo do objeto (`Table`, `View`, `Procedure`)
  - Objeto (`schema.nome`)
- Filtro por modo `Like` e `Equals`.
- Configuração simplificada de mapeamentos.
- Geração de classes `.cs` no workspace.
- Check de consistência (presença de classes locais esperadas).
- Exportação/importação do estado em JSON.

> Atualmente o provedor de metadata é **fake** (retorna objetos fixos) para validar UX e workflow. O próximo passo é substituir pelo provider real por banco.

## Comandos

- `DbSqlLikeMem: Add Connection`
- `DbSqlLikeMem: Configure Mappings`
- `DbSqlLikeMem: Generate Classes`
- `DbSqlLikeMem: Check Consistency`
- `DbSqlLikeMem: Set Filter`
- `DbSqlLikeMem: Export State`
- `DbSqlLikeMem: Import State`
- `DbSqlLikeMem: Refresh`

## Rodar localmente

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm install
npm run compile
```

Depois:

1. Abra essa pasta no VS Code.
2. Pressione `F5` para abrir o Extension Development Host.
3. Na nova janela, abra a Command Palette e execute os comandos da extensão.

## Próximos incrementos sugeridos

1. Trocar `FakeMetadataProvider` por metadata real via drivers por banco.
2. Persistir secret em `SecretStorage` em vez de `globalState`.
3. Adicionar ícones por tipo de objeto e status de consistência.
4. Oferecer Webview para editar mapeamentos de forma avançada.
5. Publicar no marketplace com `vsce package`.
