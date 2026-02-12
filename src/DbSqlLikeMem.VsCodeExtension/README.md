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
- Interface gráfica (Manager) para cadastrar/remover conexões e configurar mapeamentos.
- Configuração de mapeamentos também no menu de contexto do nó do database.
- Geração de classes de **teste** `.cs` no workspace (ação principal).
- Geração de classes de **modelo** a partir de template com tokens.
- Geração de classes de **repositório** a partir de template com tokens.
- Configuração de templates (botão no topo da view) para modelos e repositórios.
- Check de consistência para artefatos gerados (teste/model/repositório), com status visual por objeto na árvore.
- Exportação/importação do estado em JSON.

> Atualmente o provedor de metadata é **fake** (retorna objetos fixos) para validar UX e workflow. O próximo passo é substituir pelo provider real por banco.

## Comandos

- `DbSqlLikeMem: Open Manager` (UI gráfica de conexões + mapeamentos)
- `DbSqlLikeMem: Add Connection`
- `DbSqlLikeMem: Configure Mappings` (também disponível no menu de contexto do database)
- `DbSqlLikeMem: Generate Test Classes`
- `DbSqlLikeMem: Generate Model Classes`
- `DbSqlLikeMem: Generate Repository Classes`
- `DbSqlLikeMem: Configure Templates`
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

## Preparação para publicação no Marketplace (VS Code)

O projeto já foi ajustado para empacotar/publicar via `@vscode/vsce`.

### 1) Pré-requisitos

1. Definir dados reais no `package.json`:
   - `publisher`
   - `repository.url`, `bugs.url`, `homepage`
2. Criar o publisher no Visual Studio Marketplace.
3. Gerar um PAT com permissão de publicação no Marketplace.

### 2) Empacotar localmente (`.vsix`)

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm install
npm run compile
npm run package
```

O arquivo `.vsix` será gerado na pasta da extensão.

### 3) Publicar manualmente (opcional)

```bash
cd src/DbSqlLikeMem.VsCodeExtension
npm run publish
```

> O `vsce` solicitará autenticação/token conforme configuração local.

### 4) Publicar via GitHub Actions

Workflow disponível: `.github/workflows/vscode-extension-publish.yml`.

Configuração necessária no repositório:

- Secret `VSCE_PAT`: token do Marketplace para o publisher.

Como acionar:

- Manualmente via `workflow_dispatch` (com opção de apenas build ou build+publish).
- Automaticamente via tag `vscode-v*` (ex.: `vscode-v0.1.0`).


### Dica para ambientes que bloqueiam binários em PR

Este projeto não versiona mais o `icon.png` diretamente. Em vez disso, o arquivo é gerado automaticamente a partir de `resources/icon.png.base64` durante os comandos de `package` e `publish` (`npm run generate:icon`).

Assim você mantém ícone no VSIX sem incluir diff binário no PR.

## Próximos incrementos sugeridos

1. Trocar `FakeMetadataProvider` por metadata real via drivers por banco.
2. Persistir secret em `SecretStorage` em vez de `globalState`.
3. Adicionar ícones por tipo de objeto e status de consistência.
4. Oferecer Webview para editar mapeamentos de forma avançada.


## Tokens suportados nos templates

Os templates de Model e Repository aceitam os seguintes tokens para substituição durante a geração:

- `{{ClassName}}`
- `{{ObjectName}}`
- `{{Schema}}`
- `{{ObjectType}}`
- `{{DatabaseType}}`
- `{{DatabaseName}}`

### Exemplo rápido de template

```txt
// Generated from {{DatabaseType}} / {{DatabaseName}}
// Object: {{Schema}}.{{ObjectName}} ({{ObjectType}})
public class {{ClassName}}
{
}
```

## Fluxo recomendado

1. Configure conexões e mapeamentos.
2. Use **Configure Templates** para informar os arquivos `.txt` e pastas de saída de Model/Repository.
3. Use o menu de contexto do database para gerar:
   - classes de teste (ação existente),
   - classes de modelo,
   - classes de repositório.
4. Rode **Check Consistency** para validar presença dos artefatos e visualizar ícones de status na árvore.
