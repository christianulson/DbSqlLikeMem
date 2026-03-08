# DbSqlLikeMem VS Code Extension (MVP)

Extensão equivalente ao fluxo desenhado para o Visual Studio Extension Core, adaptada para VS Code.

## O que já faz

- Sidebar própria **DbSqlLikeMem** na Activity Bar.
- Cadastro de conexões (persistidas no `globalState` da extensão).
- Validação de conexão ao adicionar/editar (com tentativa real para `SqlServer` via `sqlcmd`).
- TreeView por:
  - Tipo de banco
  - Database
  - Tipo do objeto (`Table`, `View`, `Procedure`, `Sequence`)
  - Objeto (`schema.nome`)
  - Colunas e Foreign Keys (para tabelas SQL Server)
- Filtro por modo `Like` e `Equals`.
- Interface gráfica (Manager) para cadastrar/editar/remover conexões e configurar mapeamentos.
- Configuração de mapeamentos por tipo de objeto (`Table`, `View`, `Procedure`, `Sequence`) também no menu de contexto do nó do database.
- Geração de classes de **teste** `.cs` no workspace (ação principal), com prévia de conflitos (sobrescrita) e scaffold inicial explícito (`[Fact(Skip = ...)]`, metadados de origem e blocos Arrange/Act/Assert).
- Geração de classes de **modelo** a partir de template com tokens, com prévia de conflitos (sobrescrita), incluindo objetos `Sequence` quando a metadata do provider os expõe e cabeçalho padronizado `// DBSqlLikeMem:*` com snapshot estrutural mínimo (`Columns`/`ForeignKeys` e metadata de sequência) para rastreabilidade da origem.
- Geração de classes de **repositório** a partir de template com tokens, com prévia de conflitos (sobrescrita), incluindo objetos `Sequence` quando a metadata do provider os expõe e cabeçalho padronizado `// DBSqlLikeMem:*` com snapshot estrutural mínimo (`Columns`/`ForeignKeys` e metadata de sequência) para rastreabilidade da origem.
- Configuração de templates (botão no topo da view) para modelos e repositórios.
- Configuração de templates com baseline versionada do repositório (`templates/dbsqllikemem/vCurrent`) e perfis iniciais `API`/`Worker-Batch`.
- O fluxo rápido **Configure Mappings** agora também oferece defaults recomendados por perfil (`API`/`Worker-Batch`) para pastas e sufixos das classes de teste.
- Os quick picks de baseline no VS Code agora também consomem `templates/dbsqllikemem/review-metadata.json` quando ele existe no workspace, exibindo cadência, última revisão, próxima janela e eventual drift de governança em relação ao catálogo embutido.
- Quando `nextPlannedReviewOn` vence, o mesmo resumo da baseline agora acusa explicitamente revisão em atraso antes de aplicar templates ou mappings do perfil.
- O mesmo resumo agora também explicita os diretórios recomendados de saída para `Model` e `Repository`, reduzindo consulta manual ao catálogo durante a configuração.
- O fluxo de mappings e o manager visual agora também cobrem `Sequence`, mantendo paridade operacional com a VSIX quando o provider expõe esse tipo.
- Templates customizados agora também são validados contra o contrato de tokens suportados antes de entrar no fluxo de geração.
- Model e Repository agora também aceitam padrão configurável de nome de arquivo, reutilizando placeholders como `{NamePascal}`, `{Schema}`, `{DatabaseType}`, `{DatabaseName}` e `{Namespace}`.
- Check de consistência para artefatos gerados (teste/model/repositório), com status visual por objeto na árvore, tooltip com os artefatos faltantes e validação do trio completo por objeto, incluindo detecção de drift quando o arquivo existente aponta para outro objeto/fonte ou carrega snapshot estrutural defasado em relação ao objeto atual, inclusive para `Sequence`.
- Ações de geração/consistência respeitam o nó selecionado da TreeView (`Database`, `ObjectType` ou objeto individual).
- Menus de contexto de geração/consistência disponíveis em todos os níveis relevantes da árvore (tipo de banco, database, tipo de objeto, objeto e detalhes como colunas/FKs).
- Exportação/importação do estado em JSON.

> Atualmente a extensão já usa metadata real para `SqlServer` (incluindo `sys.sequences`) e mantém fallback simplificado para os demais bancos enquanto a malha de providers reais é expandida.

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
npm test
```

Depois:

1. Abra essa pasta no VS Code.
2. Pressione `F5` para abrir o Extension Development Host.
3. Na nova janela, abra a Command Palette e execute os comandos da extensão.

## Preparação para publicação no Marketplace (VS Code)

O projeto já foi ajustado para empacotar/publicar via `@vscode/vsce`.

### 1) Pré-requisitos

1. Confirmar os dados finais no `package.json`:
   - `publisher`
   - URLs de repositório/bugs/homepage, se houver mudança de origem do projeto
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

Fonte da versão publicada: `src/DbSqlLikeMem.VsCodeExtension/package.json`.

Configuração necessária no repositório:

- Secret `VSCE_PAT`: token do Marketplace para o publisher.

Como acionar:

- Manualmente via `workflow_dispatch` (com opção de apenas build ou build+publish).
- Automaticamente via tag `vscode-v*` (ex.: `vscode-v0.1.0`).
- Antes de criar a tag, revise `../../CHANGELOG.md` e `../../docs/publishing.md` para manter release notes e limitações abertas alinhadas ao publish.


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
- `{{Namespace}}` (quando definido no mapeamento do tipo de objeto)

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
   - O fluxo rápido **Configure Mappings** também aceita `namespace` opcional reaproveitado na geração das classes.
   - O mesmo comando agora pode partir dos defaults recomendados de teste para `API` (integração leve) ou `Worker-Batch` (consistência), antes de qualquer ajuste manual.
   - Quando `review-metadata.json` está presente no workspace, a escolha do perfil também mostra a janela de revisão e acusa drift de governança da baseline no próprio quick pick.
   - `Sequence` participa do mesmo fluxo de configuração, geração e consistência quando a metadata do banco o expõe.
2. Use **Configure Templates** para informar os arquivos `.txt` e pastas de saída de Model/Repository.
   - O comando agora oferece baseline pronta do repositório em `templates/dbsqllikemem/vCurrent/api` e `templates/dbsqllikemem/vCurrent/worker`, além da opção de manter valores customizados.
   - O mesmo quick pick agora reaproveita `review-metadata.json` para mostrar cadência, última revisão, próxima janela e drift entre metadata versionado e catálogo da extensão.
   - Se um template existente usar placeholders fora do contrato suportado, a extensão bloqueia a configuração ou faz fallback para o template padrão na geração.
   - O mesmo fluxo agora também permite configurar o padrão de nome de arquivo de `Model` e `Repository`.
3. Use o menu de contexto do database para gerar:
   - classes de teste (ação existente),
   - classes de modelo,
   - classes de repositório.
4. Rode **Check Consistency** para validar presença dos artefatos e visualizar ícones de status na árvore.
   - O check agora também acusa `drift` quando um arquivo local existente carrega snapshot `// DBSqlLikeMem:*` de outro objeto, mesmo que o trio esteja fisicamente presente.
