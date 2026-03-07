# DbSqlLikeMem.VisualStudioExtension

Projeto VSIX para hospedar a interface do DbSqlLikeMem no Visual Studio.

## Evoluções implementadas

1. **Conexões reais + ciclo de vida**
   - Teste de conexão ao adicionar/editar.
   - Ações de editar e remover conexão.
   - Persistência protegida da connection string (DPAPI por usuário).

2. **Carregamento real de objetos**
   - Botão **Atualizar objetos** para listar metadados estruturais via `SqlDatabaseMetadataProvider`.
   - Objetos `Sequence` entram na árvore para bancos com metadata suportada (SqlServer/SqlAzure, PostgreSql, Oracle e Db2).

3. **Menus de contexto na árvore**
   - **Gerar classes de teste**
   - **Gerar classes de modelos**
   - **Gerar classes de repositório**
   - **Checar consistência**

4. **Fluxo de geração com prévia de conflitos**
   - Pré-visualização de arquivos já existentes (sobrescrita) antes de gerar.

5. **Indicadores visuais de consistência**
   - Nó de objeto com marcador de status: 🟢 sincronizado, 🟡 divergente ou trio local incompleto, 🔴 ausente.

6. **Hardening básico**
   - Mensagens de status operacionais na UI.
   - Log local em `%LocalAppData%/DbSqlLikeMem/visual-studio-extension.log`.

7. **Templates configuráveis para modelos e repositórios**
   - Botão no topo **Configurar templates** para informar arquivo de template e diretório de saída.
   - Baselines versionadas do repositório ficam disponíveis em `templates/dbsqllikemem/vCurrent`, com perfis iniciais `api` e `worker` para reaproveitamento manual na configuração.
   - O diálogo da VSIX agora também consegue aplicar diretamente esses perfis quando localiza `templates/dbsqllikemem` a partir do ambiente atual.
   - Templates customizados agora são validados contra o contrato de tokens suportados antes de serem salvos.
   - O mapeamento padrão por tipo de objeto também aceita `namespace` opcional reaproveitado na geração.
   - Substituição de tokens no conteúdo durante a geração, incluindo `{{Namespace}}` quando configurado no mapeamento.
   - O mesmo `namespace` também pode entrar no padrão de nome de arquivo via `{Namespace}`.
   - Geração também pode consumir objetos `Sequence` quando presentes na metadata carregada.

8. **Checagem complementar de artefatos gerados**
   - A consistência considera também a presença de arquivos de Model e Repository, além das classes já geradas pelo fluxo principal.
   - Quando apenas parte do trio local existe, a VSIX agora sinaliza estado intermediário em vez de misturar esse caso com divergência pura de metadados.

9. **Importação e exportação de configurações**
   - Botões no topo para **Importar configurações** e **Exportar configurações** em JSON.
   - Exportação inclui conexões, mapeamentos e templates, com `ConnectionString` protegida (DPAPI por usuário).

## Compatibilidade VSIX

- Compatível com Visual Studio **2022 e linha futura (incluindo 2026)** (`[17.0,19.0)`) nas edições Community/Professional/Enterprise.

## Publicação da VSIX

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag automática: `vsix-v*`
- Fonte da versão publicada: `src/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest`
- Manifesto operacional: `eng/visualstudio/PublishManifest.json`
- Auditoria base: `python scripts/check_release_readiness.py`
- Auditoria estrita no publish: `python scripts/check_release_readiness.py --strict-marketplace-placeholders`

Antes do publish final, confirme o `publisher` do marketplace no manifesto operacional.


## Qualidade e performance

- Operações longas com proteção contra concorrência (uma operação por vez) e cancelamento manual.
- Refresh de objetos com execução paralela por conexão para reduzir tempo total em cenários multi-banco.
- Checagem de consistência com processamento paralelo e propagação de cancelamento.
- Timeout de teste de conexão para evitar bloqueios longos na UI.
- Tratamento centralizado de exceções em eventos da UI (resiliência + log).


## Tokens de template (Model/Repository)

- `{{ClassName}}`
- `{{ObjectName}}`
- `{{Schema}}`
- `{{ObjectType}}`
- `{{DatabaseType}}`
- `{{DatabaseName}}`
- `{{Namespace}}`

Exemplo:

```txt
namespace {{Namespace}};

// {{DatabaseType}} - {{DatabaseName}}
// {{Schema}}.{{ObjectName}}
public class {{ClassName}}
{
}
```

## Troubleshooting de depuração no VS 2022

- Para depurar a VSIX, inicie com o perfil **VS 2022 Experimental** (`/rootsuffix Exp`).
- Para depurar com deploy automático da extensão, prefira o perfil **DbSqlLikeMem.VisualStudioExtension** (`commandName: Project`); os perfis `Executable` apenas abrem o devenv e podem não instalar/atualizar a VSIX.
- Se o comando não aparecer, verifique em **View > Other Windows > DbSqlLikeMem Explorer**.
- Erros de binding como `GlyphButton`, `AIReviewStatusControl`, `SccCompartment`, `TrackingListView` e `CopilotBadgeDataSource` normalmente são de componentes internos do próprio Visual Studio/Copilot/Git e **não** da janela do DbSqlLikeMem.
- Os erros de log `TrackingListView.Background` e `CopilotBadgeDataSource -> WindowTitleBarButton.HelpText` também são conhecidos como ruído de binding do shell do VS e não bloqueiam o carregamento do pacote da extensão por si só.
- Também entram nessa categoria de ruído do shell mensagens como: `ProjectMruListBoxViewModel.Count`, `StatusControl.PullRequestDropdownText`, `AISuggestionStatusControl.ProgressText`, `AIReviewStatusControl.ProgressText`, `SectionControl.MinSqueeze` e `FileListView.MaxHeight=NaN`.
- Em geral esses bindings são de UI interna do VS (MRU/Copilot/review pane) e podem aparecer mesmo sem a DbSqlLikeMem aberta.
- Para confirmar a causa, rode com log (`/log`) e inspecione `%APPDATA%\Microsoft\VisualStudio\17.0_*Exp\ActivityLog.xml` buscando por `DbSqlLikeMem`.
- Se ainda não aparecer, execute `devenv /rootsuffix Exp /setup` e reabra a instância experimental para forçar atualização dos menus VSCT.

## Harness local para validar XAML (fora do VS)

- Foi adicionado o projeto `DbSqlLikeMem.VisualStudioExtension.XamlHarness`, uma aplicação WPF simples para validar se os XAML da extensão estão carregando corretamente sem depender do host do Visual Studio.
- Esse projeto ajuda no cenário em que a VSIX não aparece no menu de debug do Visual Studio, permitindo testar o `DbSqlLikeMemToolWindowControl` e os diálogos (`ConnectionDialog`, `MappingDialog`, `TemplateConfigurationDialog`) de forma isolada.
- Execução:

```bash
dotnet run --project src/DbSqlLikeMem.VisualStudioExtension.XamlHarness/DbSqlLikeMem.VisualStudioExtension.XamlHarness.csproj
```
