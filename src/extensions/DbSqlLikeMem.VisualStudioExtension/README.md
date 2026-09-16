# DbSqlLikeMem.VisualStudioExtension

Projeto VSIX para hospedar a interface do DbSqlLikeMem no Visual Studio.

## Como abrir a tela

Depois de compilar e iniciar a instância de destino, abra **Exibir (View) > Outras Janelas (Other Windows) > DbSqlLikeMem Explorer**. O mesmo comando também fica em **Ferramentas (Tools) > DbSqlLikeMem Explorer**.

Outra forma é abrir **Exibir > Outras Janelas > Janela de Comandos** e executar:

```text
DbSqlLikeMem.OpenExplorer
```

O nome do comando permanece igual em todas as linguagens do Visual Studio. A VSIX instalada na instância experimental aparece na janela identificada como **Experimental Instance**.

## Evoluções implementadas

1. **Conexões reais + ciclo de vida**
   - Teste de conexão ao adicionar/editar.
   - Ações de editar e remover conexão.
   - Persistência protegida da connection string (DPAPI por usuário).

2. **Carregamento real de objetos**
   - Botão **Atualizar objetos** para listar metadados estruturais via `SqlDatabaseMetadataProvider`.
   - Objetos `Function` e `Sequence` entram na árvore para bancos com metadata suportada (MySql, MariaDb, Firebird, SqlServer/SqlAzure, PostgreSql, Oracle, Sqlite e Db2).

3. **Menus de contexto na árvore**
   - **Gerar classes de teste**
   - **Gerar classes de modelos**
   - **Gerar classes de repositório**
   - **Checar consistência**

4. **Fluxo de geração com prévia de conflitos**
   - Pré-visualização de arquivos já existentes (sobrescrita) antes de gerar.

5. **Indicadores visuais de consistência**
   - Nó de objeto com marcador de status: 🟢 sincronizado, 🟡 divergente ou trio local incompleto, 🔴 ausente.

6. **Filtro global de objetos**
   - Campo na toolbar para filtrar objetos por nome em toda a árvore, com modo Contém/Exato e botão de limpar (aplicado com debounce).

6. **Hardening básico**
   - Mensagens de status operacionais na UI.
   - Log local em `%LocalAppData%/DbSqlLikeMem/visual-studio-extension.log`.

7. **Templates configuráveis para modelos e repositórios**
   - Botão no topo **Configurar templates** para informar arquivo de template e diretório de saída.
   - Baselines versionadas do repositório ficam disponíveis em `templates/dbsqllikemem/vCurrent`, com perfis iniciais `api` e `worker` para reaproveitamento manual na configuração.
   - O diálogo da VSIX agora também consegue aplicar diretamente esses perfis quando localiza `templates/dbsqllikemem` a partir do ambiente atual.
   - O diálogo também passou a exibir resumo do perfil selecionado com descrição, foco recomendado de testes e próxima janela de revisão, reduzindo ambiguidade na adoção da baseline.
   - Quando `review-metadata.json` diverge do catálogo interno, o resumo também passa a explicitar o drift de governança no próprio diálogo.
   - Quando a data `nextPlannedReviewOn` expira, o mesmo resumo também acusa revisão vencida antes da aplicação da baseline.
   - O resumo do perfil agora também mostra os diretórios recomendados de saída para `Model` e `Repository`, aproximando a escolha operacional do catálogo versionado.
- Templates customizados agora são validados contra o contrato de tokens suportados antes de serem salvos.
- Model e Repository agora também aceitam padrão configurável de nome de arquivo, reutilizando placeholders como `{NamePascal}`, `{Schema}`, `{DatabaseType}`, `{DatabaseName}` e `{Namespace}`.
- O mapeamento padrão por tipo de objeto também aceita `namespace` opcional reaproveitado na geração.
- Substituição de tokens no conteúdo durante a geração, incluindo `{{Namespace}}` quando configurado no mapeamento.
- Model e Repository gerados por template agora também recebem cabeçalho padronizado `// DBSqlLikeMem:*`, mantendo rastreabilidade de origem alinhada ao contrato já usado na geração principal.
- A checagem da VSIX agora também compara o snapshot estrutural desses artefatos (`Columns`, `ForeignKeys`, `Triggers` e metadados de sequência quando presentes) contra a classe principal gerada antes de marcar o trio como coerente.
- O mesmo `namespace` também pode entrar no padrão de nome de arquivo via `{Namespace}`.
- Geração também pode consumir objetos `Sequence` quando presentes na metadata carregada.

8. **Mapeamentos de geração realmente por conexão e tipo**
   - O menu **Configurar mapeamentos** da VSIX agora respeita o nó selecionado (`conexão + tipo de objeto`) em vez de reaplicar o mesmo padrão para toda a malha já configurada.
   - Ajustes em `Table`, `View`, `Procedure`, `Function` ou `Sequence` preservam os demais mapeamentos existentes da mesma conexão.
   - O mesmo diálogo agora também oferece perfis `API` e `Worker/Batch` para aplicar defaults versionados de pasta/padrão por tipo de objeto, alinhando a VSIX ao catálogo `templates/dbsqllikemem/vCurrent`.
   - A recomendação exibida no diálogo agora inclui também pasta/padrão sugeridos para o tipo atual, junto do contexto de foco e revisão do perfil.

9. **Checagem complementar de artefatos gerados**
   - A consistência considera também a presença de arquivos de Model e Repository, além das classes já geradas pelo fluxo principal.
   - Quando apenas parte do trio local existe, a VSIX agora sinaliza estado intermediário em vez de misturar esse caso com divergência pura de metadados.
   - O nó na árvore agora também exibe tooltip com o diagnóstico persistido da checagem, incluindo quais artefatos do trio local ainda faltam.
   - Quando `class/model/repository` existem, a VSIX agora também lê o snapshot `// DBSqlLikeMem:*` dos três artefatos para acusar drift de origem se algum arquivo local apontar para outro objeto.

10. **Importação e exportação de configurações**
   - Botões no topo para **Importar configurações** e **Exportar configurações** em JSON.
   - Exportação inclui conexões, mapeamentos e templates, com `ConnectionString` protegida (DPAPI por usuário).

## Compatibilidade VSIX

- Manifesto destinado ao Visual Studio **2022 e 2026 x64** (`[17.0,19.0)`) nas edições Community/Professional/Enterprise. A instalação e o carregamento nessas versões ainda precisam de validação do pacote final.
- A geração resolve pastas relativas a partir da solução aberta. Sem solução, usa o diretório atual.
- A prévia de sobrescrita abrange testes, models e repositories; destinos duplicados são rejeitados antes de gravar.
- O botão de cancelar permanece acessível durante operações longas.
- Exportações DPAPI são vinculadas ao usuário e ambiente Windows; importar em outro ambiente pode exigir cadastrar as conexões novamente.
- Consulte [a revisão de publicação](../RELEASE_REVIEW.md) para a validação restante.

## Publicação da VSIX

- Workflow: `.github/workflows/vsix-publish.yml`
- Secret: `VS_MARKETPLACE_TOKEN`
- Tag automática: `vsix-v*`
- Fonte da versão publicada: `src/extensions/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest`
- Contrato do workflow: `.github/workflows/vsix-publish.yml` valida explicitamente `src/extensions/DbSqlLikeMem.VisualStudioExtension/source.extension.vsixmanifest` antes do build/publish, mantendo o fluxo `tag vsix-v* -> source.extension.vsixmanifest -> publish`.
- Manifesto operacional: `eng/visualstudio/PublishManifest.json`
- Auditoria base: `python scripts/check_release_readiness.py`
- Auditoria estrita no publish: `python scripts/check_release_readiness.py --strict-marketplace-placeholders`
- Antes de criar a tag `vsix-v*`, revise `../../../CHANGELOG.md` e `../../../docs/publishing.md` para manter release notes e limitações abertas visíveis no fluxo de publicação.

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

## Depuração no Visual Studio 2022/2026

1. Defina **DbSqlLikeMem.VisualStudioExtension** como projeto de inicialização e selecione **Debug**.
2. Use o depurador **VSIX**. No seletor de destino da barra de depuração, escolha a instalação desejada do Visual Studio. Nas propriedades de debug, o destino é **VSIX Debug Target**.
3. Mantenha o sufixo **Exp**. Pressione **F5** para compilar, implantar a VSIX e abrir a segunda instância do Visual Studio.
4. Na janela **Experimental Instance**, abra o Explorer pelo menu ou comando descrito acima.

O projeto usa `DebuggerFlavor=VsixDebugger`, `VsixDeployOnDebug=true` e `RunUpdateConfigOnVsixDeploy=true`. O destino escolhido é armazenado localmente em `DbSqlLikeMem.VisualStudioExtension.csproj.user`, na propriedade `DeployTargetInstanceId`; esse arquivo não é versionado. A implantação e o depurador devem apontar para a mesma instalação.

Após atualizar estas configurações, recarregue o projeto no Visual Studio para atualizar a barra de depuração. Se ela continuar exibindo apenas perfis de executável, confira se o arquivo `.csproj.user` ainda contém algum `DebuggerFlavor=ProjectDebugger` e selecione o depurador VSIX nas propriedades.

Os perfis de `Properties/launchSettings.json` são alternativas para iniciar um executável manualmente. O caminho do `devenv.exe` nesses perfis deve corresponder à instalação escolhida para implantar a extensão. Para F5 com implantação coordenada, use o depurador VSIX.

### Pontos de interrupção e diagnóstico

- `DbSqlLikeMemExtensionPackage.InitializeAsync`: inicialização do pacote.
- `OpenToolWindowCommand.InitializeAsync`: registro do comando.
- `OpenToolWindowCommand.Execute`: clique no menu ou execução de `DbSqlLikeMem.OpenExplorer`.
- `DbSqlLikeMemToolWindow` e `DbSqlLikeMemToolWindowControl`: criação da tela.

Com `AdditionalArguments=/log`, o ActivityLog fica em:

- VS 2022: `%APPDATA%\Microsoft\VisualStudio\17.0_*Exp\ActivityLog.xml`.
- VS 2026: `%APPDATA%\Microsoft\VisualStudio\18.0_*Exp\ActivityLog.xml`.

Busque por `DbSqlLikeMemExtensionPackage`, `OpenToolWindowCommand` ou pelo GUID `f175ddf6-0067-43ed-9fd7-5780f8e8ff70`. O registro bem-sucedido grava `Registered DbSqlLikeMem.OpenExplorer.`; a abertura grava `Opened DbSqlLikeMem Explorer.`. Falhas de registro deixam de ser ignoradas e falhas ao abrir exibem uma mensagem e o detalhe no log.

Se o menu continuar ausente após F5, confira o horário da DLL implantada em `%LOCALAPPDATA%\Microsoft\VisualStudio\<instância>Exp\Extensions` e o ActivityLog dessa mesma instância. A presença na lista de extensões confirma a instalação do manifesto; o registro dos comandos e o carregamento do pacote precisam funcionar também.

### Contrato do menu

`Menus.vsct` gera `Menus.cto`, mesclado pelo VSSDK no recurso neutro da extensão. O nome `Menus.ctmenu` é compartilhado por `VSCTCompile/ResourceName` e `ProvideMenuResource`. A versão do recurso é 2 para solicitar a atualização do registro de menus após a alteração.

O botão usa diretamente o grupo padrão de **Outras Janelas**, com uma segunda posição no menu **Ferramentas**. O nome canônico começa com ponto no VSCT para manter `DbSqlLikeMem.OpenExplorer` independente do menu e do idioma.

Referências: [recursos de comandos VSCT](https://learn.microsoft.com/en-us/visualstudio/extensibility/internals/how-to-create-a-dot-vsct-file?view=visualstudio) e [nomes canônicos dos comandos](https://devblogs.microsoft.com/visualstudio/improve-the-commands-in-your-extensions/).

## Harness local para validar XAML (fora do VS)

- Foi adicionado o projeto `DbSqlLikeMem.VisualStudioExtension.XamlHarness`, uma aplicação WPF simples para validar se os XAML da extensão estão carregando corretamente sem depender do host do Visual Studio.
- Esse projeto ajuda no cenário em que a VSIX não aparece no menu de debug do Visual Studio, permitindo testar o `DbSqlLikeMemToolWindowControl` e os diálogos (`ConnectionDialog`, `MappingDialog`, `TemplateConfigurationDialog`) de forma isolada.
- Execução:

```bash
dotnet run --project src/extensions/DbSqlLikeMem.VisualStudioExtension.XamlHarness/DbSqlLikeMem.VisualStudioExtension.XamlHarness.csproj
```
