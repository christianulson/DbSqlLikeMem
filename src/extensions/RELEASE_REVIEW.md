# Revisão das extensões para publicação

## Resultado

Foram corrigidos defeitos no código e nos contratos de publicação das extensões VS Code e Visual Studio. A aprovação final do release depende de gerar, instalar e exercitar os pacotes nos hosts reais.

## Correções

### Ciclo 3 (aprofundamento de UI/UX, consistência e localização)

| Área | Problema encontrado | Alteração |
| --- | --- | --- |
| VSIX: localização | Mensagens de status, erro e operação em português hardcoded no ViewModel; textos fixos nos diálogos de mapping/templates; `OpenToolWindowCommand` com string em encoding corrompido | Strings movidas para `Resources.resx` + resx localizados; mojibake corrigido com recurso `FailedToOpenToolWindow` |
| VSIX: consistência | Cada objeto relistava todos os objetos no `GetObjectAsync` durante o check (O(N²) de listagens) | Overload com coleção já listada em `IDatabaseMetadataProvider`, `SqlDatabaseMetadataProvider` e `ObjectConsistencyChecker`; ViewModel reutiliza a lista carregada |
| VSIX: código morto | `ObjectFilterText`, `ObjectFilterMode` e `FindExistingFiles` sem uso | Reimplementado: filtro global de objetos na toolbar (Texto + modo + limpar, com debounce) usando as propriedades; `FindExistingFiles` removido por ser duplicação do preview de conflitos |
| VSIX: encoding | `FilterMode.cs` com caracteres corrompidos | Documentação normalizada para UTF-8 |
| VSIX: validação | Mapping aceitava padrão de arquivo e namespace que gerariam C#/arquivo inválido | Validação de padrão via `GeneratedFilePath.Resolve` com exemplo resolvido e regex de namespace C# |
| VSIX: UI/UX | Extração de cenário desabilitava o diálogo inteiro durante a query; preview de sobrescrita truncava sem indicar o restante; erros de UI mostravam mensagem crua; seleção da árvore era perdida após refresh | Fechar permanece habilitado durante operação; contagem de arquivos restantes no preview; erro com contexto `UnexpectedErrorDetail`; seleção e expansão restauradas após cada operação |
| VSIX: acessibilidade | Controles dos diálogos sem nomes para leitores de tela | `AutomationProperties.Name` nos campos de conexão e de cenário |
| VSIX: logging | `ExtensionLogger` com `AppendAllText` concorrente podia perder logs em refresh paralelo | Escrita protegida por lock |
| VSIX: workflow | Localização do `.vsix` varria o repositório inteiro e podia pegar artefato errado | Busca restrita ao diretório do projeto VSIX |
| VS Code: contrato | Títulos de comandos sem tokens nls (não traduzíveis) | Tokens `%command.*.title%` adicionados com traduções pt-BR |
| VS Code: UI | Strings de seções/tooltips da árvore sem l10n; IDs de nós colidiam para objetos que diferem só por caixa | `vscode.l10n.t` nas seções e tooltips; índice único no ID do nó |
| VS Code: validação | Cadastro/edição bloqueado quando o banco está indisponível; senhas com `;` quebravam o parse do fallback SQL Server | Opção "Save anyway" na falha de validação; parser de connection string extraído para `connection-string.ts` com suporte a aspas/chaves e 8 testes novos |
| VS Code: cobertura | `extension.ts` sem testes para lógica pura | Parser de connection string isolado e testado |

### Ciclo 2 (revisão de UI/UX, funcionalidades e contratos)

| Área | Problema encontrado | Alteração |
| --- | --- | --- |
| VSIX: consistência | Cabeçalho `// DBSqlLikeMem:*` da classe estruturada não emitia `Triggers`; o provider grava 5 chaves e a comparação exige contagem idêntica, então toda tabela/view gerada era marcada como divergente | `StructuredClassContentFactory` agora emite `Triggers` no cabeçalho, alinhado ao provider e ao renderizador de templates |
| VSIX: MySQL/MariaDB | `QuoteIdentifier` usava aspas duplas (literal de string) para todos os não–SQL Server, quebrando preview/extração de cenário | Backticks para MySQL/MariaDB; aspas duplas preservadas nos demais provedores |
| VSIX: publicação | `internalName` do `PublishManifest.json` divergia do `Identity Id` do manifesto; publisher com caixa divergente; sem licença no pacote; versão do registro de produto defasada | `internalName` alinhado ao Id, publisher normalizado para `dbsqllikemem`, `<License>LICENSE</License>` com o arquivo incluído no VSIX, `InstalledProductRegistration` com a versão do manifesto; validação cruzada adicionada ao `check_release_readiness.py` |
| VSIX: tipos | `max_length` em bytes dobrava o size de `nchar`/`nvarchar`/`sysname` no SQL Server; DB2 usava `SCALE` como precisão; Firebird fabricava metadados de sequence | Divisão por 2 para tipos de caractere no SQL Server; `LENGTH` como precisão de `DECIMAL`/`NUMERIC` no DB2; NULL honesto para metadados de sequence Firebird |
| VSIX: geração | Identificadores C# podiam começar com dígito; `BodySql` vazio era emitido como literal `"NULL"` | Prefixo `_` em identificadores com dígito inicial; `null` quando o corpo da função não existe |
| VSIX: UI/UX | `TemplateConfigurationDialog` validava caminhos relativos ao CWD e criava pastas lá; coluna `_Selected` exibida no grid de cenário; `DimGray` fixo quebrava contraste no tema escuro | Validação relativa ao workspace; cabeçalho localizado `SelectColumnHeader`; opacidade no lugar de cor fixa |
| VSIX: operações | Importar/exportar/extrair cenário rodavam fora do lock de operação; "Cancel operation" aparecia sem haver operação; refresh do menu refrescava todas as conexões | `TryBeginOperation`/`EndOperation` nos três fluxos; cancelamento só visível com `IsBusy`; refresh por conexão selecionada |
| VSIX: estado | Remoção de conexão não limpava o mapa de saúde; edição abria com `DatabaseName` em vez do nome amigável; `GetObjectAsync` relistava objetos | `healthByObject.Clear()` na remoção, `FriendlyName` na edição, overload com lista pré-carregada para o enriquecimento |
| VSIX: driver | `AdoNetSqlQueryExecutor` sem `CommandTimeout`; alias MySQL com aspas simples | `CommandTimeout = 60`; alias com backticks |
| VS Code: ativação | Estado corrompido/legado (vault indisponível, mapeamento órfão) derrubava a ativação inteira | `loadStateSafely` com reset guiado por erro; `normalizeState` tolerante na ativação e estrito na importação |
| VS Code: contrato | 4 comandos sem `activationEvents` (command not found no palette); 11 chaves l10n pt-BR ausentes e 4 obsoletas | Eventos adicionados; bundle completado e limpo |
| VS Code: UX | Notificação dupla ao salvar conexão no manager; seleção de conexão perdida após re-render; filtro ativo reduzia o escopo de geração/consistência sem aviso; aviso de colisão não mencionava padrão de nome | `notifySuccess` configurável; seleção preservada via `preferredConnectionId`; aviso de filtro nas gerações e checagem; mensagem de colisão orienta padrão de nome |
| VS Code: pipeline | `npm test` nunca rodava no publish; avisos de falha de conexão nunca eram limpos | Steps de compile/test no workflow; `clearConnectionWarnings` após salvar/editar conexão |
| VS Code: import | `JSON.parse` sem try/catch e `generationCheckByObjectKey/Details` sem validação de tipo | Try/catch com mensagem localizada; validação de forma no `normalizeState` |

### Ciclo 1 (revisão inicial)

| Área | Problema encontrado | Alteração |
| --- | --- | --- |
| VS Code: credenciais | Connection strings no globalState, no JSON exportado e em argumentos do processo | Migração para SecretStorage, exportação sem credenciais e envio ao bridge via stdin. SQLCMDPASSWORD substitui o argumento de senha do fallback. |
| VS Code: operações | Erros não tratados, comandos concorrentes e conexão indisponível confundida com lista vazia | Tratamento centralizado, indicação de progresso, exclusão confirmada e diagnóstico no nó da conexão. Lista vazia não recebe mensagem de consistência OK. |
| VS Code: importação | JSON aceito sem validação da estrutura | Validação anterior à substituição, confirmação e orientação para informar credenciais após importar. |
| VS Code: Manager | Edição exigia a senha novamente, sem ação para sair da edição; rótulos sem associação e layout fixo | Manutenção da credencial quando o campo está vazio, botão New connection, foco na edição, labels, foco visível, tema e layout de uma coluna em painéis estreitos. |
| VS Code: webview | JSON de mapeamento podia encerrar o script; ícones dependiam de caminho interno do editor | Escape de caracteres de abertura HTML e ações com texto. Ícones da árvore declarados nos comandos. |
| Geração | Objetos de schemas diferentes podiam sobrescrever o mesmo destino no lote | Pré-validação de destinos duplicados e nomes de arquivos antes de gravar, em ambas as extensões. |
| Templates | Template ausente/inválido podia ser substituído silenciosamente na geração do VS Code | Erro interrompe a operação. VSIX também revalida os tokens antes de gravar. |
| VSIX: sobrescrita | Models/repositories não tinham confirmação; ação conjunta previa apenas testes | Prévia cobre todos os artefatos solicitados. |
| VSIX: caminhos | Diretórios relativos dependiam do diretório atual do processo | Resolução a partir da solução aberta para geração, templates e consistência. |
| VSIX: operações | Overlay impedia cancelar, fundo branco fixo e alterações disponíveis durante execução | Cancelamento dentro do overlay, cores do host e bloqueio da barra durante a operação. |
| VSIX: cenários | Exceções em async void, dados antigos após trocar a tabela e valores do banco editáveis | Tratamento de erros, limpeza da prévia e edição restrita à seleção. Coluna de seleção não colide com nomes do banco. |
| VSIX: importação | Falha de DPAPI podia ocorrer depois de limpar as conexões atuais | Descriptografia completa antes de substituir o estado. |
| Publicação | Manifesto VSIX legado, licença fora do pacote VS Code, dependência SQLite sem bundle nativo | Manifesto 2.0 com amd64, licença MIT incluída e Microsoft.Data.Sqlite nos dois hosts. |
| Workflows | Instalação npm não reproduzível e reconstrução no publish | npm ci, SDK .NET explícito e publicação do mesmo VSIX produzido. Gate estrito da VSIX interrompe em falha. |

## Verificação realizada

- Análise de tipos TypeScript concluída sem erros: `node node_modules/typescript/lib/tsc.js -p . --noEmit`, na pasta da extensão VS Code.
- Testes da extensão VS Code executados: 50 testes passando (incluindo os 8 novos de `connection-string`), após recompilar `out/` e corrigir testes de baseline que dependiam de datas do catálogo e da data do sistema.
- Compilação do metadata bridge .NET 8 validada via `npm run compile:bridge` (cobre as alterações do Core, incluindo o novo overload de `IDatabaseMetadataProvider`).
- `python scripts/check_release_readiness.py --strict-marketplace-placeholders` executado com PASS e `python -m unittest scripts.test_check_release_readiness` com 15/15 OK (após instalação do Python 3.13 e correção de portabilidade Windows).
- Sintaxe TypeScript e JSON verificada; XML dos XAML, projetos alterados e manifestos VSIX carregados com sucesso.
- `vsce ls --no-dependencies` confirmou a presença de `LICENSE`, `resources/icon.png`, `out/extension.js` e dos assets nativos do bridge (SQLite `e_sqlite3`, SQLitePCLRaw, clidriver).
- Varredura de encoding confirmou que o mojibake de `OpenToolWindowCommand.cs` era o único caso real nos arquivos do VSIX.
- Revisão de diffs e de finais de linha CRLF.
- Builds e testes .NET não executados conforme a política do `AGENTS.md`.
- O navegador falhou ao iniciar nas tentativas de conexão. Não há evidência de renderização ou interação da interface nesta revisão.
- Não foram realizadas conexões a bancos, geração de VSIX nem instalação nos hosts. A seleção de provedores no código não comprova compatibilidade operacional de todos eles.

## Validação necessária antes de publicar

1. Em ambiente com Node e SDK .NET 8, executar na extensão VS Code:
   `npm ci`, `npm run compile`, `npm test` e `npm run package`.
2. No Developer PowerShell do Visual Studio, gerar a VSIX:
   `msbuild src/extensions/DbSqlLikeMem.VisualStudioExtension/DbSqlLikeMem.VisualStudioExtension.csproj /restore /t:Build /p:Configuration=Release /p:DeployExtension=false`.
3. Executar os projetos de testes `DbSqlLikeMem.VisualStudioExtension.Core.Test` e `DbSqlLikeMem.VisualStudioExtension.Tests`.
4. Instalar os pacotes gerados no VS Code e Visual Studio 2022/2026 x64. Conferir carregamento de drivers e dependências nativas, inclusive SQLite (SQLitePCLRaw.batteries_v2, provider.dynamic_cdecl e e_sqlite3) e DB2 (clidriver) no conteúdo do VSIX.
5. Exercitar criar/editar/remover conexão; importar/exportar; refresh com conexão indisponível; filtrar; gerar e cancelar; rejeitar colisões entre schemas; checar consistência após alterar o banco (inclusive a marcação de tabelas/views com triggers).
6. Revisar UI com teclado, tema claro/escuro e janela estreita; conferir cancelamento da VSIX, troca de tabela na extração de cenário, coluna de seleção com cabeçalho localizado e extração em MySQL/MariaDB (backticks).
7. Executar `scripts/check_release_readiness.py --strict-marketplace-placeholders`; confirmar publisher, versões, tags, `internalName`/`Id` e credenciais do Marketplace antes de acionar o publish. (Validado nesta revisão com PASS após a instalação do Python 3.13.)

## Limites funcionais que permanecem explícitos

- VS Code usa configuração global e gera na primeira pasta do workspace; não há seleção de destino por pasta em workspace com múltiplas raízes.
- O bridge é .NET 8 x64; o runtime deve existir no ambiente que executa a extensão, inclusive em sessões remotas. A matriz de sistemas operacionais/provedores precisa de teste real.
- Templates de baseline dependem dos arquivos disponíveis no workspace. Os templates padrão de model/repository são esqueletos; testes gerados no VS Code são inicialmente ignorados até serem implementados.
- Exportações da VSIX usam DPAPI e dependem do mesmo contexto de usuário/ambiente Windows.
- Um cancelamento após começar a gravação pode deixar arquivos já gerados; não há transação de filesystem.
- A geração de classes de teste no VS Code colide quando dois schemas têm o mesmo nome de objeto; a mensagem orienta o uso de padrão de nome, mas classes de teste não têm padrão configurável por objeto (os fluxos model/repository têm).
- O runtime WPF e o pacote VSIX ainda precisam de compilação para confirmar ausência de diagnósticos C# e carregamento dos recursos do tema.

## Referências dos contratos revisados

- [Comandos e ícones do VS Code](https://code.visualstudio.com/api/references/contribution-points#contributes.commands).
- [SecretStorage](https://code.visualstudio.com/api/references/vscode-api#SecretStorage).
- [Manifesto VSIX 2.0](https://learn.microsoft.com/en-us/visualstudio/extensibility/vsix-extension-schema-2-0-reference?view=vs-2022).
