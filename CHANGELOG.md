# Changelog

Este arquivo registra mudanças relevantes por impacto funcional, com foco em provider/dialeto, automação e limitações conhecidas.

## [Unreleased]

### Core parser/executor

- `LIKE ... ESCAPE ...` agora é materializado na AST e respeitado no executor, em vez de ser apenas consumido no parse.
- A semântica de escape padrão do `LIKE` passou a ser dirigida pelo dialeto (`LikeDefaultEscapeCharacter`), removendo o hardcode único do helper comum.
- A trilha recebeu regressão objetiva em helper core, parser/roundtrip DB2 e execução DB2 end-to-end.
- `LIKE ... ESCAPE ...` agora também rejeita valores com mais de um caractere no parse literal e na avaliação parametrizada, mantendo o contrato do dialeto para cardinalidade do escape.
- `JSON_VALUE(... RETURNING <tipo>)` agora respeita gate explícito de dialeto no parser e aplica coerção do valor no executor, cobrindo o contrato Oracle e rejeitando a cláusula no SQL Server.
- `REGEXP` no executor agora também respeita política de case-sensitivity definida pelo dialeto, cobrindo a semântica default do MySQL.

### Cross-dialect

- Runner central de equivalência ganhou perfil `parser`, além de `smoke` e `aggregation`.
- Refresh, validação estrutural de snapshots e CI passaram a contemplar também o snapshot `parser`.
- O profile `parser` agora inclui `SqlAzure`, apoiado por suíte dedicada de parser por nível de compatibilidade.
- Runner central de equivalência agora também expõe perfil `strategy`, cobrindo a camada Strategy por trait compartilhado `Category=Strategy`.
- Refresh, placeholder versionado, validador estrutural e CI passaram a contemplar também o snapshot `strategy`.

### SQL Azure

- O dialeto interno do `SqlAzureDbMock` passou a mapear níveis de compatibilidade (`100/110/.../170`) para semântica correspondente de versões SQL Server (`2008/2012/.../2025`) antes de aplicar gates do parser.
- Parser do SQL Azure agora cobre `OFFSET/FETCH`, `LIMIT` com dica acionável, `MERGE`, `JSON_VALUE` e `STRING_AGG ... WITHIN GROUP` por nível de compatibilidade.
- Estratégia de triggers do SQL Azure agora tem suíte dedicada cobrindo tabelas não temporárias e temporárias, alinhando o provider ao contrato já validado nos demais dialetos principais.
- Estratégia transacional do SQL Azure agora tem suíte dedicada cobrindo `commit`, `rollback`, isolamento explícito, `Close`/`Open`, rollback para savepoint e invalidação após `ResetAllVolatileData`.

### MySQL

- `GROUP_CONCAT` agora cobre sintaxe nativa com `ORDER BY ... SEPARATOR ...`.
- Parser cobre `DISTINCT` + `ORDER BY` + `SEPARATOR` e erro acionável para `SEPARATOR` sem expressão.

### SQLite

- `GROUP_CONCAT` agora cobre ordenação interna via sintaxe nativa `ORDER BY` dentro da função.
- `WITHIN GROUP` continua explicitamente não suportado no dialeto SQLite.

### VS Code Extension

- Geração de classes de teste agora cria scaffold inicial explícito com metadados de origem, método determinístico e `[Fact(Skip = ...)]`, removendo o stub com `TODO`.
- Check de consistência da extensão VS Code agora valida efetivamente `teste + model + repository` por objeto usando os mesmos caminhos determinísticos da geração.
- O check de consistência da extensão VS Code agora persiste também o detalhe dos artefatos faltantes por objeto, exibindo tooltip na árvore e limpando diagnóstico residual quando o trio volta a ficar completo.
- O comando rápido `Configure Mappings` do VS Code passou a preservar/configurar `namespace`, alinhando-o ao fluxo visual do manager.
- O comando rápido `Configure Mappings` do VS Code agora também oferece defaults de pastas/sufixos de teste alinhados aos perfis `API` e `Worker/Batch`.
- A extensão VS Code agora também trata `Sequence` como tipo de objeto de primeira classe no manager, no comando `Configure Mappings`, na árvore e na geração/consistência por template; para SQL Server, a metadata real passou a listar `sys.sequences`.
- Helpers puros de geração para a extensão VS Code agora têm malha mínima de testes locais (`npm test`) e a pasta `tests/` foi excluída do pacote via `.vscodeignore`.
- `Configure Templates` agora oferece perfis prontos baseados em `templates/dbsqllikemem/vCurrent`, com baseline `API` e `Worker/Batch` antes da edição manual dos caminhos/pastas.
- A extensão VS Code agora valida tokens suportados em templates customizados e faz fallback para o template padrão quando encontra placeholders inválidos durante a geração.
- A extensão VS Code agora também aceita padrão configurável de nome de arquivo para `Model` e `Repository`, reutilizando o mesmo cálculo na geração e no check de consistência.
- Os quick picks de baseline da extensão VS Code agora também consomem `review-metadata.json` quando presente no workspace e expõem drift de governança em relação ao catálogo embutido.
- A extensão VS Code agora grava snapshot padronizado `// DBSqlLikeMem:*` também em `teste/model/repository` e usa esse cabeçalho para detectar drift de artefato no `Check Consistency`, não só ausência física.
- O snapshot do VS Code em `Model/Repository` agora também registra estrutura mínima (`Columns`/`ForeignKeys`) quando a metadata do objeto está disponível, permitindo detectar drift estrutural sem depender apenas da identidade do objeto.
- O provider SQL Server da extensão VS Code agora também carrega metadata de `Sequence` (`StartValue`, `IncrementBy`, `CurrentValue`) para alimentar o snapshot estrutural e detectar drift também nesse tipo de artefato.

### Tooling and docs

- Geração de Model/Repository na VSIX agora suporta `{{Namespace}}`, com renderização centralizada de tokens e persistência do namespace no mapeamento exportado/importado.
- Geração principal de classes na VSIX também passou a reaproveitar o `namespace` configurado por tipo de objeto no conteúdo estruturado emitido.
- O padrão de nome de arquivo da VSIX agora também aceita `{Namespace}`, mantendo o preview de conflitos e a checagem de consistência no mesmo contrato de mapeamento.
- O fluxo `Configure Mappings` da VSIX agora atualiza apenas o recorte selecionado (`conexão + tipo de objeto`), preservando mapeamentos já existentes dos outros tipos na mesma conexão.
- O diálogo `Configure Mappings` da VSIX agora também oferece perfis `api` e `worker` para aplicar defaults versionados de pasta/padrão por tipo de objeto, reaproveitando o mesmo catálogo operacional já consumido pelas outras trilhas de baseline.
- Os diálogos `Configure Mappings` e `Configure Templates` da VSIX agora também exibem resumo do perfil selecionado com descrição, foco recomendado de testes, revisão planejada e, no caso do mapping, a recomendação específica do tipo de objeto.
- A governança da baseline na VSIX agora também acusa drift quando `review-metadata.json` diverge do catálogo interno do core, em vez de apenas exibir o resumo feliz do perfil.
- A geração por template da VSIX agora também aceita padrão configurável de nome de arquivo para `Model` e `Repository`, reutilizando o mesmo resolvedor tanto na escrita quanto na checagem de consistência.
- A geração por template da VSIX agora também prependa snapshot padronizado `// DBSqlLikeMem:*` em `Model` e `Repository`, alinhando a rastreabilidade desses artefatos ao contrato já usado pela geração principal.
- A checagem de consistência da VSIX agora distingue trio local incompleto de divergência real de metadados, em vez de tratar ausência parcial de artefatos como diferença genérica.
- A árvore da VSIX agora também expõe tooltip com o diagnóstico persistido da consistência, incluindo os artefatos faltantes do trio local em ordem determinística.
- A checagem de consistência da VSIX agora também detecta drift de snapshot em `class/model/repository`, acusando quando algum artefato local aponta para outro objeto mesmo com o trio completo presente.
- A consistência da VSIX agora também compara o snapshot estrutural de `Model/Repository` contra a classe principal gerada e o leitor passou a preservar `Triggers`, reduzindo falso verde em artefatos complementares desatualizados.
- O diálogo `Configure Templates` da VSIX agora aplica diretamente perfis `api` e `worker` quando encontra `templates/dbsqllikemem`, reduzindo drift operacional em relação ao fluxo já introduzido na extensão VS Code.
- A VSIX agora valida templates customizados contra o catálogo de tokens suportados antes de aceitar a configuração, reduzindo risco de placeholders que o runtime não sabe renderizar.
- Baselines versionadas de template foram adicionadas em `templates/dbsqllikemem/vCurrent` com perfis `api` e `worker`, além de trilha controlada `vNext` para próxima promoção.
- `TemplateBaselineCatalog` no core e `template-baselines.ts` na extensão VS Code passaram a reutilizar essas baselines físicas como fonte de verdade para configuração inicial, com resolução da raiz mais próxima do repositório no fluxo da VSIX.
- `TemplateTokenCatalog` e `templates/dbsqllikemem/review-checklist.md` passaram a formalizar o contrato de tokens e a revisão periódica da baseline, com vigilância do auditor de release.
- O auditor de release agora também falha quando encontra placeholders `{{...}}` fora do contrato suportado nas baselines versionadas de template.
- A governança de revisão de templates agora também tem metadado versionado (`templates/dbsqllikemem/review-metadata.json`) com cadência trimestral, última revisão, próxima janela-alvo e evidências mínimas, validado pelo auditor.
- A governança de revisão de templates agora também acusa explicitamente quando a janela `nextPlannedReviewOn` expira, tanto nos resumos de baseline da VSIX/VS Code quanto no auditor de release.
- Os resumos compartilhados de baseline agora também mostram os diretórios recomendados de saída para `Model` e `Repository`, fechando a última lacuna prática entre catálogo versionado e configuração diária das extensões.
- Template de PR adicionado com vínculo explícito entre código, teste, backlog e evidência de validação.
- Checklist operacional para atualização de percentuais do backlog formalizado em `docs/features-backlog/progress-update-checklist.md`.
- Status operacional do backlog separado do índice macro em `docs/features-backlog/status-operational.md`.
- Auditoria automatizada de release adicionada em `scripts/check_release_readiness.py`, cobrindo snapshots, workflows, documentação e metadados básicos de publicação.
- Validação de metadados de pacote NuGet extraída para `scripts/check_nuget_package_metadata.py`, removendo lógica inline duplicada do workflow de publicação.
- Auditoria pós-pack do NuGet passou a derivar expectativas do `Directory.Build.props` e validar mais campos do `.nuspec`, incluindo `authors`, `readme`, `tags`, `releaseNotes`, licença e `repository type`.
- O gate pós-pack do NuGet agora também valida a versão do `.nuspec` contra `src/Directory.Build.props` e o sufixo do arquivo `.nupkg`, reduzindo risco de SemVer divergente no artefato efetivamente publicado.
- O gate pós-pack do NuGet agora também valida `requireLicenseAcceptance` no `.nuspec`, mantendo esse contrato alinhado ao `PackageRequireLicenseAcceptance` centralizado em `src/Directory.Build.props`.
- O checker de cobertura da solução `.slnx` agora normaliza separadores de caminho também do lado do XML, com suíte Python dedicada para evitar falso positivo entre barras invertidas do Windows e a validação em CI Linux.
- Auditoria de release agora valida também o formato SemVer das versões do core e das extensões, reduzindo risco de versionamento inválido antes da publicação.
- Os workflows de publish agora validam explicitamente a fonte de versão de cada artefato (`src/Directory.Build.props`, `source.extension.vsixmanifest`, `package.json`), e o auditor passou a exigir esse contrato junto dos prefixos de tag documentados.
- Os READMEs operacionais das extensões agora repetem explicitamente o contrato `workflow -> fonte de versão -> publish`, e o auditor passou a vigiar essa mensagem também no ponto de execução manual.
- O workflow NuGet agora também suporta `vars.NUGET_PUBLISH_ENVIRONMENT` com fallback para `nuget-publish`, alinhando o YAML ao contrato já documentado e tornando esse detalhe parte do gate de readiness.
- O workflow NuGet agora também executa `scripts/check_release_readiness.py` antes do `restore`, alinhando o publish do pacote ao mesmo gate documental/operacional já usado nos fluxos das extensões.
- `README.md` da raiz foi alinhado aos targets reais do repositório (`net462`, `netstandard2.0`, `net8.0`, com `net6.0` restrito à malha de testes), e o auditor passou a vigiar esse contrato documental.
- `src/README.md` também foi alinhado ao mesmo contrato de targets/override, reduzindo drift entre a documentação de pacote e a documentação da raiz.
- `docs/getting-started.md` passou a explicitar o mesmo contrato de frameworks/override, reduzindo ambiguidade para quem entra pelo guia de instalação.
- `docs/Wiki/Home.md` foi alinhado ao repositório oficial (`christianulson`) e o auditor passou a vigiar esses links base.
- `docs/Wiki/Getting-Started.md` foi alinhado ao mesmo contrato de frameworks/override e entrou na auditoria de release para reduzir drift entre wiki espelhada e guias canônicos.
- `docs/old/providers-and-features.md` passou a explicitar o mesmo contrato de frameworks para consumidores e entrou na auditoria, reduzindo drift no guia secundário de compatibilidade por provider.
- `docs/info/multi-target-compat-audit.md` passou a explicitar que é um artefato histórico e não a fonte de verdade para TFMs atuais; o auditor agora vigia essa advertência.
- `docs/Wiki/Publishing.md` e `docs/Wiki/Providers-and-Compatibility.md` entraram no mesmo gate documental da wiki, reduzindo drift entre páginas espelhadas e os guias canônicos do repositório.
- O gate documental e os links de entrada da documentação agora tratam `docs/Wiki` como caminho canônico da wiki espelhada em submódulo, com fallback apenas para o layout legado.
- O auditor de release agora também valida o contrato mínimo de comunicação de mudança: `CHANGELOG.md` com `Unreleased` + subseções + `Known limitations still open`, além de referências explícitas a release notes nos guias de publicação e nos READMEs das extensões.
- Os READMEs das extensões VS Code/VSIX entraram na trilha de auditoria de release; a VSIX também recebeu seção explícita de publicação com workflow, manifesto e gate estrito.
- A documentação de publish agora prende explicitamente a fonte de verdade de versão e o prefixo de tag para NuGet, VSIX e VS Code; o auditor passou a vigiar esse contrato.
- `docs/README.md` e a wiki em `docs/Wiki` passaram a expor essa trilha de descoberta, reduzindo drift nos pontos de entrada da documentação.
- Auditoria de release passou a validar também integridade mínima das extensões: scripts/arquivos do pacote VS Code, activation events válidos e campos essenciais do manifesto de publicação VSIX.
- Auditoria de release agora vigia também a presença e o contrato mínimo das baselines versionadas em `templates/dbsqllikemem`, evitando drift entre backlog, docs e artefatos de geração.
- Compatibilidade declarada da VSIX foi alinhada para Visual Studio `17.0+`, e o auditor agora cruza `MinimumVisualStudioVersion` com o range suportado no manifesto.
- Workflows de publicação das extensões passaram a executar o auditor de release; no fluxo VSIX, o publish usa modo estrito para bloquear placeholder de `publisher`.
- Auditoria do pacote VS Code passou a validar também placeholders `%...%` contra `package.nls*.json` e a presença da pasta `l10n`.
- Governança documental do backlog passou a entrar no auditor de release, incluindo presença/contrato mínimo de `progress-update-checklist.md` e `.github/pull_request_template.md`.
- URLs de repositório da extensão VS Code e do manifesto VSIX foram alinhadas ao repositório oficial.

### Known limitations still open

- Build/test completos desta rodada permanecem pendentes até a execução final consolidada.
- `eng/visualstudio/PublishManifest.json` ainda depende da definição final do `publisher` do Visual Studio Marketplace; o novo auditor reporta esse ponto como warning por padrão.
