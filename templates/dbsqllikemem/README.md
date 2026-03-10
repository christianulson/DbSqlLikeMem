# Template baselines

Este diretório versiona os templates compartilhados de geração usados como baseline operacional pelas extensões.

Estrutura atual:

- `vCurrent/`: baseline estável recomendada para uso corrente.
- `vNext/`: área reservada para evolução planejada antes de promover uma nova baseline estável.
- `review-checklist.md`: checklist de compatibilidade e promoção antes de alterar templates compartilhados.
- `review-metadata.json`: metadado versionado com cadência, última revisão, próxima janela-alvo e evidências mínimas da baseline.
  - a janela `nextPlannedReviewOn` agora também é tratada como gate operacional: VSIX/VS Code acusam revisão vencida no resumo do perfil e o auditor de release falha quando a revisão trimestral expira.

Perfis publicados em `vCurrent`:

- `api`: saída voltada para soluções com foco em leitura, modelagem e repositórios de aplicação.
- `worker`: saída voltada para soluções de batch/worker com ênfase em processamento e consistência operacional.

Arquivos principais:

- `model.template.txt`
- `repository.template.txt`

Tokens suportados:

- `{{ClassName}}`
- `{{ObjectName}}`
- `{{Schema}}`
- `{{ObjectType}}`
- `{{DatabaseType}}`
- `{{DatabaseName}}`
- `{{Namespace}}`
