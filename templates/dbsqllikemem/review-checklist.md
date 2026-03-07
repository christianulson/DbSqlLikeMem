# Template review checklist

Use este checklist antes de promover mudanças em `templates/dbsqllikemem/vCurrent` ou preparar conteúdo novo em `vNext`.

## Compatibilidade de tokens

1. Confirme que o template usa apenas os tokens suportados:
   - `{{ClassName}}`
   - `{{ObjectName}}`
   - `{{Schema}}`
   - `{{ObjectType}}`
   - `{{DatabaseType}}`
   - `{{DatabaseName}}`
   - `{{Namespace}}`
2. Se houver necessidade de um token novo, atualize primeiro:
   - `TemplateContentRenderer`
   - catálogo de tokens suportados
   - testes do core e da extensão VS Code
   - documentação em `templates/dbsqllikemem/README.md`

## Promoção de baseline

1. Registre o motivo da mudança em `CHANGELOG.md`.
2. Atualize o percentual correspondente em `docs/features-backlog/index.md`.
3. Registre contexto operacional em `docs/features-backlog/status-operational.md` quando a mudança ainda estiver em andamento.
4. Se a mudança sair de `vNext` para `vCurrent`, revise os perfis `api` e `worker` e confirme os diretórios padrão.

## Paridade entre extensões

1. Confirme que VSIX e VS Code continuam apontando para a mesma baseline em `templates/dbsqllikemem/vCurrent`.
2. Revise prompts/diálogos de configuração para evitar valores fictícios ou drift de caminhos.
3. Mantenha a mesma semântica de `{{Namespace}}` entre geração textual e geração estruturada.
