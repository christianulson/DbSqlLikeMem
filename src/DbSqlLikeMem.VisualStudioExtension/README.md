# DbSqlLikeMem.VisualStudioExtension

Projeto VSIX para hospedar a interface do DbSqlLikeMem no Visual Studio.

## Evoluções implementadas

1. **Conexões reais + ciclo de vida**
   - Teste de conexão ao adicionar/editar.
   - Ações de editar e remover conexão.
   - Persistência protegida da connection string (DPAPI por usuário).

2. **Carregamento real de objetos**
   - Botão **Atualizar objetos** para listar metadados estruturais via `SqlDatabaseMetadataProvider`.

3. **Menus de contexto na árvore**
   - **Gerar classes**
   - **Checar consistência**

4. **Fluxo de geração com prévia de conflitos**
   - Pré-visualização de arquivos já existentes (sobrescrita) antes de gerar.

5. **Indicadores visuais de consistência**
   - Nó de objeto com marcador de status: 🟢 sincronizado, 🟡 divergente, 🔴 ausente.

6. **Hardening básico**
   - Mensagens de status operacionais na UI.
   - Log local em `%LocalAppData%/DbSqlLikeMem/visual-studio-extension.log`.

## Compatibilidade VSIX

- Compatível com Visual Studio **2019, 2022 e linha futura (incluindo 2026)** (`[16.0,19.0)`) nas edições Community/Professional/Enterprise.


## Qualidade e performance

- Operações longas com proteção contra concorrência (uma operação por vez) e cancelamento manual.
- Refresh de objetos com execução paralela por conexão para reduzir tempo total em cenários multi-banco.
- Checagem de consistência com processamento paralelo e propagação de cancelamento.
- Timeout de teste de conexão para evitar bloqueios longos na UI.
- Tratamento centralizado de exceções em eventos da UI (resiliência + log).
