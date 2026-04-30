# Paridade completa das extensions: bancos, routines e ícones

## Resumo
- O objetivo vai ser alinhar as extensions com o que o core do projeto já sabe fazer hoje, sem deixar `Procedure` e `Function` como “tipos de tela” apenas.
- Vamos fechar três frentes ao mesmo tempo: cobertura de bancos, suporte real a routines (`Procedure` e `Function`) e ícones consistentes nos menus do Visual Studio e do VS Code.
- A melhor forma de manter isso estável é criar uma fonte única de verdade para tipos de banco e tipos de objeto, e reutilizar o máximo possível do core C# nas duas extensions.

## Mudanças-chave
- [100%] Core runtime e metadados: expor uma API pública para registrar routines, porque hoje o core já executa `CreateFunction` e `CreateProcedure`, mas isso ainda está interno; também publicar a factory de user-defined function e serializar routines no snapshot.
- [100%] Modelo de objetos: adicionar `Function` ao `DatabaseObjectType`, atualizar grupos de árvore, mapeamentos padrão, consistência, leitura/gravação de estado e baselines de template para aceitar `Table`, `View`, `Procedure`, `Function` e `Sequence`.
- [100%] Geração: substituir o fluxo atual “sequência ou tabela” por um dispatcher por família de objeto, com builders separados para tabela/view, routine (`Procedure`/`Function`) e sequence, usando metadados próprios de parâmetros e retorno para routines.
- [100%] Bancos faltantes no core da extension: incluir os providers que hoje não estão cobertos nas estratégias de metadata, principalmente `Firebird` e `MariaDb`, e manter os aliases já existentes normalizados.
- [100%] Visual Studio: ampliar o seletor de conexão e o executor ADO.NET para os bancos faltantes, e aplicar ícones nos itens do menu de contexto do tool window usando monikers nativos do Visual Studio.
- [100%] VS Code: centralizar a lista de bancos suportados, adicionar as opções faltantes (`SqlAzure`, `Db2`, `Firebird`, `MariaDb`), estender o modelo de mapping/template para `Function`, e trocar o provider SQL Server-only por um helper de metadata compartilhado em .NET para não ficar preso em `[]` para os outros bancos.
- [100%] Ícones no VS Code: adicionar ícones aos comandos do `view/title` e aos itens sem ícone no `view/item/context`, especialmente `editConnection` e o segundo `removeConnection`, usando apenas codicons nativos.
- [100%] Compatibilidade: manter os arquivos de estado antigos carregando, preenchendo mappings faltantes automaticamente para `Function` e preservando nomes legados/aliases de banco sem quebrar projetos já configurados.

## Testes
- [100%] Cobrir o core com testes para enum/labels novos, defaults de mapping, baselines de template, leitura de snapshot e geração de routines.
- [100%] Cobrir metadata com casos para `Firebird` e `MariaDb`, além de rotinas com parâmetros e retorno, para garantir que o que entra no catálogo sai corretamente no gerador.
- [100%] Cobrir o VS Code com testes de `generation-support`, `template-baselines` e a nova camada de metadata helper, garantindo que os bancos e tipos de objeto apareçam no mesmo formato em toda a UI.
- [100%] Validar o Visual Studio com checagem dos estados de menu/visibilidade e uma revisão manual dos ícones no tool window para confirmar que os comandos ficaram consistentes.
- [100%] Preservar regressões de `Table`, `View` e `Sequence` com testes de não-regressão, para não mexer no que já funciona.

## Assunções
- `Procedure` deixa de ser só um nome na UI e passa a ter caminho de geração próprio, junto com `Function`.
- `Function` será tratada primeiro como rotina user-defined escalar; suporte adicional só entra se o runtime já expuser isso de forma natural.
- Para os ícones, vamos preferir recursos nativos da plataforma, sem criar arte nova customizada, a menos que algum comando não tenha moniker/codicon apropriado.
- No VS Code, a solução de paridade completa vai usar um helper de metadata em .NET compartilhado com o core, porque o pacote atual não tem drivers suficientes para suportar todos os bancos diretamente em TypeScript.

## Testes Containers
- [100%] Criar testes de banco para validar as queries do generator nas bases reais de MySql, MariaDb, Npgsql, SQL Server, Oracle, Db2 e Firebird usando as connection strings de benchmark configuradas por ambiente.
