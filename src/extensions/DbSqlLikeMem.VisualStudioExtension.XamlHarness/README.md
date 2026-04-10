# DbSqlLikeMem.VisualStudioExtension.XamlHarness

Aplicacao WPF simples para validar o carregamento dos XAML da extensao fora do Visual Studio.

## Como usar

```bash
dotnet run --project src/DbSqlLikeMem.VisualStudioExtension.XamlHarness/DbSqlLikeMem.VisualStudioExtension.XamlHarness.csproj
```

O projeto agora expõe dois profiles em `Properties/launchSettings.json`:
- `DbSqlLikeMem.XamlHarness`
- `DbSqlLikeMem.XamlHarness (Load Environment)` com `--load-harness-environment`

Exemplo para abrir o profile com ambiente:

```bash
dotnet run --project src/DbSqlLikeMem.VisualStudioExtension.XamlHarness/DbSqlLikeMem.VisualStudioExtension.XamlHarness.csproj --launch-profile "DbSqlLikeMem.XamlHarness (Load Environment)"
```

Ao abrir com o profile de ambiente, o harness:
- cria conexoes com os bancos do docker quando as connection strings de benchmark estao presentes
- cria dados de teste com tabelas, views, procedures, functions e sequences quando o provedor suporta o tipo
- remove os objetos criados ao fechar a aplicacao

Dentro do Visual Studio, o estado das conexoes da janela principal passa a ser salvo por workspace quando a solucao esta aberta.
Se existir um estado global antigo, ele e carregado e migrado para o workspace atual na primeira abertura.

Ao abrir sem o profile de ambiente, a janela carrega `DbSqlLikeMemToolWindowControl` e disponibiliza botoes para abrir:
- `ConnectionDialog`
- `MappingDialog`
- `TemplateConfigurationDialog`
