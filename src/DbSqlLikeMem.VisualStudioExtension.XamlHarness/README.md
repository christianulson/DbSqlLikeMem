# DbSqlLikeMem.VisualStudioExtension.XamlHarness

Aplicação WPF simples para validar carregamento dos XAML da extensão fora do Visual Studio.

## Como usar

```bash
dotnet run --project src/DbSqlLikeMem.VisualStudioExtension.XamlHarness/DbSqlLikeMem.VisualStudioExtension.XamlHarness.csproj
```

Ao abrir, a janela carrega `DbSqlLikeMemToolWindowControl` e disponibiliza botões para abrir:
- `ConnectionDialog`
- `MappingDialog`
- `TemplateConfigurationDialog`
