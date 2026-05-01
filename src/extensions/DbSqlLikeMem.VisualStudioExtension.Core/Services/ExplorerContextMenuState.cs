namespace DbSqlLikeMem.VisualStudioExtension.Core.Services;

/// <summary>
/// EN: Describes the effective explorer selection used to compute context menu state.
/// PT-br: Descreve a selecao efetiva do explorador usada para calcular o estado do menu de contexto.
/// </summary>
public sealed class ExplorerContextMenuSelection
{
    /// <summary>
    /// EN: Creates a selection snapshot for the explorer context menu rules.
    /// PT-br: Cria um snapshot de selecao para as regras do menu de contexto do explorador.
    /// </summary>
    public ExplorerContextMenuSelection(
        bool isConnectionNodeSelected,
        bool isSchemaNodeSelected,
        bool isObjectTypeNodeSelected,
        bool isTableNodeSelected,
        bool hasObjectTypeFilter,
        bool isGenerationSupportedSelected)
    {
        IsConnectionNodeSelected = isConnectionNodeSelected;
        IsSchemaNodeSelected = isSchemaNodeSelected;
        IsObjectTypeNodeSelected = isObjectTypeNodeSelected;
        IsTableNodeSelected = isTableNodeSelected;
        HasObjectTypeFilter = hasObjectTypeFilter;
        IsGenerationSupportedSelected = isGenerationSupportedSelected;
    }

    /// <summary>
    /// EN: Gets whether the selected node is a connection.
    /// PT-br: Obtem se o no selecionado e uma conexao.
    /// </summary>
    public bool IsConnectionNodeSelected { get; }

    /// <summary>
    /// EN: Gets whether the selected node is a schema.
    /// PT-br: Obtem se o no selecionado e um schema.
    /// </summary>
    public bool IsSchemaNodeSelected { get; }

    /// <summary>
    /// EN: Gets whether the selected node is an object-type grouping node.
    /// PT-br: Obtem se o no selecionado e um no de agrupamento por tipo de objeto.
    /// </summary>
    public bool IsObjectTypeNodeSelected { get; }

    /// <summary>
    /// EN: Gets whether the selected node represents a table object.
    /// PT-br: Obtem se o no selecionado representa um objeto de tabela.
    /// </summary>
    public bool IsTableNodeSelected { get; }

    /// <summary>
    /// EN: Gets whether the selected object type already has a filter value.
    /// PT-br: Obtem se o tipo de objeto selecionado ja possui um valor de filtro.
    /// </summary>
    public bool HasObjectTypeFilter { get; }

    /// <summary>
    /// EN: Gets whether the selected node supports generation actions.
    /// PT-br: Obtem se o no selecionado suporta acoes de geracao.
    /// </summary>
    public bool IsGenerationSupportedSelected { get; }

    /// <summary>
    /// EN: Gets whether scenario extraction should be available for the current selection.
    /// PT-br: Obtem se a extracao de scenario deve ficar disponivel para a selecao atual.
    /// </summary>
    public bool CanExtractScenario => IsConnectionNodeSelected || IsSchemaNodeSelected || IsObjectTypeNodeSelected || IsTableNodeSelected;

    /// <summary>
    /// EN: Gets whether the object type filter can be cleared for the current selection.
    /// PT-br: Obtem se o filtro de tipo de objeto pode ser limpo para a selecao atual.
    /// </summary>
    public bool CanClearObjectTypeFilter => IsObjectTypeNodeSelected && HasObjectTypeFilter;
}

/// <summary>
/// EN: Describes the visibility state of the explorer context menu commands.
/// PT-br: Descreve o estado de visibilidade dos comandos do menu de contexto do explorador.
/// </summary>
public sealed class ExplorerContextMenuState
{
    /// <summary>
    /// EN: Creates a visibility state for the explorer context menu commands.
    /// PT-br: Cria um estado de visibilidade para os comandos do menu de contexto do explorador.
    /// </summary>
    public ExplorerContextMenuState(
        bool editConnectionVisible,
        bool removeConnectionVisible,
        bool refreshConnectionVisible,
        bool cancelConnectionOperationVisible,
        bool connectionActionsSeparatorVisible,
        bool configureMappingsVisible,
        bool configureTemplatesVisible,
        bool configureObjectTypeFilterVisible,
        bool clearObjectTypeFilterVisible,
        bool generationActionsSeparatorVisible,
        bool generateAllClassesVisible,
        bool generateByTypeSeparatorVisible,
        bool generateTestClassesVisible,
        bool generateModelClassesVisible,
        bool generateRepositoryClassesVisible,
        bool checkConsistencyVisible,
        bool extractScenarioVisible)
    {
        EditConnectionVisible = editConnectionVisible;
        RemoveConnectionVisible = removeConnectionVisible;
        RefreshConnectionVisible = refreshConnectionVisible;
        CancelConnectionOperationVisible = cancelConnectionOperationVisible;
        ConnectionActionsSeparatorVisible = connectionActionsSeparatorVisible;
        ConfigureMappingsVisible = configureMappingsVisible;
        ConfigureTemplatesVisible = configureTemplatesVisible;
        ConfigureObjectTypeFilterVisible = configureObjectTypeFilterVisible;
        ClearObjectTypeFilterVisible = clearObjectTypeFilterVisible;
        GenerationActionsSeparatorVisible = generationActionsSeparatorVisible;
        GenerateAllClassesVisible = generateAllClassesVisible;
        GenerateByTypeSeparatorVisible = generateByTypeSeparatorVisible;
        GenerateTestClassesVisible = generateTestClassesVisible;
        GenerateModelClassesVisible = generateModelClassesVisible;
        GenerateRepositoryClassesVisible = generateRepositoryClassesVisible;
        CheckConsistencyVisible = checkConsistencyVisible;
        ExtractScenarioVisible = extractScenarioVisible;
    }

    /// <summary>
    /// EN: Gets whether the edit connection action is visible.
    /// PT-br: Obtem se a acao de editar conexao esta visivel.
    /// </summary>
    public bool EditConnectionVisible { get; }

    /// <summary>
    /// EN: Gets whether the remove connection action is visible.
    /// PT-br: Obtem se a acao de remover conexao esta visivel.
    /// </summary>
    public bool RemoveConnectionVisible { get; }

    /// <summary>
    /// EN: Gets whether the refresh connection action is visible.
    /// PT-br: Obtem se a acao de atualizar conexao esta visivel.
    /// </summary>
    public bool RefreshConnectionVisible { get; }

    /// <summary>
    /// EN: Gets whether the cancel connection operation action is visible.
    /// PT-br: Obtem se a acao de cancelar operacao da conexao esta visivel.
    /// </summary>
    public bool CancelConnectionOperationVisible { get; }

    /// <summary>
    /// EN: Gets whether the connection actions separator is visible.
    /// PT-br: Obtem se o separador das acoes de conexao esta visivel.
    /// </summary>
    public bool ConnectionActionsSeparatorVisible { get; }

    /// <summary>
    /// EN: Gets whether the configure mappings action is visible.
    /// PT-br: Obtem se a acao de configurar mapeamentos esta visivel.
    /// </summary>
    public bool ConfigureMappingsVisible { get; }

    /// <summary>
    /// EN: Gets whether the configure templates action is visible.
    /// PT-br: Obtem se a acao de configurar templates esta visivel.
    /// </summary>
    public bool ConfigureTemplatesVisible { get; }

    /// <summary>
    /// EN: Gets whether the configure object type filter action is visible.
    /// PT-br: Obtem se a acao de configurar filtro por tipo de objeto esta visivel.
    /// </summary>
    public bool ConfigureObjectTypeFilterVisible { get; }

    /// <summary>
    /// EN: Gets whether the clear object type filter action is visible.
    /// PT-br: Obtem se a acao de limpar o filtro por tipo de objeto esta visivel.
    /// </summary>
    public bool ClearObjectTypeFilterVisible { get; }

    /// <summary>
    /// EN: Gets whether the generation actions separator is visible.
    /// PT-br: Obtem se o separador das acoes de geracao esta visivel.
    /// </summary>
    public bool GenerationActionsSeparatorVisible { get; }

    /// <summary>
    /// EN: Gets whether the generate all classes action is visible.
    /// PT-br: Obtem se a acao de gerar todas as classes esta visivel.
    /// </summary>
    public bool GenerateAllClassesVisible { get; }

    /// <summary>
    /// EN: Gets whether the generation by type separator is visible.
    /// PT-br: Obtem se o separador de geracao por tipo esta visivel.
    /// </summary>
    public bool GenerateByTypeSeparatorVisible { get; }

    /// <summary>
    /// EN: Gets whether the generate test classes action is visible.
    /// PT-br: Obtem se a acao de gerar classes de teste esta visivel.
    /// </summary>
    public bool GenerateTestClassesVisible { get; }

    /// <summary>
    /// EN: Gets whether the generate model classes action is visible.
    /// PT-br: Obtem se a acao de gerar classes de modelo esta visivel.
    /// </summary>
    public bool GenerateModelClassesVisible { get; }

    /// <summary>
    /// EN: Gets whether the generate repository classes action is visible.
    /// PT-br: Obtem se a acao de gerar classes de repository esta visivel.
    /// </summary>
    public bool GenerateRepositoryClassesVisible { get; }

    /// <summary>
    /// EN: Gets whether the check consistency action is visible.
    /// PT-br: Obtem se a acao de verificar consistencia esta visivel.
    /// </summary>
    public bool CheckConsistencyVisible { get; }

    /// <summary>
    /// EN: Gets whether the extract scenario action is visible.
    /// PT-br: Obtem se a acao de extrair scenario esta visivel.
    /// </summary>
    public bool ExtractScenarioVisible { get; }

    /// <summary>
    /// EN: Gets whether any context menu action remains visible.
    /// PT-br: Obtem se alguma acao do menu de contexto permanece visivel.
    /// </summary>
    public bool HasVisibleAction =>
        EditConnectionVisible ||
        RemoveConnectionVisible ||
        RefreshConnectionVisible ||
        CancelConnectionOperationVisible ||
        ConnectionActionsSeparatorVisible ||
        ConfigureMappingsVisible ||
        ConfigureTemplatesVisible ||
        ConfigureObjectTypeFilterVisible ||
        ClearObjectTypeFilterVisible ||
        GenerationActionsSeparatorVisible ||
        GenerateAllClassesVisible ||
        GenerateByTypeSeparatorVisible ||
        GenerateTestClassesVisible ||
        GenerateModelClassesVisible ||
        GenerateRepositoryClassesVisible ||
        CheckConsistencyVisible ||
        ExtractScenarioVisible;
}

/// <summary>
/// EN: Calculates explorer context menu visibility from the current selection state.
/// PT-br: Calcula a visibilidade do menu de contexto do explorador a partir do estado atual da selecao.
/// </summary>
public static class ExplorerContextMenuStateCalculator
{
    /// <summary>
    /// EN: Builds the visibility state for the explorer context menu.
    /// PT-br: Monta o estado de visibilidade para o menu de contexto do explorador.
    /// </summary>
    public static ExplorerContextMenuState Calculate(ExplorerContextMenuSelection selection)
    {
        if (selection is null)
        {
            throw new ArgumentNullException(nameof(selection));
        }

        var showConnectionActions = selection.IsConnectionNodeSelected || selection.IsSchemaNodeSelected;

        return new ExplorerContextMenuState(
            selection.IsConnectionNodeSelected,
            selection.IsConnectionNodeSelected,
            showConnectionActions,
            showConnectionActions,
            showConnectionActions,
            selection.IsObjectTypeNodeSelected,
            selection.IsObjectTypeNodeSelected,
            selection.IsObjectTypeNodeSelected,
            selection.CanClearObjectTypeFilter,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.IsGenerationSupportedSelected,
            selection.CanExtractScenario);
    }
}
