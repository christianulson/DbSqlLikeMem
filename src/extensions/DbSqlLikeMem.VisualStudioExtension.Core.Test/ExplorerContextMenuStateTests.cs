namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies explorer context menu visibility rules in the Visual Studio extension core.
/// PT: Verifica as regras de visibilidade do menu de contexto do explorador no core da extensao do Visual Studio.
/// </summary>
public sealed class ExplorerContextMenuStateTests
{
    /// <summary>
    /// EN: Verifies connection and schema selections expose connection actions and scenario extraction.
    /// PT: Verifica se selecoes de conexao e schema exibem as acoes de conexao e a extracao de scenario.
    /// </summary>
    [Theory]
    [Trait("Category", "ExplorerContextMenuState")]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void Calculate_ConnectionOrSchemaSelection_ShowsConnectionActions(bool isConnectionNodeSelected, bool isSchemaNodeSelected)
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: isConnectionNodeSelected,
            isSchemaNodeSelected: isSchemaNodeSelected,
            isObjectTypeNodeSelected: false,
            isTableNodeSelected: false,
            hasObjectTypeFilter: false,
            isGenerationSupportedSelected: false));

        Assert.Equal(isConnectionNodeSelected, state.EditConnectionVisible);
        Assert.Equal(isConnectionNodeSelected, state.RemoveConnectionVisible);
        Assert.Equal(isConnectionNodeSelected || isSchemaNodeSelected, state.RefreshConnectionVisible);
        Assert.Equal(isConnectionNodeSelected || isSchemaNodeSelected, state.CancelConnectionOperationVisible);
        Assert.Equal(isConnectionNodeSelected || isSchemaNodeSelected, state.ConnectionActionsSeparatorVisible);
        Assert.True(state.ExtractScenarioVisible);
        Assert.False(state.ConfigureMappingsVisible);
        Assert.False(state.ConfigureTemplatesVisible);
        Assert.False(state.GenerateAllClassesVisible);
        Assert.True(state.HasVisibleAction);
    }

    /// <summary>
    /// EN: Verifies object type selections expose mapping and filter actions, including filter clearing.
    /// PT: Verifica se selecoes de tipo de objeto exibem as acoes de mapeamento e filtro, inclusive a limpeza do filtro.
    /// </summary>
    [Fact]
    [Trait("Category", "ExplorerContextMenuState")]
    public void Calculate_ObjectTypeSelection_WithFilter_ShowsConfigurationActions()
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: false,
            isSchemaNodeSelected: false,
            isObjectTypeNodeSelected: true,
            isTableNodeSelected: false,
            hasObjectTypeFilter: true,
            isGenerationSupportedSelected: false));

        Assert.True(state.ConfigureMappingsVisible);
        Assert.True(state.ConfigureTemplatesVisible);
        Assert.True(state.ConfigureObjectTypeFilterVisible);
        Assert.True(state.ClearObjectTypeFilterVisible);
        Assert.True(state.ExtractScenarioVisible);
        Assert.False(state.EditConnectionVisible);
        Assert.False(state.GenerateAllClassesVisible);
        Assert.True(state.HasVisibleAction);
    }

    /// <summary>
    /// EN: Verifies object type selections without a filter keep the filter action visible and the clear action hidden.
    /// PT: Verifica se selecoes de tipo de objeto sem filtro mantem a acao de filtro visivel e ocultam a acao de limpar.
    /// </summary>
    [Fact]
    [Trait("Category", "ExplorerContextMenuState")]
    public void Calculate_ObjectTypeSelection_WithoutFilter_HidesClearFilterAction()
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: false,
            isSchemaNodeSelected: false,
            isObjectTypeNodeSelected: true,
            isTableNodeSelected: false,
            hasObjectTypeFilter: false,
            isGenerationSupportedSelected: false));

        Assert.True(state.ConfigureObjectTypeFilterVisible);
        Assert.False(state.ClearObjectTypeFilterVisible);
        Assert.True(state.ExtractScenarioVisible);
        Assert.True(state.HasVisibleAction);
    }

    /// <summary>
    /// EN: Verifies table selections expose generation actions and scenario extraction.
    /// PT: Verifica se selecoes de tabela exibem as acoes de geracao e a extracao de scenario.
    /// </summary>
    [Fact]
    [Trait("Category", "ExplorerContextMenuState")]
    public void Calculate_TableSelection_ShowsGenerationActions()
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: false,
            isSchemaNodeSelected: false,
            isObjectTypeNodeSelected: false,
            isTableNodeSelected: true,
            hasObjectTypeFilter: false,
            isGenerationSupportedSelected: true));

        Assert.True(state.GenerationActionsSeparatorVisible);
        Assert.True(state.GenerateAllClassesVisible);
        Assert.True(state.GenerateByTypeSeparatorVisible);
        Assert.True(state.GenerateTestClassesVisible);
        Assert.True(state.GenerateModelClassesVisible);
        Assert.True(state.GenerateRepositoryClassesVisible);
        Assert.True(state.CheckConsistencyVisible);
        Assert.True(state.ExtractScenarioVisible);
        Assert.False(state.ConfigureMappingsVisible);
        Assert.False(state.RefreshConnectionVisible);
        Assert.True(state.HasVisibleAction);
    }

    /// <summary>
    /// EN: Verifies function object selections expose generation actions without table-specific scenario actions.
    /// PT: Verifica se selecoes de objeto function exibem acoes de geracao sem as acoes de scenario especificas de tabela.
    /// </summary>
    [Fact]
    [Trait("Category", "ExplorerContextMenuState")]
    public void Calculate_FunctionObjectSelection_ShowsGenerationActions()
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: false,
            isSchemaNodeSelected: false,
            isObjectTypeNodeSelected: false,
            isTableNodeSelected: false,
            hasObjectTypeFilter: false,
            isGenerationSupportedSelected: true));

        Assert.True(state.GenerationActionsSeparatorVisible);
        Assert.True(state.GenerateAllClassesVisible);
        Assert.True(state.GenerateByTypeSeparatorVisible);
        Assert.True(state.GenerateTestClassesVisible);
        Assert.True(state.GenerateModelClassesVisible);
        Assert.True(state.GenerateRepositoryClassesVisible);
        Assert.True(state.CheckConsistencyVisible);
        Assert.False(state.ExtractScenarioVisible);
        Assert.False(state.ConfigureMappingsVisible);
        Assert.False(state.ConfigureTemplatesVisible);
        Assert.True(state.HasVisibleAction);
    }

    /// <summary>
    /// EN: Verifies empty selections hide every context menu action.
    /// PT: Verifica se selecoes vazias ocultam todas as acoes do menu de contexto.
    /// </summary>
    [Fact]
    [Trait("Category", "ExplorerContextMenuState")]
    public void Calculate_EmptySelection_HidesAllActions()
    {
        var state = ExplorerContextMenuStateCalculator.Calculate(new ExplorerContextMenuSelection(
            isConnectionNodeSelected: false,
            isSchemaNodeSelected: false,
            isObjectTypeNodeSelected: false,
            isTableNodeSelected: false,
            hasObjectTypeFilter: false,
            isGenerationSupportedSelected: false));

        Assert.False(state.HasVisibleAction);
        Assert.DoesNotContain(true, GetVisibilityFlags(state));
    }

    private static IReadOnlyCollection<bool> GetVisibilityFlags(ExplorerContextMenuState state)
        => [
            state.EditConnectionVisible,
            state.RemoveConnectionVisible,
            state.RefreshConnectionVisible,
            state.CancelConnectionOperationVisible,
            state.ConnectionActionsSeparatorVisible,
            state.ConfigureMappingsVisible,
            state.ConfigureTemplatesVisible,
            state.ConfigureObjectTypeFilterVisible,
            state.ClearObjectTypeFilterVisible,
            state.GenerationActionsSeparatorVisible,
            state.GenerateAllClassesVisible,
            state.GenerateByTypeSeparatorVisible,
            state.GenerateTestClassesVisible,
            state.GenerateModelClassesVisible,
            state.GenerateRepositoryClassesVisible,
            state.CheckConsistencyVisible,
            state.ExtractScenarioVisible
        ];
}
