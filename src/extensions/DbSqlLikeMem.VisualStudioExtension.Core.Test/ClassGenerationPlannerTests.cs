namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// EN: Verifies the generation planner identifies missing mappings correctly.
/// PT: Verifica se o planejador de geracao identifica corretamente mapeamentos ausentes.
/// </summary>
public class ClassGenerationPlannerTests
{
    /// <summary>
    /// EN: Verifies a request without configuration requires mapping setup.
    /// PT: Verifica se uma requisicao sem configuracao exige preparo de mapeamento.
    /// </summary>
    [Fact]
    [Trait("Category", "ClassGenerationPlanner")]
    public void BuildPlan_WithoutConfiguration_RequiresConfiguration()
    {
        var planner = new ClassGenerationPlanner();
        var request = new GenerationRequest(
            new ConnectionDefinition("1", "SqlServer", "ERP", "Server=.;Database=ERP"),
            [new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table)]);

        var plan = planner.BuildPlan(request, null);

        Assert.True(plan.RequiresConfiguration);
        Assert.Single(plan.MissingMappings);
        Assert.Contains(DatabaseObjectType.Table, plan.MissingMappings);
    }

    /// <summary>
    /// EN: Verifies partially mapped requests report the remaining missing types.
    /// PT: Verifica se requisicoes com mapeamento parcial relatam os tipos ausentes restantes.
    /// </summary>
    [Fact]
    [Trait("Category", "ClassGenerationPlanner")]
    public void BuildPlan_WithPartialMappings_ReturnsMissingTypes()
    {
        var planner = new ClassGenerationPlanner();
        var request = new GenerationRequest(
            new ConnectionDefinition("1", "SqlServer", "ERP", "Server=.;Database=ERP"),
            [
                new DatabaseObjectReference("dbo", "Orders", DatabaseObjectType.Table),
                new DatabaseObjectReference("dbo", "vw_Orders", DatabaseObjectType.View)
            ]);

        var config = new ConnectionMappingConfiguration("1", new Dictionary<DatabaseObjectType, ObjectTypeMapping>
        {
            [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, "c:/temp")
        });

        var plan = planner.BuildPlan(request, config);

        Assert.True(plan.RequiresConfiguration);
        Assert.Single(plan.MissingMappings);
        Assert.Contains(DatabaseObjectType.View, plan.MissingMappings);
    }
}
