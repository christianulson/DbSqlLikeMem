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
    /// EN: Verifies normalized configurations keep procedure and function requests ready for generation.
    /// PT: Verifica se configuracoes normalizadas mantem requisicoes de procedure e function prontas para geracao.
    /// </summary>
    [Fact]
    [Trait("Category", "ClassGenerationPlanner")]
    public void BuildPlan_WithNormalizedConfiguration_DoesNotReportRoutineMissingTypes()
    {
        var planner = new ClassGenerationPlanner();
        var request = new GenerationRequest(
            new ConnectionDefinition("1", "SqlServer", "ERP", "Server=.;Database=ERP"),
            [
                new DatabaseObjectReference("dbo", "sp_update_customer", DatabaseObjectType.Procedure),
                new DatabaseObjectReference("dbo", "fn_total", DatabaseObjectType.Function)
            ]);

        var config = new ConnectionMappingConfiguration("1", new Dictionary<DatabaseObjectType, ObjectTypeMapping>
        {
            [DatabaseObjectType.Table] = new(DatabaseObjectType.Table, "c:/temp")
        });

        var plan = planner.BuildPlan(request, config);

        Assert.False(plan.RequiresConfiguration);
        Assert.Empty(plan.MissingMappings);
        Assert.Equal(2, plan.ObjectsToGenerate.Count);
        Assert.Contains(DatabaseObjectType.Procedure, config.Mappings.Keys);
        Assert.Contains(DatabaseObjectType.Function, config.Mappings.Keys);
    }
}
