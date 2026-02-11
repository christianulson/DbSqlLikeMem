namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public class ClassGenerationPlannerTests
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
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
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    [Fact]
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
