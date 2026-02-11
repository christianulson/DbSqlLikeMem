using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Test;

public class ClassGenerationPlannerTests
{
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
