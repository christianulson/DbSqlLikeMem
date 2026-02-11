using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public sealed class ClassGenerationPlanner
{
    public GenerationPlan BuildPlan(GenerationRequest request, ConnectionMappingConfiguration? configuration)
    {
        var selectedTypes = request.SelectedObjects.Select(o => o.Type).Distinct().ToArray();

        if (configuration is null)
        {
            return new GenerationPlan(true, selectedTypes, request.SelectedObjects);
        }

        var missingMappings = selectedTypes
            .Where(type => !configuration.HasMappingFor(type))
            .Distinct()
            .ToArray();

        return new GenerationPlan(
            missingMappings.Length > 0,
            missingMappings,
            request.SelectedObjects);
    }
}
