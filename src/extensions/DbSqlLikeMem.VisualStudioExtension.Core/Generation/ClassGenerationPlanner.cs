using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Computes which mappings are required before generation can run.
/// PT-br: Calcula quais mapeamentos sao necessarios antes que a geracao possa executar.
/// </summary>
public sealed class ClassGenerationPlanner
{
    /// <summary>
    /// EN: Builds a generation plan and reports missing mappings for the request.
    /// PT-br: Monta um plano de geracao e informa mapeamentos ausentes para a requisicao.
    /// </summary>
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
