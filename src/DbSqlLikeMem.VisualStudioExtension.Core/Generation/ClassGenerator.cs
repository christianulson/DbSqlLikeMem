using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

public sealed class ClassGenerator
{
    public async Task<IReadOnlyCollection<string>> GenerateAsync(
        GenerationRequest request,
        ConnectionMappingConfiguration configuration,
        Func<DatabaseObjectReference, string> classContentFactory,
        CancellationToken cancellationToken = default)
    {
        var writtenFiles = new List<string>();

        foreach (var dbObject in request.SelectedObjects)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (!configuration.Mappings.TryGetValue(dbObject.Type, out var mapping))
            {
                continue;
            }

            Directory.CreateDirectory(mapping.OutputDirectory);
            var fileName = mapping.FileNamePattern.Replace("{Name}", dbObject.Name, StringComparison.OrdinalIgnoreCase);
            var fullPath = Path.Combine(mapping.OutputDirectory, fileName);
            var content = classContentFactory(dbObject);

            await File.WriteAllTextAsync(fullPath, content, cancellationToken);
            writtenFiles.Add(fullPath);
        }

        return writtenFiles;
    }
}
