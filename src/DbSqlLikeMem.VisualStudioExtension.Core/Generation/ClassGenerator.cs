using DbSqlLikeMem.VisualStudioExtension.Core.Models;
namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class ClassGenerator
{
    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
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
            var fileName = ResolveFileName(mapping.FileNamePattern, request.Connection, dbObject);
            var fullPath = Path.Combine(mapping.OutputDirectory, fileName);
            var content = classContentFactory(dbObject);

            cancellationToken.ThrowIfCancellationRequested();

            await File.WriteAllTextAsync(fullPath, content, cancellationToken);
            writtenFiles.Add(fullPath);
        }

        return writtenFiles;
    }

    private static string ResolveFileName(string fileNamePattern, ConnectionDefinition connection, DatabaseObjectReference dbObject)
    {
        var safePattern = string.IsNullOrWhiteSpace(fileNamePattern)
            ? "{NamePascal}{Type}Factory.cs"
            : fileNamePattern;

        var namePascal = GenerationRuleSet.ToPascalCase(dbObject.Name);
        var typeName = dbObject.Type.ToString();

        return safePattern
            .Replace("{NamePascal}", namePascal, StringComparison.OrdinalIgnoreCase)
            .Replace("{Name}", dbObject.Name, StringComparison.OrdinalIgnoreCase)
            .Replace("{Type}", typeName, StringComparison.OrdinalIgnoreCase)
            .Replace("{Schema}", dbObject.Schema, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseType}", connection.DatabaseType, StringComparison.OrdinalIgnoreCase)
            .Replace("{DatabaseName}", connection.DatabaseName, StringComparison.OrdinalIgnoreCase);
    }

}
