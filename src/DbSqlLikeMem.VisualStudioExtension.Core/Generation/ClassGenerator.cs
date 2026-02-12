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

#pragma warning disable AsyncFixer02 // Blocking call inside an async method
            File.WriteAllText(fullPath, content);
#pragma warning restore AsyncFixer02 // Blocking call inside an async method
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

        return ReplaceIgnoreCase(
            ReplaceIgnoreCase(
                ReplaceIgnoreCase(
                    ReplaceIgnoreCase(
                        ReplaceIgnoreCase(
                            ReplaceIgnoreCase(safePattern, "{NamePascal}", namePascal),
                            "{Name}",
                            dbObject.Name),
                        "{Type}",
                        typeName),
                    "{Schema}",
                    dbObject.Schema),
                "{DatabaseType}",
                connection.DatabaseType),
            "{DatabaseName}",
            connection.DatabaseName);
    }

    private static string ReplaceIgnoreCase(string value, string oldValue, string newValue)
    {
        var current = value;
        var index = current.IndexOf(oldValue, StringComparison.OrdinalIgnoreCase);

        while (index >= 0)
        {
            current = current.Remove(index, oldValue.Length).Insert(index, newValue);
            index = current.IndexOf(oldValue, index + newValue.Length, StringComparison.OrdinalIgnoreCase);
        }

        return current;
    }

}
