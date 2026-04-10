using DbSqlLikeMem.VisualStudioExtension.Core.Models;
namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Writes generated class files for the selected database objects.
/// PT: Grava arquivos de classe gerados para os objetos de banco selecionados.
/// </summary>
public sealed class ClassGenerator
{
    /// <summary>
    /// EN: Generates the files for the current request and returns the written paths.
    /// PT: Gera os arquivos para a requisicao atual e retorna os caminhos gravados.
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
            var fileName = ResolveFileName(mapping.FileNamePattern, request.Connection, dbObject, mapping.Namespace);
            var fullPath = Path.Combine(mapping.OutputDirectory, fileName);
            var content = classContentFactory(dbObject);

#pragma warning disable AsyncFixer02 // Blocking call inside an async method
            File.WriteAllText(fullPath, content);
#pragma warning restore AsyncFixer02 // Blocking call inside an async method
            writtenFiles.Add(fullPath);
        }

        return writtenFiles;
    }

    private static string ResolveFileName(
        string fileNamePattern,
        ConnectionDefinition connection,
        DatabaseObjectReference dbObject,
        string? @namespace = null)
    {
        var safePattern = string.IsNullOrWhiteSpace(fileNamePattern)
            ? "{NamePascal}{Type}Factory.cs"
            : fileNamePattern;

        var namePascal = GenerationRuleSet.ToPascalCase(dbObject.Name);
        var typeName = dbObject.Type.ToString();

        var resolved = ReplaceIgnoreCase(safePattern, "{NamePascal}", namePascal);
        resolved = ReplaceIgnoreCase(resolved, "{Name}", dbObject.Name);
        resolved = ReplaceIgnoreCase(resolved, "{Type}", typeName);
        resolved = ReplaceIgnoreCase(resolved, "{Schema}", dbObject.Schema);
        resolved = ReplaceIgnoreCase(resolved, "{DatabaseType}", connection.DatabaseType);
        resolved = ReplaceIgnoreCase(resolved, "{DatabaseName}", connection.DatabaseName);
        return ReplaceIgnoreCase(resolved, "{Namespace}", @namespace ?? string.Empty);
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
