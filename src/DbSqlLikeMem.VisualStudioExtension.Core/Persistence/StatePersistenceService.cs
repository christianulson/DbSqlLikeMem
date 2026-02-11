using System.Text.Json;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

public sealed class StatePersistenceService
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    public string GetDefaultStatePath(string? baseDirectory = null)
    {
        var root = baseDirectory ?? Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(root, "DbSqlLikeMem", "visual-studio-extension-state.json");
    }

    public async Task SaveAsync(ExtensionState state, string outputPath, CancellationToken cancellationToken = default)
    {
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(state, SerializerOptions);
        await File.WriteAllTextAsync(outputPath, json, cancellationToken);
    }

    public async Task<ExtensionState?> LoadAsync(string inputPath, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(inputPath))
        {
            return null;
        }

        var json = await File.ReadAllTextAsync(inputPath, cancellationToken);
        return JsonSerializer.Deserialize<ExtensionState>(json, SerializerOptions);
    }

    public Task ExportAsync(ExtensionState state, string filePath, CancellationToken cancellationToken = default)
        => SaveAsync(state, filePath, cancellationToken);

    public Task<ExtensionState?> ImportAsync(string filePath, CancellationToken cancellationToken = default)
        => LoadAsync(filePath, cancellationToken);
}
