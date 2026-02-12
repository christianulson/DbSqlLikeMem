using System.Text.Json;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class StatePersistenceService
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public string GetDefaultStatePath(string? baseDirectory = null)
    {
        var root = baseDirectory ?? Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(root, "DbSqlLikeMem", "visual-studio-extension-state.json");
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public Task SaveAsync(ExtensionState state, string outputPath, CancellationToken cancellationToken = default)
    {
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(state, SerializerOptions);
        return File.WriteAllTextAsync(outputPath, json, cancellationToken);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public async Task<ExtensionState?> LoadAsync(string inputPath, CancellationToken cancellationToken = default)
    {
        if (!File.Exists(inputPath))
        {
            return null;
        }

        var json = await File.ReadAllTextAsync(inputPath, cancellationToken);
        return JsonSerializer.Deserialize<ExtensionState>(json, SerializerOptions);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public Task ExportAsync(ExtensionState state, string filePath, CancellationToken cancellationToken = default)
        => SaveAsync(state, filePath, cancellationToken);

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public Task<ExtensionState?> ImportAsync(string filePath, CancellationToken cancellationToken = default)
        => LoadAsync(filePath, cancellationToken);
}
