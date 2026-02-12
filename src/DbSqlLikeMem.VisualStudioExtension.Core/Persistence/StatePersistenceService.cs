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
        cancellationToken.ThrowIfCancellationRequested();
        return Task.Run(() => File.WriteAllText(outputPath, json), cancellationToken);
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

        cancellationToken.ThrowIfCancellationRequested();
        var json = await Task.Run(() => File.ReadAllText(inputPath), cancellationToken);
        return JsonSerializer.Deserialize<ExtensionState>(json, SerializerOptions);
    }


    /// <summary>
    /// Persists extension state to disk synchronously.
    /// Persiste o estado da extensão em disco de forma síncrona.
    /// </summary>
    public void Save(ExtensionState state, string outputPath)
    {
        var directory = Path.GetDirectoryName(outputPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        var json = JsonSerializer.Serialize(state, SerializerOptions);
        File.WriteAllText(outputPath, json);
    }

    /// <summary>
    /// Loads extension state from disk synchronously.
    /// Carrega o estado da extensão a partir do disco de forma síncrona.
    /// </summary>
    public ExtensionState? Load(string inputPath)
    {
        if (!File.Exists(inputPath))
        {
            return null;
        }

        var json = File.ReadAllText(inputPath);
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
