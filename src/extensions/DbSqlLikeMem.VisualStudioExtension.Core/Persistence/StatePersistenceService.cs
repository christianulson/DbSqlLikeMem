using System.Text.Json;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Persistence;

/// <summary>
/// EN: Saves and loads extension state from the local filesystem.
/// PT: Salva e carrega o estado da extensao a partir do sistema de arquivos local.
/// </summary>
public sealed class StatePersistenceService
{
    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        WriteIndented = true,
        PropertyNameCaseInsensitive = true
    };

    /// <summary>
    /// EN: Returns the default file path used to store extension state.
    /// PT: Retorna o caminho padrao usado para armazenar o estado da extensao.
    /// </summary>
    public string GetDefaultStatePath(string? baseDirectory = null)
    {
        var root = baseDirectory ?? Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return Path.Combine(root, "DbSqlLikeMem", "visual-studio-extension-state.json");
    }

    /// <summary>
    /// EN: Saves the supplied state asynchronously to the requested file path.
    /// PT: Salva o estado informado de forma assincrona no caminho de arquivo solicitado.
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
    /// EN: Loads extension state asynchronously when the file exists.
    /// PT: Carrega o estado da extensao de forma assincrona quando o arquivo existe.
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
    /// EN: Saves the supplied state synchronously to the requested file path.
    /// PT: Salva o estado informado de forma sincronizada no caminho de arquivo solicitado.
    /// </summary>
    public Task ExportAsync(ExtensionState state, string filePath, CancellationToken cancellationToken = default)
        => SaveAsync(state, filePath, cancellationToken);

    /// <summary>
    /// EN: Loads extension state synchronously when the file exists.
    /// PT: Carrega o estado da extensao de forma sincronizada quando o arquivo existe.
    /// </summary>
    public Task<ExtensionState?> ImportAsync(string filePath, CancellationToken cancellationToken = default)
        => LoadAsync(filePath, cancellationToken);
}
