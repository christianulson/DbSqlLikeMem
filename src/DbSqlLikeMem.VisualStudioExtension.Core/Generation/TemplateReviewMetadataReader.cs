using System.Text.Json;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Loads the versioned review metadata that governs the shared template baseline when the repository files are available.
/// PT: Carrega os metadados versionados de revisao que governam a baseline compartilhada de templates quando os arquivos do repositorio estao disponiveis.
/// </summary>
public static class TemplateReviewMetadataReader
{
    /// <summary>
    /// EN: Tries to load review metadata from the informed repository root and returns `null` when the file is unavailable or invalid.
    /// PT: Tenta carregar os metadados de revisao a partir da raiz de repositorio informada e retorna `null` quando o arquivo estiver indisponivel ou invalido.
    /// </summary>
    /// <param name="repositoryRoot">EN: Absolute repository root path. PT: Caminho absoluto da raiz do repositorio.</param>
    public static TemplateReviewMetadata? TryLoadFromRepositoryRoot(string repositoryRoot)
    {
        if (!TryGetMetadataPath(repositoryRoot, out var metadataPath))
            return null;

        try
        {
            return LoadMetadata(metadataPath);
        }
        catch
        {
            return null;
        }
    }

    private static bool TryGetMetadataPath(string repositoryRoot, out string metadataPath)
    {
        metadataPath = string.Empty;
        if (string.IsNullOrWhiteSpace(repositoryRoot))
            return false;

        metadataPath = Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "review-metadata.json");
        return File.Exists(metadataPath);
    }

    private static TemplateReviewMetadata LoadMetadata(string metadataPath)
    {
        using var document = JsonDocument.Parse(File.ReadAllText(metadataPath));
        var root = document.RootElement;

        return new TemplateReviewMetadata(
            ReadTrimmedString(root, "currentBaseline"),
            ReadTrimmedString(root, "promotionStagingPath"),
            ReadTrimmedString(root, "reviewCadence"),
            ReadTrimmedString(root, "lastReviewedOn"),
            ReadTrimmedString(root, "nextPlannedReviewOn"),
            ReadProfileFocus(root),
            ReadEvidenceFiles(root));
    }

    private static Dictionary<string, string> ReadProfileFocus(JsonElement root)
    {
        var profileFocus = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (!TryGetObjectProperty(root, "profiles", out var profilesElement))
            return profileFocus;

        foreach (var profile in profilesElement.EnumerateObject())
        {
            if (TryReadProfileFocus(profile.Value, out var focus))
                profileFocus[profile.Name] = focus;
        }

        return profileFocus;
    }

    private static bool TryReadProfileFocus(JsonElement profileElement, out string focus)
    {
        focus = string.Empty;
        if (profileElement.ValueKind != JsonValueKind.Object
            || !profileElement.TryGetProperty("focus", out var focusElement))
        {
            return false;
        }

        var value = focusElement.GetString();
        if (string.IsNullOrWhiteSpace(value))
            return false;

        focus = value!.Trim();
        return true;
    }

    private static List<string> ReadEvidenceFiles(JsonElement root)
    {
        var evidenceFiles = new List<string>();
        if (!TryGetArrayProperty(root, "evidenceFiles", out var evidenceFilesElement))
            return evidenceFiles;

        foreach (var entry in evidenceFilesElement.EnumerateArray())
        {
            var value = entry.GetString();
            if (!string.IsNullOrWhiteSpace(value))
                evidenceFiles.Add(value!.Trim());
        }

        return evidenceFiles;
    }

    private static string ReadTrimmedString(JsonElement root, string propertyName)
        => root.TryGetProperty(propertyName, out var propertyElement)
            ? propertyElement.GetString()?.Trim() ?? string.Empty
            : string.Empty;

    private static bool TryGetObjectProperty(JsonElement root, string propertyName, out JsonElement propertyElement)
        => TryGetProperty(root, propertyName, JsonValueKind.Object, out propertyElement);

    private static bool TryGetArrayProperty(JsonElement root, string propertyName, out JsonElement propertyElement)
        => TryGetProperty(root, propertyName, JsonValueKind.Array, out propertyElement);

    private static bool TryGetProperty(
        JsonElement root,
        string propertyName,
        JsonValueKind expectedKind,
        out JsonElement propertyElement)
    {
        if (root.TryGetProperty(propertyName, out propertyElement)
            && propertyElement.ValueKind == expectedKind)
        {
            return true;
        }

        propertyElement = default;
        return false;
    }
}
