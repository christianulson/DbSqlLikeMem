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
        if (string.IsNullOrWhiteSpace(repositoryRoot))
        {
            return null;
        }

        var metadataPath = Path.Combine(repositoryRoot, "templates", "dbsqllikemem", "review-metadata.json");
        if (!File.Exists(metadataPath))
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(File.ReadAllText(metadataPath));
            var root = document.RootElement;

            var profileFocus = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            if (root.TryGetProperty("profiles", out var profilesElement)
                && profilesElement.ValueKind == JsonValueKind.Object)
            {
                foreach (var profile in profilesElement.EnumerateObject())
                {
                    if (profile.Value.ValueKind == JsonValueKind.Object
                        && profile.Value.TryGetProperty("focus", out var focusElement))
                    {
                        var focus = focusElement.GetString();
                        if (!string.IsNullOrWhiteSpace(focus))
                        {
                            profileFocus[profile.Name] = focus!.Trim();
                        }
                    }
                }
            }

            var evidenceFiles = new List<string>();
            if (root.TryGetProperty("evidenceFiles", out var evidenceFilesElement)
                && evidenceFilesElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var entry in evidenceFilesElement.EnumerateArray())
                {
                    var value = entry.GetString();
                    if (!string.IsNullOrWhiteSpace(value))
                    {
                        evidenceFiles.Add(value!.Trim());
                    }
                }
            }

            return new TemplateReviewMetadata(
                root.TryGetProperty("currentBaseline", out var currentBaselineElement) ? currentBaselineElement.GetString()?.Trim() ?? string.Empty : string.Empty,
                root.TryGetProperty("promotionStagingPath", out var promotionStagingPathElement) ? promotionStagingPathElement.GetString()?.Trim() ?? string.Empty : string.Empty,
                root.TryGetProperty("reviewCadence", out var reviewCadenceElement) ? reviewCadenceElement.GetString()?.Trim() ?? string.Empty : string.Empty,
                root.TryGetProperty("lastReviewedOn", out var lastReviewedOnElement) ? lastReviewedOnElement.GetString()?.Trim() ?? string.Empty : string.Empty,
                root.TryGetProperty("nextPlannedReviewOn", out var nextPlannedReviewOnElement) ? nextPlannedReviewOnElement.GetString()?.Trim() ?? string.Empty : string.Empty,
                profileFocus,
                evidenceFiles);
        }
        catch
        {
            return null;
        }
    }
}
