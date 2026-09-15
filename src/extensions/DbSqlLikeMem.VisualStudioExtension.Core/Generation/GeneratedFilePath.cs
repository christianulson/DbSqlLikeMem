namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Validates generated file names and rejects duplicate output paths before writing.
/// PT-br: Valida nomes de arquivos gerados e rejeita caminhos de saida duplicados antes da gravacao.
/// </summary>
public static class GeneratedFilePath
{
    /// <summary>
    /// EN: Resolves a single file inside its configured output directory.
    /// PT-br: Resolve um arquivo dentro do diretorio de saida configurado.
    /// </summary>
    public static string Resolve(string outputDirectory, string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName) || fileName is "." or ".."
            || fileName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0
            || fileName.IndexOfAny(['/', '\\', ':', '<', '>', '"', '|', '?', '*']) >= 0
            || fileName.EndsWith(".", StringComparison.Ordinal) || fileName.EndsWith(" ", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Invalid generated file name: {fileName}");
        }

        return Path.GetFullPath(Path.Combine(outputDirectory, fileName));
    }

    /// <summary>
    /// EN: Rejects multiple artifacts that would overwrite the same destination.
    /// PT-br: Rejeita artefatos que sobrescreveriam o mesmo destino.
    /// </summary>
    public static void EnsureUnique(IEnumerable<string> paths)
    {
        var comparer = Path.DirectorySeparatorChar == '\\' ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
        var targets = new HashSet<string>(comparer);
        foreach (var path in paths)
        {
            if (!targets.Add(Path.GetFullPath(path)))
            {
                throw new InvalidOperationException($"Multiple objects target the same file: {path}. Include schema or object type in the file name.");
            }
        }
    }
}
