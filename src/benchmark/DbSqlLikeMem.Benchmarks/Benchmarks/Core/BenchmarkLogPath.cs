namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Resolves the benchmark log directory and safe file names used by benchmark sessions.
/// PT-br: Resolve a pasta de logs do benchmark e nomes de arquivo seguros usados pelas sessoes de benchmark.
/// </summary>
public static class BenchmarkLogPath
{
    private static readonly string BenchmarkProjectRelativePath = Path.Combine("src", "benchmark", "DbSqlLikeMem.Benchmarks", "DbSqlLikeMem.Benchmarks.csproj");

    /// <summary>
    /// EN: Returns the benchmark log directory inside the benchmark project folder.
    /// PT-br: Retorna a pasta de logs do benchmark dentro da pasta do projeto de benchmark.
    /// </summary>
    /// <returns>EN: The absolute benchmark log directory. PT-br: A pasta absoluta de logs do benchmark.</returns>
    public static string GetDirectory()
    {
        var projectDirectory = FindProjectDirectory();
        return Path.Combine(projectDirectory, "Logs", BenchmarkRunContext.RunId);
    }

    /// <summary>
    /// EN: Builds the absolute path for a benchmark log file using a safe file name.
    /// PT-br: Monta o caminho absoluto de um arquivo de log do benchmark usando um nome de arquivo seguro.
    /// </summary>
    /// <param name="fileName">EN: The raw file name to sanitize. PT-br: O nome bruto do arquivo a ser sanitizado.</param>
    /// <returns>EN: The absolute benchmark log file path. PT-br: O caminho absoluto do arquivo de log do benchmark.</returns>
    internal static string GetFilePath(string fileName)
        => Path.Combine(GetDirectory(), GetSafeFileName(fileName));

    /// <summary>
    /// EN: Sanitizes a file name so it can be used safely on Windows file systems.
    /// PT-br: Sanitiza um nome de arquivo para que possa ser usado com seguranca em sistemas de arquivos Windows.
    /// </summary>
    /// <param name="fileName">EN: The raw file name to sanitize. PT-br: O nome bruto do arquivo a ser sanitizado.</param>
    /// <returns>EN: The sanitized file name. PT-br: O nome de arquivo sanitizado.</returns>
    internal static string GetSafeFileName(string fileName)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = new char[fileName.Length];

        for (var i = 0; i < fileName.Length; i++)
        {
            sanitized[i] = Array.IndexOf(invalidChars, fileName[i]) >= 0 ? '_' : fileName[i];
        }

        return new string(sanitized).Trim();
    }

    private static string FindProjectDirectory()
    {
        var projectDirectory = TryFindProjectDirectory(Directory.GetCurrentDirectory());
        if (projectDirectory is not null)
        {
            return projectDirectory;
        }

        projectDirectory = TryFindProjectDirectory(AppContext.BaseDirectory);
        if (projectDirectory is not null)
        {
            return projectDirectory;
        }

        return Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", ".."));
    }

    private static string? TryFindProjectDirectory(string startDirectory)
    {
        var current = new DirectoryInfo(startDirectory);

        while (current is not null)
        {
            var directProjectFile = Path.Combine(current.FullName, "DbSqlLikeMem.Benchmarks.csproj");
            if (File.Exists(directProjectFile))
            {
                return current.FullName;
            }

            var nestedProjectFile = Path.Combine(current.FullName, BenchmarkProjectRelativePath);
            if (File.Exists(nestedProjectFile))
            {
                return Path.GetDirectoryName(nestedProjectFile);
            }

            current = current.Parent;
        }

        return null;
    }
}
