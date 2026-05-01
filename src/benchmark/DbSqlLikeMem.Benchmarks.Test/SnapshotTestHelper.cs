namespace DbSqlLikeMem.Benchmarks.Test;

internal static class SnapshotTestHelper
{
    internal static void AssertFileMatchesSnapshot(string actualRelativePath, string snapshotRelativePath)
    {
        var actual = NormalizeLineEndings(ReadRepoFile(actualRelativePath));
        var expected = NormalizeLineEndings(ReadRepoFile(snapshotRelativePath));

        Assert.Equal(expected, actual);
    }

    internal static string ReadRepoFile(string relativePath)
    {
        var repoRoot = FindRepoRoot();
        var filePath = Path.Combine(repoRoot, relativePath.Replace('/', Path.DirectorySeparatorChar));
        return File.ReadAllText(filePath);
    }

    internal static string NormalizeLineEndings(string value)
        => value.Replace("\r\n", "\n").Replace("\r", "\n");

    private static string FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "AGENTS.md")))
                return directory.FullName;

            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Unable to locate the repository root for the benchmark snapshot tests.");
    }
}
