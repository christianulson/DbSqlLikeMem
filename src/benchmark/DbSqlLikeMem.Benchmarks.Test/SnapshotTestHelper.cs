namespace DbSqlLikeMem.Benchmarks.Test;

internal static class SnapshotTestHelper
{
    internal static void AssertFileMatchesSnapshot(string actualRelativePath, string snapshotRelativePath)
    {
        var actual = NormalizeLineEndings(ReadRepoFile(actualRelativePath));
        var expected = NormalizeLineEndings(ReadSnapshotFixture(snapshotRelativePath));

        Assert.Equal(expected, actual);
    }

    internal static string ReadRepoFile(string relativePath)
    {
        var normalizedPath = relativePath.Replace('/', Path.DirectorySeparatorChar);
        if (normalizedPath.EndsWith(".snapshot.md", StringComparison.OrdinalIgnoreCase))
            return ReadSnapshotFixture(normalizedPath);

        var filePath = Path.Combine(FindRepoRoot(), normalizedPath);
        return File.ReadAllText(filePath);
    }

    internal static string NormalizeLineEndings(string value)
        => value.Replace("\r\n", "\n").Replace("\r", "\n");

    private static string ReadSnapshotFixture(string relativePath)
    {
        var fileName = Path.GetFileName(relativePath.Replace('/', Path.DirectorySeparatorChar));
        var fixturePath = Path.Combine(FindRepoRoot(), "src", "benchmark", "DbSqlLikeMem.Benchmarks.Test", "Fixtures", fileName);

        return File.ReadAllText(fixturePath);
    }

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
