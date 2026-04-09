using System.IO;

namespace DbSqlLikeMem.VisualStudioExtension.Services;

internal static class ExtensionLogger
{
    public static void Log(string message)
    {
        try
        {
            var root = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), "DbSqlLikeMem");
            Directory.CreateDirectory(root);
            var file = Path.Combine(root, "visual-studio-extension.log");
            var line = $"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss zzz}] {message}{Environment.NewLine}";
            File.AppendAllText(file, line);
        }
        catch
        {
            // Ignora falhas de log para não impactar UX.
        }
    }
}
