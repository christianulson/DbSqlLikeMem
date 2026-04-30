using System.Diagnostics;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Redirects Console output to xUnit test output.
/// PT: Redireciona a saída do Console para o output do xUnit.
/// </summary>
public class ConsoleTestWriter(
    ITestOutputHelper helper
    ) : StringWriter
{
    private readonly ITestOutputHelper? _helper = helper;

    /// <summary>
    /// EN: Writes a line to xUnit output when the helper is available and falls back to Debug output otherwise.
    /// PT: Escreve uma linha no output do xUnit quando o helper esta disponivel e usa Debug como fallback.
    /// </summary>
    public override void WriteLine(string? value)
    {
        try
        {
            if (_helper is null)
            {
                Debug.WriteLine(value);
                return;
            }

            _helper.WriteLine(value ?? string.Empty);
        }
        catch (Exception ex) when (ex is InvalidOperationException || ex is ObjectDisposedException || ex is NullReferenceException)
        {
            Debug.WriteLine(ex.ToString());
            Debug.WriteLine(value);
        }
    }
}
