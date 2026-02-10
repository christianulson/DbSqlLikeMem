using System.Diagnostics;
using Xunit.Abstractions;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Redirects Console output to xUnit test output.
/// PT: Redireciona a saída do Console para o output do xUnit.
/// </summary>
public class ConsoleTestWriter(
    ITestOutputHelper helper
    ) : StringWriter
{
    private readonly ITestOutputHelper? _helper = helper;

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
        catch (Exception ex) when (ex is InvalidOperationException || ex is ObjectDisposedException)
        {
            Debug.WriteLine(ex.ToString());
            Debug.WriteLine(value);
        }
    }
}
