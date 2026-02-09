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
    public override void WriteLine(string? value)
    {
        try
        {
            helper.WriteLine(value);
        }
        catch (InvalidOperationException ex)
        {
            Debug.WriteLine(ex.ToString());
            Debug.WriteLine(value);
        }
    }
}
