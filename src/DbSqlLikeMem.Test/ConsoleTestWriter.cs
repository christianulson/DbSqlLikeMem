using System.Diagnostics;
using Xunit.Abstractions;

namespace DbSqlLikeMem.Test;

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