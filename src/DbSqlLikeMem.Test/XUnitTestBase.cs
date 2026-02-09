using System.Diagnostics;
using Xunit.Abstractions;

namespace DbSqlLikeMem.Test;

public abstract class XUnitTestBase : IDisposable
{
    protected ConsoleTestWriter ConsoleWriter { get; }
    private bool disposedValue;

    protected XUnitTestBase(
        ITestOutputHelper helper)
    {
        ConsoleWriter = new ConsoleTestWriter(helper);
        Console.SetOut(ConsoleWriter);
    }

    protected static void CopyWritableProps<T>(
        T src,
        T dst)
    {
        var props = typeof(T)
            .GetProperties(System.Reflection.BindingFlags.Instance
                          | System.Reflection.BindingFlags.Public)
            .Where(p => p.CanRead
                     && p.CanWrite
                     && p.GetIndexParameters().Length == 0);

        foreach (var p in props)
            p.SetValue(dst, p.GetValue(src));
    }

    protected static string GetTestName()
    {
        var stack = new StackTrace();
        var testMethod = stack.GetFrames()
            ?.Select(f => f.GetMethod())
            .FirstOrDefault(m => m?.GetCustomAttributes(typeof(FactAttribute), true).Length != 0);
        return testMethod?.Name ?? "UnknownTest";
    }

    protected virtual void Dispose(bool disposing)
    {
        ConsoleWriter?.Flush();
        if (!disposedValue)
        {
            if (disposing)
            {
                // TODO: dispose managed state (managed objects)
                ConsoleWriter?.Dispose();
            }


#pragma warning disable S1135 // Track uses of "TODO" tags
            // TODO: free unmanaged resources (unmanaged objects) and override finalizer

#pragma warning disable S1135 // Track uses of "TODO" tags
            // TODO: set large fields to null
            disposedValue = true;
#pragma warning restore S1135 // Track uses of "TODO" tags
#pragma warning restore S1135 // Track uses of "TODO" tags
        }
    }


#pragma warning disable S1135 // Track uses of "TODO" tags
    // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
    ~XUnitTestBase()
#pragma warning restore S1135 // Track uses of "TODO" tags
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: false);
    }

    public void Dispose()
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}