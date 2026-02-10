using System.Diagnostics;

namespace DbSqlLikeMem.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public abstract class XUnitTestBase : IDisposable
{
    /// <summary>
    /// EN: Test output writer used to redirect console output.
    /// PT: Escritor de saída de teste usado para redirecionar a saída do console.
    /// </summary>
    protected ConsoleTestWriter ConsoleWriter { get; }
    private bool disposedValue;

    /// <summary>
    /// EN: Initializes the base test with the output helper.
    /// PT: Inicializa o teste base com o helper de saída.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT: Helper de saída.</param>
    protected XUnitTestBase(
        ITestOutputHelper helper)
    {
        ConsoleWriter = new ConsoleTestWriter(helper);
        Console.SetOut(ConsoleWriter);
    }

    /// <summary>
    /// EN: Copies writable public instance properties from one object to another.
    /// PT: Copia propriedades públicas graváveis de instância de um objeto para outro.
    /// </summary>
    /// <param name="src">EN: Source instance. PT: Instância de origem.</param>
    /// <param name="dst">EN: Destination instance. PT: Instância de destino.</param>
    /// <typeparam name="T">EN: Instance type. PT: Tipo da instância.</typeparam>
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

    /// <summary>
    /// EN: Resolves the current test method name.
    /// PT: Resolve o nome do método de teste atual.
    /// </summary>
    /// <returns>EN: Test name. PT: Nome do teste.</returns>
    protected static string GetTestName()
    {
        var stack = new StackTrace();
        var testMethod = stack.GetFrames()
            ?.Select(f => f.GetMethod())
            .FirstOrDefault(m => m?.GetCustomAttributes(typeof(FactAttribute), true).Length != 0);
        return testMethod?.Name ?? "UnknownTest";
    }

    /// <summary>
    /// EN: Disposes managed resources when requested.
    /// PT: Descarta recursos gerenciados quando solicitado.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
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
    /// <summary>
    /// Finalizes the instance and releases unmanaged resources before the object is reclaimed by garbage collection.
    /// </summary>
    /// <remarks>This destructor is called automatically when the object is no longer referenced. Cleanup
    /// logic for unmanaged resources should be placed in the Dispose method with disposing set to false. Do not
    /// override the finalizer unless unmanaged resources need to be released.</remarks>
    ~XUnitTestBase()
#pragma warning restore S1135 // Track uses of "TODO" tags
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: false);
    }

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void Dispose()
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
