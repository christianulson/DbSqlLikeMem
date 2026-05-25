using System.Diagnostics;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides shared helpers for XUnit-based test classes.
/// PT-br: Fornece helpers compartilhados para classes de teste baseadas em XUnit.
/// </summary>
public abstract class XUnitTestBase : IDisposable
{
    /// <summary>
    /// EN: Test output writer used to redirect console output.
    /// PT-br: Escritor de saída de teste usado para redirecionar a saída do console.
    /// </summary>
    protected ConsoleTestWriter ConsoleWriter { get; }
    private bool disposedValue;

    /// <summary>
    /// EN: Creates the base test with the output helper.
    /// PT-br: Cria o teste base com o helper de saida.
    /// </summary>
    /// <param name="helper">EN: Output helper. PT-br: Helper de saída.</param>
    protected XUnitTestBase(
        ITestOutputHelper helper)
    {
        ConsoleWriter = new ConsoleTestWriter(helper);
        Console.SetOut(ConsoleWriter);
    }

    /// <summary>
    /// EN: Copies writable public instance properties from one object to another.
    /// PT-br: Copia propriedades públicas graváveis de instância de um objeto para outro.
    /// </summary>
    /// <param name="src">EN: Source instance. PT-br: Instância de origem.</param>
    /// <param name="dst">EN: Destination instance. PT-br: Instância de destino.</param>
    /// <typeparam name="T">EN: Instance type. PT-br: Tipo da instância.</typeparam>
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
    /// PT-br: ResolveRowsFrameRange o nome do método de teste atual.
    /// </summary>
    /// <returns>EN: Test name. PT-br: Nome do teste.</returns>
    protected static string GetTestName()
    {
        var stack = new StackTrace();
        var testMethod = stack.GetFrames()
            ?.Select(f => f.GetMethod())
            .FirstOrDefault(m => m?.GetCustomAttributes(typeof(FactAttribute), true).Length != 0);
        return testMethod?.Name ?? "UnknownTest";
    }

    /// <summary>
    /// EN: Creates a dialect or helper instance for the requested version.
    /// PT-br: Cria um dialeto ou helper para a versão solicitada.
    /// </summary>
    /// <typeparam name="T">EN: Instance type. PT-br: Tipo da instância.</typeparam>
    /// <param name="version">EN: Version number used to build the instance. PT-br: Número da versão usado para construir a instância.</param>
    /// <param name="fnCreate">EN: Factory that creates the instance for the requested version. PT-br: Factory que cria a instância para a versão solicitada.</param>
    /// <returns>EN: Created instance of type T. PT-br: Instância criada do tipo T.</returns>
    protected static T Get<T>(int version, Func<int, T> fnCreate)
        => fnCreate(version)!;

    #region Dispose

    /// <summary>
    /// EN: Disposes managed resources when requested.
    /// PT-br: Descarta recursos gerenciados quando solicitado.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected virtual void Dispose(bool disposing)
    {
        ConsoleWriter?.Flush();
        if (!disposedValue)
        {
            if (disposing)
            {
                ConsoleWriter?.Dispose();
            }

            disposedValue = true;
        }
    }

    /// <summary>
    /// Finalizes the instance and releases unmanaged resources before the object is reclaimed by garbage collection.
    /// </summary>
    /// <remarks>This destructor is called automatically when the object is no longer referenced. Cleanup
    /// logic for unmanaged resources should be placed in the Dispose method with disposing set to false. Do not
    /// override the finalizer unless unmanaged resources need to be released.</remarks>
    ~XUnitTestBase()
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: false);
    }

    /// <summary>
    /// EN: Disposes the test base and suppresses finalization.
    /// PT-br: Descarta a base de teste e suprime a finalizacao.
    /// </summary>
    public void Dispose()
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }

    #endregion
}
