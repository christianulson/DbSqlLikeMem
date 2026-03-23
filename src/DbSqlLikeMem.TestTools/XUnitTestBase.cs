using System.Diagnostics;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides shared helpers for XUnit-based test classes.
/// PT: Fornece helpers compartilhados para classes de teste baseadas em XUnit.
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
    /// EN: Gets the lazy flag that enables container-backed test execution.
    /// PT: Obtem a flag preguiçosa que habilita a execucao de testes com container.
    /// </summary>
    protected static Lazy<bool> RunContainerTests = new(() =>
        Environment.GetEnvironmentVariable("RUN_CONTAINER_TESTS") == "true");

    /// <summary>
    /// EN: Resolves a container connection string for the specified provider when one is available.
    /// PT: Resolve uma string de conexao de container para o provedor informado quando houver uma disponivel.
    /// </summary>
    /// <param name="provider">EN: The provider identifier used to select the environment variables. PT: O identificador do provedor usado para selecionar as variaveis de ambiente.</param>
    /// <param name="connectionString">EN: The resolved container connection string when available. PT: A string de conexao de container resolvida quando disponivel.</param>
    /// <returns>EN: True when the provider has a usable container connection string. PT: True quando o provedor possui uma string de conexao de container utilizavel.</returns>
    protected static bool TryResolveContainerConnectionString(
        ProviderId provider,
        out string connectionString)
        => ProviderConnectionStringResolver.TryResolve(provider, out connectionString);

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
    /// PT: Descarta a base de teste e suprime a finalizacao.
    /// </summary>
    public void Dispose()
    {
        // Não altere este código. Coloque o código de limpeza no método 'Dispose(bool disposing)'
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}
