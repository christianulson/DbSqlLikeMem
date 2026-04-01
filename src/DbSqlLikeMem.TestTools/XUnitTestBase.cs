using System.Collections.Concurrent;
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
    /// EN: Resolves whether a suite-specific container comparison is enabled for the current provider.
    /// PT: ResolveRowsFrameRange se uma comparacao com container especifica da suite esta habilitada para o provedor atual.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <param name="environmentVariableName">EN: The environment variable that enables the suite-specific comparison. PT: A variavel de ambiente que habilita a comparacao especifica da suite.</param>
    /// <returns>EN: True when the global flag, the suite flag, and the provider-specific flag are enabled. PT: True quando a flag global, a flag da suite e a flag especifica do provedor estao habilitadas.</returns>
    protected static bool IsContainerComparisonEnabled(
        ProviderId provider,
        string environmentVariableName)
        => RunContainerTests.Value
            && Environment.GetEnvironmentVariable(environmentVariableName) == "true"
            && IsProviderContainerComparisonEnabled(provider);

    /// <summary>
    /// EN: Resolves whether the specified provider can run container comparisons in the current environment.
    /// PT: ResolveRowsFrameRange se o provedor informado pode executar comparacoes com container no ambiente atual.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when the provider is allowed to run container comparisons. PT: True quando o provedor pode executar comparacoes com container.</returns>
    protected static bool IsProviderContainerComparisonEnabled(ProviderId provider)
        => provider switch
        {
            ProviderId.Db2 => Environment.GetEnvironmentVariable("RUN_DB2_CONTAINER_TESTS") == "true",
            ProviderId.MariaDb => Environment.GetEnvironmentVariable("RUN_MARIADB_CONTAINER_TESTS") == "true",
            _ => true,
        };

    /// <summary>
    /// EN: Resolves whether performance fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade de performance tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when performance container comparison is enabled. PT: True quando a comparacao de performance com container esta habilitada.</returns>
    protected static bool IsPerformanceContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_PERFORMANCE_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether temporary-table fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade de tabela temporaria tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when temporary-table container comparison is enabled. PT: True quando a comparacao de tabela temporaria com container esta habilitada.</returns>
    protected static bool IsTemporaryTableContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_TEMPORARY_TABLE_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether DDL fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade DDL tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when DDL container comparison is enabled. PT: True quando a comparacao DDL com container esta habilitada.</returns>
    protected static bool IsTableContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_TABLE_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether DML fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade DML tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when DML container comparison is enabled. PT: True quando a comparacao DML com container esta habilitada.</returns>
    protected static bool IsInsertContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_INSERT_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether query fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade de query tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when query container comparison is enabled. PT: True quando a comparacao de query com container esta habilitada.</returns>
    protected static bool IsSelectContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_SELECT_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether schema fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade de schema tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when schema container comparison is enabled. PT: True quando a comparacao de schema com container esta habilitada.</returns>
    protected static bool IsSchemaContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_SCHEMA_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves whether transaction fidelity tests should also run against a container.
    /// PT: ResolveRowsFrameRange se os testes de fidelidade de transacao tambem devem rodar contra um container.
    /// </summary>
    /// <param name="provider">EN: The provider being evaluated. PT: O provedor avaliado.</param>
    /// <returns>EN: True when transaction container comparison is enabled. PT: True quando a comparacao de transacao com container esta habilitada.</returns>
    protected static bool IsTransactionContainerComparisonEnabled(ProviderId provider)
        => IsContainerComparisonEnabled(provider, "RUN_TRANSACTION_CONTAINER_TESTS");

    /// <summary>
    /// EN: Resolves a container connection string for the specified provider when one is available.
    /// PT: ResolveRowsFrameRange uma string de conexao de container para o provedor informado quando houver uma disponivel.
    /// </summary>
    /// <param name="provider">EN: The provider identifier used to select the environment variables. PT: O identificador do provedor usado para selecionar as variaveis de ambiente.</param>
    /// <param name="connectionString">EN: The resolved container connection string when available. PT: A string de conexao de container resolvida quando disponivel.</param>
    /// <returns>EN: True when the provider has a usable container connection string. PT: True quando o provedor possui uma string de conexao de container utilizavel.</returns>
    protected static bool TryResolveContainerConnectionString(
        ProviderId provider,
        out string connectionString)
        => ProviderConnectionStringResolver.TryResolve(provider, out connectionString);

    /// <summary>
    /// EN: Creates the base test with the output helper.
    /// PT: Cria o teste base com o helper de saida.
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
    /// PT: ResolveRowsFrameRange o nome do método de teste atual.
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

    private static readonly ConcurrentDictionary<string, object> ObjCache = [];
    
    /// <summary>
    /// Retrieves a cached dialect instance for the specified version, or creates and caches a new instance if one does
    /// not exist.
    /// </summary>
    /// <remarks>This method uses an internal cache to store and retrieve dialect instances by type and
    /// version. If a dialect for the given version does not exist in the cache, the provided creation function is used
    /// to instantiate and cache it. This approach improves performance by avoiding redundant instantiations.</remarks>
    /// <typeparam name="T">The type of the dialect to retrieve or create.</typeparam>
    /// <param name="version">The version number used to identify the dialect instance.</param>
    /// <param name="fnCreate">A function that creates a new dialect instance for the specified version if one is not already cached.</param>
    /// <returns>A dialect instance of type T corresponding to the specified version.</returns>
    protected static T Get<T>(int version, Func<int,T> fnCreate)
        => (T)ObjCache.GetOrAdd($"{typeof(T).FullName}_v{version}", _=> fnCreate(version)!);

    #region Dispose

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

    #endregion
}
