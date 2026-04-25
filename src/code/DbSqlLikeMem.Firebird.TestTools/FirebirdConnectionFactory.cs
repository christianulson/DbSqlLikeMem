using System.Runtime.InteropServices;
using FirebirdSql.Data.FirebirdClient;
using DbSqlLikeMem.TestTools;

namespace DbSqlLikeMem.Firebird.TestTools;

/// <summary>
/// EN: Creates Firebird provider connections after verifying that the native client is available.
/// PT: Cria conexoes do provedor Firebird apos verificar que o cliente nativo esta disponivel.
/// </summary>
public static class FirebirdConnectionFactory
{
    private const string NativeLibraryName = "fbclient.dll";

    /// <summary>
    /// EN: Creates a Firebird connection for fidelity tests and skips the test when the native client is missing.
    /// PT: Cria uma conexao Firebird para testes de fidelidade e ignora o teste quando o cliente nativo estiver ausente.
    /// </summary>
    /// <param name="connectionString">EN: The provider connection string used to open the connection. PT: A string de conexao do provedor usada para abrir a conexao.</param>
    /// <returns>EN: A ready-to-use Firebird connection. PT: Uma conexao Firebird pronta para uso.</returns>
    public static FbConnection Create(string connectionString)
    {
        EnsureNativeClientAvailable();
        return new FbConnection(connectionString);
    }

    /// <summary>
    /// EN: Ensures the Firebird native client can be loaded by the current test process.
    /// PT: Garante que o cliente nativo do Firebird possa ser carregado pelo processo de teste atual.
    /// </summary>
    /// <exception cref="FidelityTestSkippedException">EN: Thrown when the native client is not available. PT: Lancada quando o cliente nativo nao esta disponivel.</exception>
    public static void EnsureNativeClientAvailable()
    {
        var candidatePath = Path.Combine(AppContext.BaseDirectory, NativeLibraryName);

#if NET462
        if (!IsNativeLibraryAvailable(candidatePath) && !IsNativeLibraryAvailable(NativeLibraryName))
        {
            throw new FidelityTestSkippedException("Firebird native client is not available.");
        }
#else
        if (!NativeLibrary.TryLoad(candidatePath, out var handle)
            && !NativeLibrary.TryLoad(NativeLibraryName, out handle))
        {
            throw new FidelityTestSkippedException("Firebird native client is not available.");
        }

        NativeLibrary.Free(handle);
#endif
    }

#if NET462
    [DllImport("kernel32.dll", CharSet = CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr LoadLibrary(string lpFileName);

    [DllImport("kernel32.dll", SetLastError = true)]
    private static extern bool FreeLibrary(IntPtr hModule);

    private static bool IsNativeLibraryAvailable(string libraryName)
    {
        var handle = LoadLibrary(libraryName);
        if (handle == IntPtr.Zero)
        {
            return false;
        }

        FreeLibrary(handle);
        return true;
    }
#endif
}
