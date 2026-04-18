namespace DbSqlLikeMem.Db2.TestTools;

/// <summary>
/// EN: Validates that the Db2 native client can be loaded before any real provider connection is created.
/// PT: Valida se o cliente nativo do Db2 pode ser carregado antes de qualquer conexao real do provedor ser criada.
/// </summary>
public static class Db2NativeClientGuard
{
    private const string NativeLibraryName = "db2app64.dll";

    /// <summary>
    /// EN: Ensures the Db2 native client is available in the current process.
    /// PT: Garante que o cliente nativo do Db2 esteja disponivel no processo atual.
    /// </summary>
    /// <exception cref="InvalidOperationException">EN: Thrown when the Db2 native client cannot be loaded. PT: Lancada quando o cliente nativo do Db2 nao pode ser carregado.</exception>
    public static void EnsureNativeClientAvailable()
    {
#if NET462
        if (!IsNativeLibraryAvailable(NativeLibraryName))
        {
            throw new InvalidOperationException("Db2 native client 'db2app64.dll' is not available. Install the Db2 client/runtime or disable Db2 fidelity tests.");
        }
#else
        if (!System.Runtime.InteropServices.NativeLibrary.TryLoad(NativeLibraryName, out var handle))
        {
            throw new InvalidOperationException("Db2 native client 'db2app64.dll' is not available. Install the Db2 client/runtime or disable Db2 fidelity tests.");
        }

        System.Runtime.InteropServices.NativeLibrary.Free(handle);
#endif
    }

#if NET462
    [System.Runtime.InteropServices.DllImport("kernel32.dll", CharSet = System.Runtime.InteropServices.CharSet.Unicode, SetLastError = true)]
    private static extern IntPtr LoadLibrary(string lpFileName);

    [System.Runtime.InteropServices.DllImport("kernel32.dll", SetLastError = true)]
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
