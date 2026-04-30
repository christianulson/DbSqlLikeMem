using DbSqlLikeMem.Db2.TestTools;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Skips Db2 fidelity tests when the native client cannot be loaded.
/// PT: Ignora testes de fidelidade Db2 quando o cliente nativo nao pode ser carregado.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class FidelityNativeClientSkipAttribute : Attribute, IFidelityTestSkipProvider
{
    /// <inheritdoc />
    public string? GetSkipReason()
    {
        try
        {
            Db2NativeClientGuard.EnsureNativeClientAvailable();
            return null;
        }
        catch (FidelityTestSkippedException ex)
        {
            return ex.Message;
        }
    }
}

