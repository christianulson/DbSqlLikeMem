using System;
using DbSqlLikeMem.Firebird.TestTools;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Skips Firebird fidelity tests when the native client cannot be loaded.
/// PT: Ignora testes de fidelidade Firebird quando o cliente nativo nao pode ser carregado.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = false, Inherited = true)]
public sealed class FidelityNativeClientSkipAttribute : Attribute, IFidelityTestSkipProvider
{
    /// <inheritdoc />
    public string? GetSkipReason()
    {
        try
        {
            FirebirdConnectionFactory.EnsureNativeClientAvailable();
            return null;
        }
        catch (FidelityTestSkippedException ex)
        {
            return ex.Message;
        }
    }
}

