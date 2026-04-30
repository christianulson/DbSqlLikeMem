using System.Runtime.CompilerServices;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Marks a fidelity test that can skip when the provider-specific fidelity guard reports it is unavailable.
/// PT: Marca um teste de fidelidade que pode ser ignorado quando o guard especifico do provedor indicar indisponibilidade.
/// </summary>
#if !NET8_0_OR_GREATER
[Xunit.Sdk.XunitTestCaseDiscoverer("DbSqlLikeMem.TestTools.FidelityFactDiscoverer", "DbSqlLikeMem.TestTools")]
#endif
public sealed class FidelityFactAttribute : FactAttribute
{
    /// <summary>
    /// EN: Creates a fidelity fact for the current xUnit target.
    /// PT: Cria um fact de fidelidade para o alvo atual do xUnit.
    /// </summary>
    public FidelityFactAttribute()
    {
#if NET8_0_OR_GREATER
        SkipExceptions = new[] { typeof(FidelityTestSkippedException) };
#endif
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Creates a fidelity fact with source information for xUnit analyzers.
    /// PT: Cria um fact de fidelidade com informacoes de origem para os analisadores do xUnit.
    /// </summary>
    /// <param name="filePath">EN: The source file path for the test attribute. PT: O caminho do arquivo fonte do atributo de teste.</param>
    /// <param name="lineNumber">EN: The source line number for the test attribute. PT: O numero da linha fonte do atributo de teste.</param>
    public FidelityFactAttribute(
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = -1)
        : base(filePath, lineNumber)
    {
        SkipExceptions = new[] { typeof(FidelityTestSkippedException) };
    }
#endif
}
