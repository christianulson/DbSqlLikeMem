using System.Runtime.CompilerServices;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Marks a fidelity theory that should skip when the shared fidelity infrastructure is unavailable.
/// PT-br: Marca uma theory de fidelidade que deve ser ignorada quando a infraestrutura compartilhada de fidelidade estiver indisponivel.
/// </summary>
public sealed class FidelityTheoryAttribute : TheoryAttribute
{
    /// <summary>
    /// EN: Creates a fidelity theory for the current xUnit target.
    /// PT-br: Cria uma theory de fidelidade para o alvo atual do xUnit.
    /// </summary>
    public FidelityTheoryAttribute()
    {
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Creates a fidelity theory with source information for xUnit analyzers.
    /// PT-br: Cria uma theory de fidelidade com informacoes de origem para os analisadores do xUnit.
    /// </summary>
    /// <param name="filePath">EN: The source file path for the test attribute. PT-br: O caminho do arquivo fonte do atributo de teste.</param>
    /// <param name="lineNumber">EN: The source line number for the test attribute. PT-br: O numero da linha fonte do atributo de teste.</param>
    public FidelityTheoryAttribute(
        [CallerFilePath] string? filePath = null,
        [CallerLineNumber] int lineNumber = -1)
        : base(filePath, lineNumber)
    {
    }
#endif
}
