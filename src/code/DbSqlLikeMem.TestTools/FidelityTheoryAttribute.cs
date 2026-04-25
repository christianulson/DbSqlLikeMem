namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Marks a fidelity theory that should skip when the shared fidelity infrastructure is unavailable.
/// PT: Marca uma theory de fidelidade que deve ser ignorada quando a infraestrutura compartilhada de fidelidade estiver indisponivel.
/// </summary>
public sealed class FidelityTheoryAttribute : TheoryAttribute
{
    /// <summary>
    /// EN: Creates a fidelity theory that skips on <see cref="FidelityTestSkippedException"/>.
    /// PT: Cria uma theory de fidelidade que ignora ao receber <see cref="FidelityTestSkippedException"/>.
    /// </summary>
    public FidelityTheoryAttribute()
    {
        SkipExceptions = new[] { typeof(FidelityTestSkippedException) };
    }
}
