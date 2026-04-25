namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Marks a fidelity test that should skip when the shared fidelity infrastructure is unavailable.
/// PT: Marca um teste de fidelidade que deve ser ignorado quando a infraestrutura compartilhada de fidelidade estiver indisponivel.
/// </summary>
public sealed class FidelityFactAttribute : FactAttribute
{
    /// <summary>
    /// EN: Creates a fidelity fact that skips on <see cref="FidelityTestSkippedException"/>.
    /// PT: Cria um fact de fidelidade que ignora ao receber <see cref="FidelityTestSkippedException"/>.
    /// </summary>
    public FidelityFactAttribute()
    {
        SkipExceptions = new[] { typeof(FidelityTestSkippedException) };
    }
}
