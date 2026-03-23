namespace DbSqlLikeMem.TestTools.DML;

public partial class DmlMutationServiceTest<T>
{
    /// <summary>
    /// EN: Reads the next sequence value for the configured sequence.
    /// PT: Lê o próximo valor da sequência configurada.
    /// </summary>
    public object? RunSequenceNextValue(params object[] pars)
    {
        if (!Dialect.SupportsSequence)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the sequence benchmark.");
        }

        var sequence = (string)pars[0];
        var value = ExecuteScalar(Dialect.NextSequenceValue(sequence));
        GC.KeepAlive(value);
        return value;
    }
}
