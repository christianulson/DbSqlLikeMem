namespace DbSqlLikeMem;

/// <summary>
/// EN: Stores the definition and current state of a schema sequence.
/// PT: Armazena a definicao e o estado atual de uma sequence do schema.
/// </summary>
public sealed class SequenceDef
{
    /// <summary>
    /// EN: Initializes a sequence definition with its numeric settings.
    /// PT: Inicializa uma definicao de sequence com suas configuracoes numericas.
    /// </summary>
    /// <param name="name">EN: Sequence name. PT: Nome da sequence.</param>
    /// <param name="startValue">EN: First value produced by the sequence. PT: Primeiro valor produzido pela sequence.</param>
    /// <param name="incrementBy">EN: Increment step between values. PT: Passo de incremento entre valores.</param>
    /// <param name="currentValue">EN: Current sequence value when known. PT: Valor atual da sequence quando conhecido.</param>
    public SequenceDef(
        string name,
        long startValue = 1,
        long incrementBy = 1,
        long? currentValue = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        if (incrementBy == 0)
            throw new ArgumentOutOfRangeException(nameof(incrementBy));

        Name = name.NormalizeName();
        StartValue = startValue;
        IncrementBy = incrementBy;
        CurrentValue = currentValue;
        IsCalled = currentValue.HasValue;
    }

    /// <summary>
    /// EN: Gets the normalized sequence name.
    /// PT: Obtem o nome normalizado da sequence.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// EN: Gets the first value produced by the sequence.
    /// PT: Obtem o primeiro valor produzido pela sequence.
    /// </summary>
    public long StartValue { get; }

    /// <summary>
    /// EN: Gets the increment step between generated values.
    /// PT: Obtem o passo de incremento entre os valores gerados.
    /// </summary>
    public long IncrementBy { get; }

    /// <summary>
    /// EN: Gets or sets the current sequence value when it is known.
    /// PT: Obtem ou define o valor atual da sequence quando ele e conhecido.
    /// </summary>
    public long? CurrentValue { get; set; }

    internal bool IsCalled { get; private set; }

    /// <summary>
    /// EN: Generates the next value and stores it as the current value.
    /// PT: Gera o proximo valor e o armazena como valor atual.
    /// </summary>
    /// <returns>EN: Next sequence value. PT: Proximo valor da sequence.</returns>
    public long NextValue()
    {
        CurrentValue = !CurrentValue.HasValue
            ? StartValue
            : IsCalled
                ? CurrentValue.Value + IncrementBy
                : CurrentValue.Value;

        IsCalled = true;
        return CurrentValue.Value;
    }

    internal long SetValue(long value, bool isCalled)
    {
        CurrentValue = value;
        IsCalled = isCalled;
        return value;
    }
}
