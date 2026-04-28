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
    /// <param name="minValue">EN: Minimum allowed value when the sequence is bounded. PT: Valor minimo permitido quando a sequence e limitada.</param>
    /// <param name="maxValue">EN: Maximum allowed value when the sequence is bounded. PT: Valor maximo permitido quando a sequence e limitada.</param>
    /// <param name="isCycle">EN: Whether the sequence wraps around when it reaches a bound. PT: Indica se a sequence reinicia ao atingir um limite.</param>
    /// <param name="ownedBySchema">EN: Schema of the owning table when the sequence is attached to a column. PT: Schema da tabela proprietaria quando a sequence e vinculada a uma coluna.</param>
    /// <param name="ownedByTable">EN: Owning table name when the sequence is attached to a column. PT: Nome da tabela proprietaria quando a sequence e vinculada a uma coluna.</param>
    /// <param name="ownedByColumn">EN: Owning column name when the sequence is attached to a column. PT: Nome da coluna proprietaria quando a sequence e vinculada a uma coluna.</param>
    [System.Text.Json.Serialization.JsonConstructor]
    public SequenceDef(
        string name,
        long startValue = 1,
        long incrementBy = 1,
        long? currentValue = null,
        long? minValue = null,
        long? maxValue = null,
        bool isCycle = false,
        string? ownedBySchema = null,
        string? ownedByTable = null,
        string? ownedByColumn = null)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(name, nameof(name));
        if (incrementBy == 0)
            throw new ArgumentOutOfRangeException(nameof(incrementBy));
        if (minValue.HasValue && maxValue.HasValue && minValue.Value > maxValue.Value)
            throw new ArgumentOutOfRangeException(nameof(minValue), "Minimum value cannot be greater than maximum value.");

        Name = name.NormalizeName();
        StartValue = startValue;
        IncrementBy = incrementBy;
        CurrentValue = currentValue;
        MinValue = minValue;
        MaxValue = maxValue;
        IsCycle = isCycle;
        OwnedBySchema = ownedBySchema?.NormalizeName();
        OwnedByTable = ownedByTable?.NormalizeName();
        OwnedByColumn = ownedByColumn?.NormalizeName();
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
    public long IncrementBy { get; private set; }

    /// <summary>
    /// EN: Gets the minimum allowed value when the sequence is bounded.
    /// PT: Obtem o valor minimo permitido quando a sequence e limitada.
    /// </summary>
    public long? MinValue { get; }

    /// <summary>
    /// EN: Gets the maximum allowed value when the sequence is bounded.
    /// PT: Obtem o valor maximo permitido quando a sequence e limitada.
    /// </summary>
    public long? MaxValue { get; }

    /// <summary>
    /// EN: Gets whether the sequence wraps around after reaching a bound.
    /// PT: Obtem se a sequence reinicia apos atingir um limite.
    /// </summary>
    public bool IsCycle { get; }

    /// <summary>
    /// EN: Gets the schema of the owning table when the sequence is attached to a column.
    /// PT: Obtem o schema da tabela proprietaria quando a sequence e vinculada a uma coluna.
    /// </summary>
    public string? OwnedBySchema { get; private set; }

    /// <summary>
    /// EN: Gets the owning table name when the sequence is attached to a column.
    /// PT: Obtem o nome da tabela proprietaria quando a sequence e vinculada a uma coluna.
    /// </summary>
    public string? OwnedByTable { get; private set; }

    /// <summary>
    /// EN: Gets the owning column name when the sequence is attached to a column.
    /// PT: Obtem o nome da coluna proprietaria quando a sequence e vinculada a uma coluna.
    /// </summary>
    public string? OwnedByColumn { get; private set; }

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
        var nextValue = !CurrentValue.HasValue
            ? StartValue
            : IsCalled
                ? CurrentValue.Value + IncrementBy
                : CurrentValue.Value;

        nextValue = AdjustForBounds(nextValue);
        CurrentValue = nextValue;
        IsCalled = true;
        return CurrentValue.Value;
    }

    private long AdjustForBounds(long nextValue)
    {
        if (IncrementBy > 0)
        {
            if (MaxValue.HasValue && nextValue > MaxValue.Value)
                return IsCycle ? (MinValue ?? throw new InvalidOperationException($"Sequence '{Name}' reached its minimum value without a configured minimum.")) : throw new InvalidOperationException($"Sequence '{Name}' reached its maximum value.");

            if (MinValue.HasValue && nextValue < MinValue.Value)
                return IsCycle ? (MaxValue ?? throw new InvalidOperationException($"Sequence '{Name}' reached its maximum value without a configured maximum.")) : throw new InvalidOperationException($"Sequence '{Name}' reached its minimum value.");

            return nextValue;
        }

        if (MinValue.HasValue && nextValue < MinValue.Value)
            return IsCycle ? (MaxValue ?? throw new InvalidOperationException($"Sequence '{Name}' reached its maximum value without a configured maximum.")) : throw new InvalidOperationException($"Sequence '{Name}' reached its minimum value.");

        if (MaxValue.HasValue && nextValue > MaxValue.Value)
            return IsCycle ? (MinValue ?? throw new InvalidOperationException($"Sequence '{Name}' reached its minimum value without a configured minimum.")) : throw new InvalidOperationException($"Sequence '{Name}' reached its maximum value.");

        return nextValue;
    }

    internal long SetValue(long value, bool isCalled)
    {
        CurrentValue = value;
        IsCalled = isCalled;
        return value;
    }

    internal void SetIncrementBy(long incrementBy)
    {
        if (incrementBy == 0)
            throw new ArgumentOutOfRangeException(nameof(incrementBy));

        IncrementBy = incrementBy;
    }

    internal void SetOwnership(
        string? ownedBySchema,
        string? ownedByTable,
        string? ownedByColumn)
    {
        OwnedBySchema = ownedBySchema?.NormalizeName();
        OwnedByTable = ownedByTable?.NormalizeName();
        OwnedByColumn = ownedByColumn?.NormalizeName();
    }

    internal void ClearOwnership()
    {
        OwnedBySchema = null;
        OwnedByTable = null;
        OwnedByColumn = null;
    }
}
