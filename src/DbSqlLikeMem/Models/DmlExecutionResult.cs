namespace DbSqlLikeMem;

/// <summary>
/// EN: Represents the result of a DML operation (INSERT, UPDATE, DELETE) including affected rows and their indexes.
/// PT: Representa o resultado de uma operação DML (INSERT, UPDATE, DELETE) incluindo as linhas afetadas e seus índices.
/// </summary>
internal sealed class DmlExecutionResult
{
    private static readonly IList<int> EmptyIndexes = [];

    /// <summary>
    /// EN: Gets the number of rows affected by the operation.
    /// PT: Obtem o numero de linhas afetadas pela operacao.
    /// </summary>
    public int AffectedRows { get; internal set; }

    /// <summary>
    /// EN: Gets the list of row indexes affected by the operation.
    /// PT: Obtem a lista de indices de linhas afetadas pela operacao.
    /// </summary>
    public IList<int> AffectedIndexes { get; init; } = EmptyIndexes;

    /// <summary>
    /// EN: Gets the list of row data snapshots affected by the operation.
    /// PT: Obtem a lista de snapshots de dados de linhas afetadas pela operacao.
    /// </summary>
    public IList<IReadOnlyDictionary<int, object?>> AffectedRowsData { get; init; } = [];

    /// <summary>
    /// EN: Creates a result for a specified number of affected rows without indexes.
    /// PT: Cria um resultado para um numero especificado de linhas afetadas sem indices.
    /// </summary>
    public static DmlExecutionResult ForCount(int count) => new() { AffectedRows = count };

    public void IncreseAffected(int count = 1)
        => AffectedRows += count;
}
