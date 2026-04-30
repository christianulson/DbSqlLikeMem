namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements a column dictionary with case-insensitive comparison.
/// PT: Implementa um dicionário de colunas com comparação case-insensitive.
/// </summary>
public class ColumnDictionary
    : Dictionary<string, ColumnDef>,
        IColumnDictionary
{
    /// <summary>
    /// EN: Creates a column dictionary ignoring case.
    /// PT: Cria um dicionário de colunas ignorando maiúsculas/minúsculas.
    /// </summary>
    public ColumnDictionary() : base(StringComparer.OrdinalIgnoreCase) { }
}
