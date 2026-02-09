namespace DbSqlLikeMem;
/// <summary>
/// Implementa um dicionário de colunas com comparação case-insensitive.
/// </summary>
public class ColumnDictionary
    : Dictionary<string, ColumnDef>,
        IColumnDictionary
{
    /// <summary>
    /// Cria um dicionário de colunas ignorando maiúsculas/minúsculas.
    /// </summary>
    public ColumnDictionary() : base(StringComparer.OrdinalIgnoreCase) { }
}
