namespace DbSqlLikeMem;

/// <summary>
/// Implementa um dicionário de tabelas com comparação case-insensitive.
/// </summary>
public class TableDictionary
    : Dictionary<string, ITableMock>,
    ITableDictionary
{
    /// <summary>
    /// Cria um dicionário de tabelas vazio.
    /// </summary>
    public TableDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// Cria um dicionário de tabelas a partir de outro conjunto.
    /// </summary>
    /// <param name="tables">Tabelas iniciais.</param>
    public TableDictionary(
        IDictionary<string, ITableMock>? tables)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullException.ThrowIfNull(tables);
        foreach (var (k, v) in tables)
            Add(k, v);
    }
}
