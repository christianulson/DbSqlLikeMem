namespace DbSqlLikeMem;

/// <summary>
/// EN: Implements a table dictionary with case-insensitive comparison.
/// PT: Implementa um dicionário de tabelas com comparação case-insensitive.
/// </summary>
public class TableDictionary
    : Dictionary<string, ITableMock>,
    ITableDictionary
{
    /// <summary>
    /// EN: Creates an empty table dictionary.
    /// PT: Cria um dicionário de tabelas vazio.
    /// </summary>
    public TableDictionary()
        : base(StringComparer.OrdinalIgnoreCase)
    {
    }

    /// <summary>
    /// EN: Creates a table dictionary from another set.
    /// PT: Cria um dicionário de tabelas a partir de outro conjunto.
    /// </summary>
    /// <param name="tables">EN: Initial tables. PT: Tabelas iniciais.</param>
    public TableDictionary(
        IDictionary<string, ITableMock>? tables)
    : base(StringComparer.OrdinalIgnoreCase)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(tables, nameof(tables));
        foreach (var it in tables)
            Add(it.Key, it.Value);
    }
}
