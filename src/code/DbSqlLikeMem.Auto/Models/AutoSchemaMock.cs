namespace DbSqlLikeMem.Auto;

/// <summary>
/// EN: Schema mock for Auto databases.
/// PT-br: Esquema simulado para bancos Auto.
/// </summary>
public class AutoSchemaMock(
    string schemaName,
    AutoDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a Auto table mock for this schema.
    /// PT-br: Cria um simulado de tabela Auto para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT-br: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new AutoTableMock(tableName, this, columns, rows);
}
