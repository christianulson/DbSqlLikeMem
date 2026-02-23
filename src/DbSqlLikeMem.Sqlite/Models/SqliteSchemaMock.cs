namespace DbSqlLikeMem.Sqlite;

/// <summary>
/// EN: Schema mock for SQLite databases.
/// PT: Esquema simulado para bancos SQLite.
/// </summary>
public class SqliteSchemaMock(
    string schemaName,
    SqliteDbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
    ) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a SQLite table mock for this schema.
    /// PT: Cria um simulado de tabela SQLite para este schema.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: Table mock. PT: Mock de tabela.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new SqliteTableMock(tableName, this, columns, rows);
}
