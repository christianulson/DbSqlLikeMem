namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: In-memory schema mock configured for Firebird.
/// PT-br: Schema simulado em memória configurado para Firebird.
/// </summary>
public class FirebirdSchemaMock(
    string schemaName,
    DbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a Firebird table mock instance.
    /// PT-br: Cria uma instância de tabela simulada Firebird.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT-br: Linhas iniciais.</param>
    /// <returns>EN: New table mock. PT-br: Nova tabela simulada.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new FirebirdTableMock(tableName, this, columns, rows);
}

