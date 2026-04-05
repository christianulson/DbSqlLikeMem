namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: In-memory schema mock configured for Firebird.
/// PT: Schema simulado em memória configurado para Firebird.
/// </summary>
public class FirebirdSchemaMock(
    string schemaName,
    DbMock db,
    IDictionary<string, (IEnumerable<Col> columns, IEnumerable<Dictionary<int, object?>>? rows)>? tables = null
) : SchemaMock(schemaName, db, tables)
{
    /// <summary>
    /// EN: Creates a Firebird table mock instance.
    /// PT: Cria uma instância de tabela simulada Firebird.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Initial rows. PT: Linhas iniciais.</param>
    /// <returns>EN: New table mock. PT: Nova tabela simulada.</returns>
    protected override TableMock NewTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null)
        => new FirebirdTableMock(tableName, this, columns, rows);
}

