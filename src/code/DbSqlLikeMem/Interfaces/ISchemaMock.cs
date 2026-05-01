namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the contract of an in-memory schema with tables, procedures, and sequences.
/// PT-br: Define o contrato de um schema em memória com tabelas, procedimentos e sequences.
/// </summary>
public interface ISchemaMock
{
    /// <summary>
    /// EN: Normalized schema name.
    /// PT-br: Nome normalizado do schema.
    /// </summary>
    string SchemaName { get; }

    /// <summary>
    /// EN: Creates a table with columns and optional initial rows.
    /// PT-br: Cria uma tabela com colunas e linhas iniciais opcionais.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT-br: Colunas da tabela.</param>
    /// <param name="rows">EN: Optional initial rows. PT-br: Linhas iniciais opcionais.</param>
    /// <returns>EN: Created table. PT-br: Tabela criada.</returns>
    TableMock CreateTable(
        string tableName,
        IEnumerable<Col> columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// EN: Tries to get a table by name in the schema.
    /// PT-br: Tenta obter uma tabela pelo nome do schema.
    /// </summary>
    /// <param name="key">EN: Table name. PT-br: Nome da tabela.</param>
    /// <param name="value">EN: Found table, if any. PT-br: Tabela encontrada, se houver.</param>
    /// <returns>EN: True if the table exists. PT-br: True se a tabela existir.</returns>
    bool TryGetTable(string key, out ITableMock? value);

    /// <summary>
    /// EN: Gets the sequences registered in the schema.
    /// PT-br: Obtém as sequences registradas no schema.
    /// </summary>
    IReadOnlyDictionary<string, SequenceDef> Sequences { get; }

    /// <summary>
    /// EN: Backs up all tables, ignoring individual failures.
    /// PT-br: Realiza backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void BackupAllTablesBestEffort();

    /// <summary>
    /// EN: Restores backups of all tables, ignoring individual failures.
    /// PT-br: Restaura backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void RestoreAllTablesBestEffort();

    /// <summary>
    /// EN: Clears stored backups for all tables.
    /// PT-br: Limpa backups armazenados para todas as tabelas.
    /// </summary>
    void ClearBackupAllTablesBestEffort();
}
