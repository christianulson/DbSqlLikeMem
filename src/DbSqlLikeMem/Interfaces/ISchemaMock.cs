using System.Collections.Generic;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the contract of an in-memory schema with table creation and retrieval.
/// PT: Define o contrato de um schema em memória com criação e recuperação de tabelas.
/// </summary>
public interface ISchemaMock
{
    /// <summary>
    /// EN: Creates a table with columns and optional initial rows.
    /// PT: Cria uma tabela com colunas e linhas iniciais opcionais.
    /// </summary>
    /// <param name="tableName">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="columns">EN: Table columns. PT: Colunas da tabela.</param>
    /// <param name="rows">EN: Optional initial rows. PT: Linhas iniciais opcionais.</param>
    /// <returns>EN: Created table. PT: Tabela criada.</returns>
    TableMock CreateTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// EN: Tries to get a table by name in the schema.
    /// PT: Tenta obter uma tabela pelo nome do schema.
    /// </summary>
    /// <param name="key">EN: Table name. PT: Nome da tabela.</param>
    /// <param name="value">EN: Found table, if any. PT: Tabela encontrada, se houver.</param>
    /// <returns>EN: True if the table exists. PT: True se a tabela existir.</returns>
    bool TryGetTable(string key, out ITableMock? value);

    /// <summary>
    /// EN: Backs up all tables, ignoring individual failures.
    /// PT: Realiza backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void BackupAllTablesBestEffort();

    /// <summary>
    /// EN: Restores backups of all tables, ignoring individual failures.
    /// PT: Restaura backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void RestoreAllTablesBestEffort();

    /// <summary>
    /// EN: Clears stored backups for all tables.
    /// PT: Limpa backups armazenados para todas as tabelas.
    /// </summary>
    void ClearBackupAllTablesBestEffort();
}
