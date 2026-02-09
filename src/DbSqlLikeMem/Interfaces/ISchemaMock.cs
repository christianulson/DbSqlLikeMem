namespace DbSqlLikeMem;

/// <summary>
/// Define o contrato de um schema em memória com criação e recuperação de tabelas.
/// </summary>
public interface ISchemaMock
{
    /// <summary>
    /// Cria uma tabela com colunas e linhas iniciais opcionais.
    /// </summary>
    /// <param name="tableName">Nome da tabela.</param>
    /// <param name="columns">Colunas da tabela.</param>
    /// <param name="rows">Linhas iniciais opcionais.</param>
    /// <returns>Tabela criada.</returns>
    TableMock CreateTable(
        string tableName,
        IColumnDictionary columns,
        IEnumerable<Dictionary<int, object?>>? rows = null);

    /// <summary>
    /// Tenta obter uma tabela pelo nome do schema.
    /// </summary>
    /// <param name="key">Nome da tabela.</param>
    /// <param name="value">Tabela encontrada, se houver.</param>
    /// <returns>True se a tabela existir.</returns>
    bool TryGetTable(string key, out ITableMock? value);

    /// <summary>
    /// Realiza backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void BackupAllTablesBestEffort();

    /// <summary>
    /// Restaura backup de todas as tabelas, ignorando falhas individuais.
    /// </summary>
    void RestoreAllTablesBestEffort();

    /// <summary>
    /// Limpa backups armazenados para todas as tabelas.
    /// </summary>
    void ClearBackupAllTablesBestEffort();
}
