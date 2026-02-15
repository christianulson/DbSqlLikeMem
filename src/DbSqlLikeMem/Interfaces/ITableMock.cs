using System.Collections.Immutable;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the contract of an in-memory table with data, metadata, and index utilities.
/// PT: Define o contrato de uma tabela em memória, com dados, metadados e utilidades de índice.
/// </summary>
public interface ITableMock
    : IReadOnlyList<IReadOnlyDictionary<int, object?>>
{
    /// <summary>
    /// EN: Normalized table name.
    /// PT: Nome normalizado da tabela.
    /// </summary>
    string TableName { get; }

    /// <summary>
    /// EN: Schema to which the table belongs.
    /// PT: Schema ao qual a tabela pertence.
    /// </summary>
    SchemaMock Schema { get; }

    /// <summary>
    /// EN: Gets or sets the next identity value for auto-increment columns.
    /// PT: Obtém ou define o próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    int NextIdentity { get; set; }

    /// <summary>
    /// EN: Holds the column _indexes that compose the primary key.
    /// PT: Mantém os índices das colunas que compõem a chave primária.
    /// </summary>
    ImmutableHashSet<int> PrimaryKeyIndexes { get; }

    /// <summary>
    /// EN: Exposes foreign keys configured on the table.
    /// PT: Expõe as chaves estrangeiras configuradas na tabela.
    /// </summary>
    IReadOnlyList<(
        string Col, 
        string RefTable,
        string RefCol
        )> ForeignKeys { get; }

    /// <summary>
    /// EN: Creates a foreign key linking a local column to a column in another table.
    /// PT: Cria uma chave estrangeira ligando uma coluna local a uma coluna de outra tabela.
    /// </summary>
    /// <param name="col">EN: Local column name. PT: Nome da coluna local.</param>
    /// <param name="refTable">EN: Referenced table. PT: Tabela referenciada.</param>
    /// <param name="refCol">EN: Referenced column. PT: Coluna referenciada.</param>
    void CreateForeignKey(
        string col,
        string refTable,
        string refCol);

    /// <summary>
    /// EN: Gets the table column dictionary.
    /// PT: Obtém o dicionário de colunas da tabela.
    /// </summary>
    ImmutableDictionary<string, ColumnDef> Columns { get; }

    ColumnDef AddColumn(
        string name,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null);

    /// <summary>
    /// EN: Finds a column by name or throws if it does not exist.
    /// PT: Localiza uma coluna pelo nome ou lança erro se não existir.
    /// </summary>
    /// <param name="columnName">EN: Column name. PT: Nome da coluna.</param>
    /// <returns>EN: Found column definition. PT: Definição da coluna encontrada.</returns>
    ColumnDef GetColumn(string columnName);

    /// <summary>
    /// EN: Gets the _indexes declared for the table.
    /// PT: Obtém os índices declarados para a tabela.
    /// </summary>
    ImmutableDictionary<string, IndexDef> Indexes { get; }

    /// <summary>
    /// EN: Creates an index using the provided definition.
    /// PT: Cria um índice com a definição informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    void CreateIndex(IndexDef def);

    /// <summary>
    /// EN: Updates index structures using the specified row.
    /// PT: Atualiza estruturas de índice usando a linha indicada.
    /// </summary>
    /// <param name="rowIdx">EN: Updated row index. PT: Índice da linha atualizada.</param>
    void UpdateIndexesWithRow(int rowIdx);

    /// <summary>
    /// EN: Rebuilds all table _indexes.
    /// PT: Reconstrói todos os índices da tabela.
    /// </summary>
    void RebuildAllIndexes();
    /// <summary>
    /// EN: Performs a lookup in an index using the given key.
    /// PT: Executa uma busca em um índice com a chave informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT: Chave a buscar.</param>
    /// <returns>EN: List of positions or null if none. PT: Lista de posições ou null se não houver.</returns>
    IEnumerable<int>? Lookup(IndexDef def, string key);

    /// <summary>
    /// EN: Adds multiple items by converting them into rows.
    /// PT: Adiciona múltiplos itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo dos itens.</typeparam>
    /// <param name="items">EN: Items to insert. PT: Itens a inserir.</param>
    void AddRangeItems<T>(IEnumerable<T> items);

    /// <summary>
    /// EN: Adds a single item by converting it into a table row.
    /// PT: Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT: Tipo do item.</typeparam>
    /// <param name="item">EN: Item to insert. PT: Item a inserir.</param>
    void AddItem<T>(T item);

    /// <summary>
    /// EN: Adds a set of already materialized rows.
    /// PT: Adiciona um conjunto de linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT: Linhas a inserir.</param>
    void AddRange(IEnumerable<Dictionary<int, object?>> items);

    /// <summary>
    /// EN: Adds a specific row to the table.
    /// PT: Adiciona uma linha específica à tabela.
    /// </summary>
    /// <param name="value">EN: Row to insert. PT: Linha a inserir.</param>
    void Add(Dictionary<int, object?> value);


    /// <summary>
    /// Removes the element at the specified index and returns a dictionary containing the removed item.
    /// </summary>
    /// <param name="idx">The zero-based index of the element to remove. Must be within the valid range of the collection.</param>
    /// <returns>A dictionary containing the removed element, where the key is the index and the value is the element. If the
    /// index is invalid, the dictionary will be empty.</returns>
    Dictionary<int, object?> RemoveAt(int idx);

    /// <summary>
    /// Updates the value of a specific cell in the table at the given row and column indices.
    /// </summary>
    /// <param name="rowIdx">The zero-based index of the row containing the cell to update. Must be within the valid range of rows.</param>
    /// <param name="colIdx">The zero-based index of the column containing the cell to update. Must be within the valid range of columns.</param>
    /// <param name="value">The new value to assign to the specified cell. Can be null to clear the cell's contents.</param>
    void UpdateRowColumn(
        int rowIdx,
        int colIdx,
        object? value);

    /// <summary>
    /// EN: Backs up current rows for possible restoration.
    /// PT: Realiza backup das linhas atuais para possível restauração.
    /// </summary>
    void Backup();

    /// <summary>
    /// EN: Restores the last backup performed.
    /// PT: Restaura o último backup realizado.
    /// </summary>
    void Restore();

    /// <summary>
    /// EN: Clears the stored backup.
    /// PT: Limpa o backup armazenado.
    /// </summary>
    void ClearBackup();

    /// <summary>
    /// EN: Gets or sets the column currently being evaluated during parsing/execution.
    /// PT: Obtém ou define a coluna atualmente em avaliação durante parsing/execução.
    /// </summary>
    string? CurrentColumn { get; set; }

    /// <summary>
    /// EN: Resolves an SQL token to its value in the table context.
    /// PT: Resolve um token SQL para o valor correspondente no contexto da tabela.
    /// </summary>
    /// <param name="token">EN: Token to resolve. PT: Token a resolver.</param>
    /// <param name="dbType">EN: Expected token type. PT: Tipo esperado do token.</param>
    /// <param name="isNullable">EN: Whether the value can be null. PT: Indica se o valor pode ser nulo.</param>
    /// <param name="pars">EN: Query parameters, if any. PT: Parâmetros de consulta, quando aplicável.</param>
    /// <param name="colDict">EN: Optional column dictionary. PT: Dicionário de colunas opcional.</param>
    /// <returns>EN: Resolved value or null. PT: Valor resolvido ou null.</returns>
    object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        ImmutableDictionary<string, ColumnDef>? colDict = null);

    /// <summary>
    /// EN: Creates the exception for an unknown column.
    /// PT: Cria a exceção apropriada para coluna inexistente.
    /// </summary>
    /// <param name="columnName">EN: Invalid column name. PT: Nome da coluna inválida.</param>
    Exception UnknownColumn(string columnName);
    /// <summary>
    /// EN: Creates the exception for a duplicate key.
    /// PT: Cria a exceção apropriada para chave duplicada.
    /// </summary>
    /// <param name="tbl">EN: Affected table. PT: Tabela afetada.</param>
    /// <param name="key">EN: Key name. PT: Nome da chave.</param>
    /// <param name="val">EN: Duplicate value. PT: Valor duplicado.</param>
    Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// EN: Creates the exception for a non-nullable column.
    /// PT: Cria a exceção apropriada para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">EN: Column name. PT: Nome da coluna.</param>
    Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// EN: Creates the exception for a foreign key violation.
    /// PT: Cria a exceção apropriada para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">EN: Referencing column. PT: Coluna que referencia.</param>
    /// <param name="refTbl">EN: Referenced table. PT: Tabela referenciada.</param>
    Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// EN: Creates the exception for a row referenced by another table.
    /// PT: Cria a exceção apropriada para linha referenciada por outra tabela.
    /// </summary>
    /// <param name="tbl">EN: Referenced table. PT: Tabela referenciada.</param>
    Exception ReferencedRow(string tbl);
}
