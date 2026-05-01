namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines the contract of an in-memory table with data, metadata, and index utilities.
/// PT-br: Define o contrato de uma tabela em memória, com dados, metadados e utilidades de índice.
/// </summary>
public interface ITableMock
    : IReadOnlyList<IReadOnlyDictionary<int, object?>>
{
    /// <summary>
    /// EN: Gets the normalized table name used by the table mock.
    /// PT-br: Obtém o nome normalizado usado pelo mock da tabela.
    /// </summary>
    string TableName { get; }

    /// <summary>
    /// EN: Gets the schema that owns the table.
    /// PT-br: Obtém o schema que possui a tabela.
    /// </summary>
    SchemaMock Schema { get; }

    /// <summary>
    /// EN: Gets or sets the next identity value for auto-increment columns.
    /// PT-br: Obtém ou define o próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    int NextIdentity { get; set; }

    /// <summary>
    /// EN: Gets or sets whether explicit values can be inserted into identity columns.
    /// PT-br: Obtém ou define se valores explícitos podem ser inseridos em colunas identity.
    /// </summary>
    bool AllowIdentityInsert { get; set; }

    /// <summary>
    /// EN: Gets or sets the partitioning clause captured for the table.
    /// PT-br: Obtém ou define a clausula de particionamento capturada para a tabela.
    /// </summary>
    string? PartitionClauseSql { get; set; }

    /// <summary>
    /// EN: Sets whether explicit values can be inserted into identity columns.
    /// PT-br: Define se valores explicitos podem ser inseridos em colunas identity.
    /// </summary>
    /// <param name="allowIdentityInsert">EN: Allow identity insert. PT-br: Permitir insercao em identity.</param>
    ITableMock SetAllowIdentityInsert(bool allowIdentityInsert);

    /// <summary>
    /// EN: Gets the column indexes that compose the primary key.
    /// PT-br: Obtém os índices das colunas que compõem a chave primária.
    /// </summary>
    IReadOnlyHashSet<int> PrimaryKeyIndexes { get; }

    /// <summary>
    /// EN: Adds primary key index columns by name.
    /// PT-br: Adiciona colunas de índice de chave primária pelo nome.
    /// </summary>
    /// <param name="columns">EN: Primary key columns. PT-br: Colunas da chave primária.</param>
    void AddPrimaryKeyIndexes(params string[] columns);

    /// <summary>
    /// EN: Gets the foreign keys configured on the table.
    /// PT-br: Obtém as chaves estrangeiras configuradas na tabela.
    /// </summary>
    IReadOnlyDictionary<string, ForeignDef> ForeignKeys { get; }

    /// <summary>
    /// EN: Gets the check constraints configured on the table.
    /// PT-br: Obtém as restricoes check configuradas na tabela.
    /// </summary>
    IReadOnlyList<SchemaSnapshotCheckConstraint> CheckConstraints { get; }

    /// <summary>
    /// EN: Creates a foreign key linking a local column to a column in another table.
    /// PT-br: Cria uma chave estrangeira ligando uma coluna local a uma coluna de outra tabela.
    /// </summary>
    /// <param name="name">EN: Foreign key name. PT-br: Nome da chave estrangeira.</param>
    /// <param name="refTable">EN: Referenced table. PT-br: Tabela referenciada.</param>
    /// <param name="references">EN: Local/reference column mappings. PT-br: Mapeamentos coluna local/referenciada.</param>
    ForeignDef CreateForeignKey(
        string name,
        string refTable,
        HashSet<(string col, string refCol)> references);

    /// <summary>
    /// EN: Gets the dictionary of table columns keyed by name.
    /// PT-br: Obtém o dicionário de colunas da tabela indexado pelo nome.
    /// </summary>
    IReadOnlyDictionary<string, ColumnDef> Columns { get; }

    /// <summary>
    /// EN: Adds a new column definition to the table.
    /// PT-br: Adiciona uma nova definição de coluna à tabela.
    /// </summary>
    /// <param name="name">EN: Column name. PT-br: Nome da coluna.</param>
    /// <param name="dbType">EN: Column type. PT-br: Tipo da coluna.</param>
    /// <param name="nullable">EN: Allows null values. PT-br: Permite valores nulos.</param>
    /// <param name="size">EN: Optional size/length. PT-br: Tamanho/comprimento opcional.</param>
    /// <param name="decimalPlaces">EN: Optional decimal places. PT-br: Casas decimais opcionais.</param>
    /// <param name="identity">EN: Auto-increment flag. PT-br: Indicador de auto incremento.</param>
    /// <param name="defaultValue">EN: Optional default value. PT-br: Valor padrão opcional.</param>
    /// <param name="enumValues">EN: Optional enum values. PT-br: Valores de enum opcionais.</param>
    /// <param name="computedExpression">EN: Optional computed expression text. PT-br: Texto opcional da expressao computada.</param>
    ColumnDef AddColumn(
        string name,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null,
        string? computedExpression = null);

    /// <summary>
    /// EN: Finds a column by name or throws if it does not exist.
    /// PT-br: Localiza uma coluna pelo nome ou lança erro se não existir.
    /// </summary>
    /// <param name="columnName">EN: Column name. PT-br: Nome da coluna.</param>
    /// <returns>EN: Found column definition. PT-br: Definição da coluna encontrada.</returns>
    ColumnDef GetColumn(string columnName);

    /// <summary>
    /// EN: Gets the indexes declared for the table.
    /// PT-br: Obtém os índices declarados para a tabela.
    /// </summary>
    IReadOnlyDictionary<string, IndexDef> Indexes { get; }

    /// <summary>
    /// EN: Creates an index using the provided definition.
    /// PT-br: Cria um índice com a definição informada.
    /// </summary>
    IndexDef CreateIndex(
        string name,
        IEnumerable<string> keyCols,
        string[]? include = null,
        bool unique = false);

    /// <summary>
    /// EN: Updates index structures using the specified row.
    /// PT-br: Atualiza estruturas de índice usando a linha indicada.
    /// </summary>
    /// <param name="rowIdx">EN: Updated row index. PT-br: Índice da linha atualizada.</param>
    void UpdateIndexesWithRow(int rowIdx);

    /// <summary>
    /// EN: Rebuilds all table _indexes.
    /// PT-br: Reconstrói todos os índices da tabela.
    /// </summary>
    void RebuildAllIndexes();

    /// <summary>
    /// EN: Tries to find a row by its primary key using the fast PK index.
    /// PT-br: Tenta encontrar uma linha pela chave primaria usando o indice PK rapido.
    /// </summary>
    /// <param name="row">EN: Row containing PK values. PT-br: Linha contendo valores de PK.</param>
    /// <param name="rowIndex">EN: Found row index. PT-br: Indice da linha encontrada.</param>
    /// <returns>EN: True if a matching row was found. PT-br: True se uma linha correspondente foi encontrada.</returns>
    bool TryFindRowByPk(IReadOnlyDictionary<int, object?> row, out int rowIndex);
    /// <summary>
    /// EN: Performs a lookup in an index using the given key.
    /// PT-br: Executa uma busca em um índice com a chave informada.
    /// </summary>
    /// <param name="def">EN: Index definition. PT-br: Definição do índice.</param>
    /// <param name="key">EN: Key to search. PT-br: Chave a buscar.</param>
    /// <returns>EN: List of positions or null if none. PT-br: Lista de posições ou null se não houver.</returns>
    IReadOnlyDictionary<int, IReadOnlyDictionary<string, object?>>? Lookup(IndexDef def, IndexKey key);

    /// <summary>
    /// EN: Adds multiple items by converting them into rows.
    /// PT-br: Adiciona múltiplos itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT-br: Tipo dos itens.</typeparam>
    /// <param name="items">EN: Items to insert. PT-br: Itens a inserir.</param>
    ITableMock AddRangeItems<T>(IEnumerable<T> items);

    /// <summary>
    /// EN: Adds a single item by converting it into a table row.
    /// PT-br: Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">EN: Item type. PT-br: Tipo do item.</typeparam>
    /// <param name="item">EN: Item to insert. PT-br: Item a inserir.</param>
    ITableMock AddItem<T>(T item);

    /// <summary>
    /// EN: Adds a set of already materialized rows.
    /// PT-br: Adiciona um conjunto de linhas já materializadas.
    /// </summary>
    /// <param name="items">EN: Rows to insert. PT-br: Linhas a inserir.</param>
    ITableMock AddRange(IEnumerable<Dictionary<int, object?>> items);

    /// <summary>
    /// EN: Adds a specific row to the table.
    /// PT-br: Adiciona uma linha específica à tabela.
    /// </summary>
    /// <param name="value">EN: Row to insert. PT-br: Linha a inserir.</param>
    ITableMock Add(Dictionary<int, object?> value);

    /// <summary>
    /// EN: Removes the row at the specified index and returns the removed values.
    /// PT-br: Remove a linha no indice informado e retorna os valores removidos.
    /// </summary>
    /// <param name="idx">EN: Zero-based row index to remove. PT-br: Indice da linha, com base zero, a remover.</param>
    /// <returns>EN: Removed row values keyed by column index. PT-br: Valores da linha removida indexados pela coluna.</returns>
    Dictionary<int, object?> RemoveAt(int idx);

    /// <summary>
    /// EN: Updates a single cell in the table using the specified row and column indices.
    /// PT-br: Atualiza uma unica celula da tabela usando os indices de linha e coluna informados.
    /// </summary>
    /// <param name="rowIdx">EN: Zero-based row index containing the cell to update. PT-br: Indice da linha, com base zero, que contem a celula a atualizar.</param>
    /// <param name="colIdx">EN: Zero-based column index of the cell to update. PT-br: Indice da coluna, com base zero, da celula a atualizar.</param>
    /// <param name="value">EN: New value for the cell. PT-br: Novo valor para a celula.</param>
    void UpdateRowColumn(
        int rowIdx,
        int colIdx,
        object? value);

    /// <summary>
    /// EN: Backs up current rows for possible restoration.
    /// PT-br: Realiza backup das linhas atuais para possível restauração.
    /// </summary>
    void Backup();

    /// <summary>
    /// EN: Restores the last backup performed.
    /// PT-br: Restaura o último backup realizado.
    /// </summary>
    void Restore();

    /// <summary>
    /// EN: Clears the stored backup.
    /// PT-br: Limpa o backup armazenado.
    /// </summary>
    void ClearBackup();

    /// <summary>
    /// EN: Gets or sets the column currently being evaluated during parsing or execution.
    /// PT-br: Obtém ou define a coluna atualmente em avaliação durante parsing ou execucao.
    /// </summary>
    string? CurrentColumn { get; set; }

    /// <summary>
    /// EN: Resolves an SQL token to its value in the table context.
    /// PT-br: Resolve um token SQL para o valor correspondente no contexto da tabela.
    /// </summary>
    /// <param name="token">EN: Token to resolve. PT-br: Token a resolver.</param>
    /// <param name="dbType">EN: Expected token type. PT-br: Tipo esperado do token.</param>
    /// <param name="isNullable">EN: Whether the value can be null. PT-br: Indica se o valor pode ser nulo.</param>
    /// <param name="pars">EN: Query parameters, if any. PT-br: Parâmetros de consulta, quando aplicável.</param>
    /// <param name="colDict">EN: Optional column dictionary. PT-br: Dicionário de colunas opcional.</param>
    /// <returns>EN: Resolved value or null. PT-br: Valor resolvido ou null.</returns>
    object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IReadOnlyDictionary<string, ColumnDef>? colDict = null);

    /// <summary>
    /// EN: Creates the exception for an unknown column.
    /// PT-br: Cria a exceção apropriada para coluna inexistente.
    /// </summary>
    /// <param name="columnName">EN: Invalid column name. PT-br: Nome da coluna inválida.</param>
    Exception UnknownColumn(string columnName);
    /// <summary>
    /// EN: Creates the exception for a duplicate key.
    /// PT-br: Cria a exceção apropriada para chave duplicada.
    /// </summary>
    /// <param name="tbl">EN: Affected table. PT-br: Tabela afetada.</param>
    /// <param name="key">EN: Key name. PT-br: Nome da chave.</param>
    /// <param name="val">EN: Duplicate value. PT-br: Valor duplicado.</param>
    Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// EN: Creates the exception for a non-nullable column.
    /// PT-br: Cria a exceção apropriada para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">EN: Column name. PT-br: Nome da coluna.</param>
    Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// EN: Creates the exception for a foreign key violation.
    /// PT-br: Cria a exceção apropriada para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">EN: Referencing column. PT-br: Coluna que referencia.</param>
    /// <param name="refTbl">EN: Referenced table. PT-br: Tabela referenciada.</param>
    Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// EN: Creates the exception for a row referenced by another table.
    /// PT-br: Cria a exceção apropriada para linha referenciada por outra tabela.
    /// </summary>
    /// <param name="tbl">EN: Referenced table. PT-br: Tabela referenciada.</param>
    Exception ReferencedRow(string tbl);

    /// <summary>
    /// EN: Whether the table has any triggers for the specified event.
    /// PT-br: Se a tabela possui gatilhos para o evento especificado.
    /// </summary>
    bool HasTriggers(TableTriggerEvent evt);
}
