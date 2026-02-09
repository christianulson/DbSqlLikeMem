namespace DbSqlLikeMem;
/// <summary>
/// Define o contrato de uma tabela em memória, com dados, metadados e utilidades de índice.
/// </summary>
public interface ITableMock
    : IList<Dictionary<int, object?>>
{
    /// <summary>
    /// Obtém ou define o próximo valor de identidade para colunas auto incrementais.
    /// </summary>
    int NextIdentity { get; set; }

    /// <summary>
    /// Mantém os índices das colunas que compõem a chave primária.
    /// </summary>
    HashSet<int> PrimaryKeyIndexes { get; }

    /// <summary>
    /// Expõe as chaves estrangeiras configuradas na tabela.
    /// </summary>
    IReadOnlyList<(
        string Col, 
        string RefTable,
        string RefCol
        )> ForeignKeys { get; }

    /// <summary>
    /// Cria uma chave estrangeira ligando uma coluna local a uma coluna de outra tabela.
    /// </summary>
    /// <param name="col">Nome da coluna local.</param>
    /// <param name="refTable">Tabela referenciada.</param>
    /// <param name="refCol">Coluna referenciada.</param>
    void CreateForeignKey(
        string col,
        string refTable,
        string refCol);

    /// <summary>
    /// Obtém o dicionário de colunas da tabela.
    /// </summary>
    IColumnDictionary Columns{ get; }

    /// <summary>
    /// Localiza uma coluna pelo nome ou lança erro se não existir.
    /// </summary>
    /// <param name="columnName">Nome da coluna.</param>
    /// <returns>Definição da coluna encontrada.</returns>
    ColumnDef GetColumn(string columnName);

    /// <summary>
    /// Obtém os índices declarados para a tabela.
    /// </summary>
    IndexDictionary Indexes { get; }

    /// <summary>
    /// Cria um índice com a definição informada.
    /// </summary>
    /// <param name="def">Definição do índice.</param>
    void CreateIndex(IndexDef def);

    /// <summary>
    /// Atualiza estruturas de índice usando a linha indicada.
    /// </summary>
    /// <param name="rowIdx">Índice da linha atualizada.</param>
    void UpdateIndexesWithRow(int rowIdx);

    /// <summary>
    /// Reconstrói todos os índices da tabela.
    /// </summary>
    void RebuildAllIndexes();
    /// <summary>
    /// Executa uma busca em um índice com a chave informada.
    /// </summary>
    /// <param name="def">Definição do índice.</param>
    /// <param name="key">Chave a buscar.</param>
    /// <returns>Lista de posições ou null se não houver.</returns>
    IEnumerable<int>? Lookup(IndexDef def, string key);

    /// <summary>
    /// Adiciona múltiplos itens convertendo-os em linhas.
    /// </summary>
    /// <typeparam name="T">Tipo dos itens.</typeparam>
    /// <param name="items">Itens a inserir.</param>
    void AddRangeItems<T>(IEnumerable<T> items);

    /// <summary>
    /// Adiciona um item convertendo-o em linha da tabela.
    /// </summary>
    /// <typeparam name="T">Tipo do item.</typeparam>
    /// <param name="item">Item a inserir.</param>
    void AddItem<T>(T item);

    /// <summary>
    /// Adiciona um conjunto de linhas já materializadas.
    /// </summary>
    /// <param name="items">Linhas a inserir.</param>
    void AddRange(IEnumerable<Dictionary<int, object?>> items);

    /// <summary>
    /// Adiciona uma linha específica à tabela.
    /// </summary>
    /// <param name="value">Linha a inserir.</param>
    new void Add(Dictionary<int, object?> value);

    /// <summary>
    /// Realiza backup das linhas atuais para possível restauração.
    /// </summary>
    void Backup();

    /// <summary>
    /// Restaura o último backup realizado.
    /// </summary>
    void Restore();

    /// <summary>
    /// Limpa o backup armazenado.
    /// </summary>
    void ClearBackup();

    /// <summary>
    /// Obtém ou define a coluna atualmente em avaliação durante parsing/execução.
    /// </summary>
    string? CurrentColumn { get; set; }

    /// <summary>
    /// Resolve um token SQL para o valor correspondente no contexto da tabela.
    /// </summary>
    /// <param name="token">Token a resolver.</param>
    /// <param name="dbType">Tipo esperado do token.</param>
    /// <param name="isNullable">Indica se o valor pode ser nulo.</param>
    /// <param name="pars">Parâmetros de consulta, quando aplicável.</param>
    /// <param name="colDict">Dicionário de colunas opcional.</param>
    /// <returns>Valor resolvido ou null.</returns>
    object? Resolve(
        string token,
        DbType dbType,
        bool isNullable,
        IDataParameterCollection? pars = null,
        IColumnDictionary? colDict = null);

    /// <summary>
    /// Cria a exceção apropriada para coluna inexistente.
    /// </summary>
    /// <param name="columnName">Nome da coluna inválida.</param>
    Exception UnknownColumn(string columnName);
    /// <summary>
    /// Cria a exceção apropriada para chave duplicada.
    /// </summary>
    /// <param name="tbl">Tabela afetada.</param>
    /// <param name="key">Nome da chave.</param>
    /// <param name="val">Valor duplicado.</param>
    Exception DuplicateKey(string tbl, string key, object? val);
    /// <summary>
    /// Cria a exceção apropriada para coluna que não aceita nulos.
    /// </summary>
    /// <param name="col">Nome da coluna.</param>
    Exception ColumnCannotBeNull(string col);
    /// <summary>
    /// Cria a exceção apropriada para violação de chave estrangeira.
    /// </summary>
    /// <param name="col">Coluna que referencia.</param>
    /// <param name="refTbl">Tabela referenciada.</param>
    Exception ForeignKeyFails(string col, string refTbl);
    /// <summary>
    /// Cria a exceção apropriada para linha referenciada por outra tabela.
    /// </summary>
    /// <param name="tbl">Tabela referenciada.</param>
    Exception ReferencedRow(string tbl);
}
