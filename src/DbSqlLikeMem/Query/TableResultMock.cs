using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;
/// <summary>
/// Representa o resultado de uma consulta como uma lista de linhas indexadas por coluna,
/// usado para transportar dados retornados pela execução do SQL em memória.
/// </summary>
public class TableResultMock : List<Dictionary<int, object?>>
{
    /// <summary>
    /// Define a lista de colunas presentes no resultado, com seus metadados.
    /// </summary>
    public IList<TableResultColMock> Columns
    { get; internal set; } = [];

    /// <summary>
    /// Mantém os campos auxiliares usados em joins para compor resultados combinados.
    /// </summary>
    public IList<Dictionary<string, object?>> JoinFields { get; internal set; } = [];

    /// <summary>
    /// Obtém o índice da coluna pelo alias ou nome e lança exceção se não encontrar.
    /// </summary>
    /// <param name="col">Nome ou alias da coluna a localizar.</param>
    /// <returns>Índice da coluna no resultado.</returns>
    public int GetColumnIndexOrThrow(string col)
    {
        var found = Columns.FirstOrDefault(c =>
            c.ColumnAlias.Equals(col, StringComparison.OrdinalIgnoreCase)
            || c.ColumnName.Equals(col, StringComparison.OrdinalIgnoreCase))
            ?? throw new InvalidOperationException($"Column '{col}' not found in subquery result.");
        return found.ColumIndex;
    }
}


/// <summary>
/// Describe metadados de uma coluna dentro de um resultado de consulta.
/// </summary>
public class TableResultColMock
{
    /// <summary>
    /// Inicializa um metadado de coluna com alias, nome e tipo.
    /// </summary>
    /// <param name="tableAlias">Alias da tabela no resultado.</param>
    /// <param name="columnAlias">Alias da coluna no resultado.</param>
    /// <param name="columnName">Nome real da coluna.</param>
    /// <param name="columIndex">Posição da coluna no resultado.</param>
    /// <param name="dbType">Tipo do dado no resultado.</param>
    /// <param name="isNullable">Indica se a coluna aceita valores nulos.</param>
    [SetsRequiredMembers]
    public TableResultColMock(
        string tableAlias,
        string columnAlias,
        string columnName,
        int columIndex,
        DbType dbType,
        bool isNullable
        )
    {
        TableAlias = tableAlias;
        ColumnAlias = columnAlias;
        ColumnName = columnName;
        ColumIndex = columIndex;
        DbType = dbType;
        IsNullable = isNullable;
    }

    /// <summary>
    /// Alias da tabela associado à coluna.
    /// </summary>
    public required string TableAlias { get; set; }
    /// <summary>
    /// Alias da coluna no resultado.
    /// </summary>
    public required string ColumnAlias { get; set; }
    /// <summary>
    /// Nome real da coluna na tabela de origem.
    /// </summary>
    public required string ColumnName { get; set; }
    /// <summary>
    /// Índice da coluna no conjunto de resultados.
    /// </summary>
    public int ColumIndex { get; set; }
    /// <summary>
    /// Tipo do dado retornado pela coluna.
    /// </summary>
    public DbType DbType { get; set; }
    /// <summary>
    /// Indica se o valor da coluna pode ser nulo.
    /// </summary>
    public bool IsNullable { get; set; }
}
