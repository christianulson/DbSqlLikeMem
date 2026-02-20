using System.Diagnostics.CodeAnalysis;

namespace DbSqlLikeMem;
/// <summary>
/// EN: Represents a query result as a list of rows indexed by column, used to transport
/// data returned by in-memory SQL execution.
/// PT: Representa o resultado de uma consulta como uma lista de linhas indexadas por coluna,
/// usado para transportar dados retornados pela execução do SQL em memória.
/// </summary>
public class TableResultMock : List<Dictionary<int, object?>>
{
    /// <summary>
    /// EN: Textual execution plan generated for the query that produced this result.
    /// PT: Plano de execução textual gerado para a consulta que produziu este resultado.
    /// </summary>
    public string? ExecutionPlan { get; internal set; }

    /// <summary>
    /// EN: Defines the list of columns present in the result, with metadata.
    /// PT: Define a lista de colunas presentes no resultado, com seus metadados.
    /// </summary>
    public IList<TableResultColMock> Columns
    { get; internal set; } = [];

    /// <summary>
    /// EN: Holds auxiliary fields used in joins to compose combined results.
    /// PT: Mantém os campos auxiliares usados em joins para compor resultados combinados.
    /// </summary>
    public IList<Dictionary<string, object?>> JoinFields { get; internal set; } = [];

    /// <summary>
    /// EN: Gets the column index by alias or name and throws if not found.
    /// PT: Obtém o índice da coluna pelo alias ou nome e lança exceção se não encontrar.
    /// </summary>
    /// <param name="col">EN: Column name or alias to locate. PT: Nome ou alias da coluna a localizar.</param>
    /// <returns>EN: Column index in the result. PT: Índice da coluna no resultado.</returns>
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
/// EN: Describes metadata for a column inside a query result.
/// PT: Describe metadados de uma coluna dentro de um resultado de consulta.
/// </summary>
public class TableResultColMock
{
    /// <summary>
    /// EN: Initializes column metadata with alias, name, and type.
    /// PT: Inicializa um metadado de coluna com alias, nome e tipo.
    /// </summary>
    /// <param name="tableAlias">EN: Table alias in the result. PT: Alias da tabela no resultado.</param>
    /// <param name="columnAlias">EN: Column alias in the result. PT: Alias da coluna no resultado.</param>
    /// <param name="columnName">EN: Real column name. PT: Nome real da coluna.</param>
    /// <param name="columIndex">EN: Column position in the result. PT: Posição da coluna no resultado.</param>
    /// <param name="dbType">EN: Data type in the result. PT: Tipo do dado no resultado.</param>
    /// <param name="isNullable">EN: Whether the column accepts nulls. PT: Indica se a coluna aceita valores nulos.</param>
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
    /// EN: Table alias associated with the column.
    /// PT: Alias da tabela associado à coluna.
    /// </summary>
    public required string TableAlias { get; set; }
    /// <summary>
    /// EN: Column alias in the result.
    /// PT: Alias da coluna no resultado.
    /// </summary>
    public required string ColumnAlias { get; set; }
    /// <summary>
    /// EN: Real column name in the source table.
    /// PT: Nome real da coluna na tabela de origem.
    /// </summary>
    public required string ColumnName { get; set; }
    /// <summary>
    /// EN: Column index in the result set.
    /// PT: Índice da coluna no conjunto de resultados.
    /// </summary>
    public int ColumIndex { get; set; }
    /// <summary>
    /// EN: Data type returned by the column.
    /// PT: Tipo do dado retornado pela coluna.
    /// </summary>
    public DbType DbType { get; set; }
    /// <summary>
    /// EN: Indicates whether the column value can be null.
    /// PT: Indica se o valor da coluna pode ser nulo.
    /// </summary>
    public bool IsNullable { get; set; }
}
