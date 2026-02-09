namespace DbSqlLikeMem;

/// <summary>
/// Define os metadados de uma coluna, incluindo tipo, nulabilidade e identidade.
/// </summary>
public sealed class ColumnDef
{
    /// <summary>
    /// Inicializa uma definição de coluna vazia.
    /// </summary>
    public ColumnDef()
    { }

    /// <summary>
    /// Inicializa uma coluna com valores completos, incluindo padrão.
    /// </summary>
    /// <param name="index">Posição da coluna.</param>
    /// <param name="dbType">Tipo de dados.</param>
    /// <param name="nullable">Indica se aceita nulos.</param>
    /// <param name="identity">Indica se é identidade.</param>
    /// <param name="defaultValue">Valor padrão.</param>
    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable,
        bool identity,
        object? defaultValue
    ) : this(index, dbType, nullable, identity)
    {
        DefaultValue = defaultValue;
    }

    /// <summary>
    /// Inicializa uma coluna com identidade e configuração de nulabilidade.
    /// </summary>
    /// <param name="index">Posição da coluna.</param>
    /// <param name="dbType">Tipo de dados.</param>
    /// <param name="nullable">Indica se aceita nulos.</param>
    /// <param name="identity">Indica se é identidade.</param>
    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable,
        bool identity
    ) : this(index, dbType, nullable)
    {
        Identity = identity;
    }

    /// <summary>
    /// Inicializa uma coluna não nula com índice e tipo.
    /// </summary>
    /// <param name="index">Posição da coluna.</param>
    /// <param name="dbType">Tipo de dados.</param>
    public ColumnDef(
        int index,
        DbType dbType
    ) : this(index, dbType, false) 
    { }

    /// <summary>
    /// Inicializa uma coluna com índice, tipo e nulabilidade.
    /// </summary>
    /// <param name="index">Posição da coluna.</param>
    /// <param name="dbType">Tipo de dados.</param>
    /// <param name="nullable">Indica se aceita nulos.</param>
    public ColumnDef(
        int index,
        DbType dbType,
        bool nullable)
    {
        Index = index;
        DbType = dbType;
        Nullable = nullable;
    }

    /// <summary>
    /// Obtém a posição da coluna na tabela.
    /// </summary>
    public int Index { get; init; }
    /// <summary>
    /// Obtém o tipo de dados da coluna.
    /// </summary>
    public DbType DbType { get; init; }
    /// <summary>
    /// Indica se a coluna aceita valores nulos.
    /// </summary>
    public bool Nullable { get; init; }
    /// <summary>
    /// Indica se a coluna é auto incrementável.
    /// </summary>
    public bool Identity { get; set; }
    /// <summary>
    /// Obtém ou define o valor padrão da coluna.
    /// </summary>
    public object? DefaultValue { get; set; }

    /// <summary>
    /// Lista de valores permitidos quando a coluna é um enum.
    /// </summary>
    public HashSet<string> EnumValues { get; internal set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// Define os valores permitidos para a coluna enum.
    /// </summary>
    /// <param name="values">Valores permitidos.</param>
    public void SetEnumValues(params string[] values) => EnumValues = new(values, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Função geradora de valor calculado para colunas derivadas.
    /// </summary>
    public Func<Dictionary<int, object?>, ITableMock, object?>? GetGenValue { get; set; }
}
