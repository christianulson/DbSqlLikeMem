namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines column metadata including type, nullability, and identity.
/// PT: Define os metadados de uma coluna, incluindo tipo, nulabilidade e identidade.
/// </summary>
public sealed class ColumnDef
{
    /// <summary>
    /// EN: Initializes an empty column definition.
    /// PT: Inicializa uma definição de coluna vazia.
    /// </summary>
    public ColumnDef()
    { }

    /// <summary>
    /// EN: Initializes a column with full values, including default.
    /// PT: Inicializa uma coluna com valores completos, incluindo padrão.
    /// </summary>
    /// <param name="index">EN: Column position. PT: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT: Tipo de dados.</param>
    /// <param name="nullable">EN: Whether it accepts nulls. PT: Indica se aceita nulos.</param>
    /// <param name="identity">EN: Whether it is an identity column. PT: Indica se é identidade.</param>
    /// <param name="defaultValue">EN: Default value. PT: Valor padrão.</param>
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
    /// EN: Initializes a column with identity and nullability.
    /// PT: Inicializa uma coluna com identidade e configuração de nulabilidade.
    /// </summary>
    /// <param name="index">EN: Column position. PT: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT: Tipo de dados.</param>
    /// <param name="nullable">EN: Whether it accepts nulls. PT: Indica se aceita nulos.</param>
    /// <param name="identity">EN: Whether it is an identity column. PT: Indica se é identidade.</param>
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
    /// EN: Initializes a non-null column with index and type.
    /// PT: Inicializa uma coluna não nula com índice e tipo.
    /// </summary>
    /// <param name="index">EN: Column position. PT: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT: Tipo de dados.</param>
    public ColumnDef(
        int index,
        DbType dbType
    ) : this(index, dbType, false) 
    { }

    /// <summary>
    /// EN: Initializes a column with index, type, and nullability.
    /// PT: Inicializa uma coluna com índice, tipo e nulabilidade.
    /// </summary>
    /// <param name="index">EN: Column position. PT: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT: Tipo de dados.</param>
    /// <param name="nullable">EN: Whether it accepts nulls. PT: Indica se aceita nulos.</param>
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
    /// EN: Gets the column position within the table.
    /// PT: Obtém a posição da coluna na tabela.
    /// </summary>
    public int Index { get; init; }
    /// <summary>
    /// EN: Gets the column data type.
    /// PT: Obtém o tipo de dados da coluna.
    /// </summary>
    public DbType DbType { get; init; }
    /// <summary>
    /// EN: Indicates whether the column accepts null values.
    /// PT: Indica se a coluna aceita valores nulos.
    /// </summary>
    public bool Nullable { get; init; }
    /// <summary>
    /// EN: Indicates whether the column is auto-increment.
    /// PT: Indica se a coluna é auto incrementável.
    /// </summary>
    public bool Identity { get; set; }
    /// <summary>
    /// EN: Gets or sets the column default value.
    /// PT: Obtém ou define o valor padrão da coluna.
    /// </summary>
    public object? DefaultValue { get; set; }

    /// <summary>
    /// EN: Allowed values when the column is an enum.
    /// PT: Lista de valores permitidos quando a coluna é um enum.
    /// </summary>
    public HashSet<string> EnumValues { get; internal set; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
    /// <summary>
    /// EN: Sets the allowed values for the enum column.
    /// PT: Define os valores permitidos para a coluna enum.
    /// </summary>
    /// <param name="values">EN: Allowed values. PT: Valores permitidos.</param>
    public void SetEnumValues(params string[] values) => EnumValues = new(values, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Value generator for computed/derived columns.
    /// PT: Função geradora de valor calculado para colunas derivadas.
    /// </summary>
    public Func<Dictionary<int, object?>, ITableMock, object?>? GetGenValue { get; set; }
}
