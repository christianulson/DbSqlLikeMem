namespace DbSqlLikeMem;

/// <summary>
/// EN: Temporary column definition record used during parsing.
/// PT-br: Registro temporario de definicao de coluna usado durante o parsing.
/// </summary>
/// <param name="name">EN: Column name. PT-br: Nome da coluna.</param>
/// <param name="dbType">EN: Data type. PT-br: Tipo de dados.</param>
/// <param name="nullable">EN: Accepts nulls. PT-br: Aceita nulos.</param>
/// <param name="size">EN: Optional size. PT-br: Tamanho opcional.</param>
/// <param name="decimalPlaces">EN: Optional decimal places. PT-br: Casas decimais opcionais.</param>
/// <param name="identity">EN: Identity flag. PT-br: Indicador de identidade.</param>
/// <param name="defaultValue">EN: Default value. PT-br: Valor padrao.</param>
/// <param name="enumValues">EN: Enum values. PT-br: Valores enum.</param>
/// <param name="computedExpression">EN: Optional computed expression text. PT-br: Texto opcional da expressao computada.</param>
public sealed record Col(
    string name,
    DbType dbType,
    bool nullable,
    int? size = null,
    int? decimalPlaces = null,
    bool identity = false,
    object? defaultValue = null,
    IList<string>? enumValues = null,
    string? computedExpression = null
);

/// <summary>
/// EN: Defines column metadata including type, nullability, and identity.
/// PT-br: Define os metadados de uma coluna, incluindo tipo, nulabilidade e identidade.
/// </summary>
public sealed class ColumnDef
{
    /// <summary>
    /// EN: Initializes a column with index, type, and nullability.
    /// PT-br: Inicializa uma coluna com índice, tipo e nulabilidade.
    /// </summary>
    /// <param name="table">EN: Parent table. PT-br: Tabela pai.</param>
    /// <param name="name">EN: Column name. PT-br: Nome da coluna.</param>
    /// <param name="index">EN: Column position. PT-br: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT-br: Tipo de dados.</param>
    /// <param name="nullable">EN: Whether it accepts nulls. PT-br: Indica se aceita nulos.</param>
    /// <param name="size">EN: Optional column size. PT-br: Tamanho opcional da coluna.</param>
    /// <param name="decimalPlaces">EN: Optional decimal places. PT-br: Casas decimais opcionais.</param>
    /// <param name="identity">EN: Identity flag. PT-br: Indicador de identidade.</param>
    /// <param name="defaultValue">EN: Optional default value. PT-br: Valor padrão opcional.</param>
    /// <param name="enumValues">EN: Optional enum values. PT-br: Valores de enum opcionais.</param>
    /// <param name="computedExpression">EN: Optional computed expression text. PT-br: Texto opcional da expressao computada.</param>
    internal ColumnDef(
        ITableMock table,
        string name,
        int index,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null,
        string? computedExpression = null)
    {
        if (dbType == DbType.String
            && size <= 0)
            throw new InvalidOperationException(SqlExceptionMessages.StringColumnSizeRequired());
        if ((dbType == DbType.Currency
                || dbType == DbType.Decimal
                // || dbType == DbType.Double
                )
            && !decimalPlaces.HasValue)
            throw new InvalidOperationException(SqlExceptionMessages.DecimalPlacesRequiredForDbType(dbType));

        Table = table;
        Name = name;
        Index = index;
        DbType = dbType;
        Nullable = nullable;

        Size = size;
        DecimalPlaces = decimalPlaces;
        Identity = identity;
        DefaultValue = defaultValue;
        ComputedExpression = computedExpression;
        enumValues ??= [];
        EnumValues = new HashSet<string>(
                    enumValues.Select(v => v.Trim()),
                    StringComparer.OrdinalIgnoreCase);
        if (enumValues.Count != EnumValues.Count)
            throw new InvalidOperationException(SqlExceptionMessages.DuplicateEnumItems());
    }

    /// <summary>
    /// EN: Parent table.
    /// PT-br: Tabela pai.
    /// </summary>
    public ITableMock Table { get; private set; }

    /// <summary>
    /// EN: Column name.
    /// PT-br: Nome da coluna.
    /// </summary>
    public string Name { get; private set; }

    /// <summary>
    /// EN: Gets the column position within the table.
    /// PT-br: Obtém a posição da coluna na tabela.
    /// </summary>
    public int Index { get; init; }

    /// <summary>
    /// EN: Gets the column data type.
    /// PT-br: Obtém o tipo de dados da coluna.
    /// </summary>
    public DbType DbType { get; init; }

    /// <summary>
    /// EN: Indicates whether the column accepts null values.
    /// PT-br: Indica se a coluna aceita valores nulos.
    /// </summary>
    public bool Nullable { get; init; }

    /// <summary>
    /// EN: Maximum size/length for character/binary columns.
    /// PT-br: Tamanho máximo para colunas de caracteres/binárias.
    /// </summary>
    public int? Size { get; init; }

    /// <summary>
    /// EN: Maximum number of decimal places for numeric columns.
    /// PT-br: Quantidade máxima de casas decimais para colunas numéricas.
    /// </summary>
    public int? DecimalPlaces { get; init; }

    /// <summary>
    /// EN: Indicates whether the column is auto-increment.
    /// PT-br: Indica se a coluna é auto incrementável.
    /// </summary>
    public bool Identity { get; private set; }

    /// <summary>
    /// EN: Gets or sets the column default value.
    /// PT-br: Obtém ou define o valor padrão da coluna.
    /// </summary>
    public object? DefaultValue { get; private set; }

    /// <summary>
    /// EN: Optional computed expression text captured for the column.
    /// PT-br: Texto opcional da expressao computada capturado para a coluna.
    /// </summary>
    public string? ComputedExpression { get; private set; }

    /// <summary>
    /// EN: Allowed values when the column is an enum.
    /// PT-br: Lista de valores permitidos quando a coluna é um enum.
    /// </summary>
    public HashSet<string> EnumValues { get; private set; }

    /// <summary>
    /// EN: Sets the allowed values for the enum column.
    /// PT-br: Define os valores permitidos para a coluna enum.
    /// </summary>
    /// <param name="values">EN: Allowed values. PT-br: Valores permitidos.</param>
    public void SetEnumValues(params string[] values) => EnumValues = new(values, StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// EN: Value generator for computed/derived columns.
    /// PT-br: Função geradora de valor calculado para colunas derivadas.
    /// </summary>
    public Func<IReadOnlyDictionary<int, object?>, ITableMock, object?>? GetGenValue
    {
        get => _getGenValue;
        set
        {
            _getGenValue = value;
            MetadataChanged?.Invoke();
        }
    }

    private Func<IReadOnlyDictionary<int, object?>, ITableMock, object?>? _getGenValue;

    /// <summary>
    /// EN: When true, stores the generated value in the row and keeps it updated on writes.
    /// PT-br: Quando true, armazena o valor calculado na linha e o mantém atualizado em escritas.
    /// </summary>
    public bool PersistComputedValue
    {
        get => _persistComputedValue;
        set
        {
            _persistComputedValue = value;
            MetadataChanged?.Invoke();
        }
    }

    private bool _persistComputedValue;

    internal Action? MetadataChanged { get; set; }
}
