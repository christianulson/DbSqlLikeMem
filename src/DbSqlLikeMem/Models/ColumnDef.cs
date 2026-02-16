namespace DbSqlLikeMem;

public sealed record Col(
    string name,
    DbType dbType,
    bool nullable,
    int? size = null,
    int? decimalPlaces = null,
    bool identity = false,
    object? defaultValue = null,
    IList<string>? enumValues = null
);

/// <summary>
/// EN: Defines column metadata including type, nullability, and identity.
/// PT: Define os metadados de uma coluna, incluindo tipo, nulabilidade e identidade.
/// </summary>
public sealed class ColumnDef
{
    /// <summary>
    /// EN: Initializes a column with index, type, and nullability.
    /// PT: Inicializa uma coluna com índice, tipo e nulabilidade.
    /// </summary>
    /// <param name="table"></param>
    /// <param name="index">EN: Column position. PT: Posição da coluna.</param>
    /// <param name="dbType">EN: Data type. PT: Tipo de dados.</param>
    /// <param name="nullable">EN: Whether it accepts nulls. PT: Indica se aceita nulos.</param>
    /// <param name="size"></param>
    /// <param name="decimalPlaces"></param>
    /// <param name="identity"></param>
    /// <param name="defaultValue"></param>
    /// <param name="enumValues"></param>
    public ColumnDef(
        ITableMock table,
        int index,
        DbType dbType,
        bool nullable,
        int? size = null,
        int? decimalPlaces = null,
        bool identity = false,
        object? defaultValue = null,
        IList<string>? enumValues = null)
    {
        if (dbType == DbType.String
            && size <= 0)
            throw new InvalidOperationException("Tamanho do campo é obrigatório para o tipo String");
        if ((dbType == DbType.Currency
                || dbType == DbType.Decimal
                || dbType == DbType.Double)
            && !decimalPlaces.HasValue)
            throw new InvalidOperationException($"DbType {dbType} é obrigatório informafar decimalPlaces");

        Table = table;
        Index = index;
        DbType = dbType;
        Nullable = nullable;

        Size = size;
        DecimalPlaces = decimalPlaces;
        Identity = identity;
        DefaultValue = defaultValue;
        enumValues ??= [];
        EnumValues = new HashSet<string>(
                    enumValues.Select(v => v.Trim()),
                    StringComparer.OrdinalIgnoreCase);
        if (enumValues.Count != EnumValues.Count)
            throw new InvalidOperationException("Existem itens de enum duplicados");
    }

    /// <summary>
    /// EN: Parent table.
    /// PT: Tabela pai.
    /// </summary>
    public ITableMock Table { get; private set; }

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
    /// EN: Maximum size/length for character/binary columns.
    /// PT: Tamanho máximo para colunas de caracteres/binárias.
    /// </summary>
    public int? Size { get; init; }

    /// <summary>
    /// EN: Maximum number of decimal places for numeric columns.
    /// PT: Quantidade máxima de casas decimais para colunas numéricas.
    /// </summary>
    public int? DecimalPlaces { get; init; }

    /// <summary>
    /// EN: Indicates whether the column is auto-increment.
    /// PT: Indica se a coluna é auto incrementável.
    /// </summary>
    public bool Identity { get; private set; }

    /// <summary>
    /// EN: Gets or sets the column default value.
    /// PT: Obtém ou define o valor padrão da coluna.
    /// </summary>
    public object? DefaultValue { get; private set; }

    /// <summary>
    /// EN: Allowed values when the column is an enum.
    /// PT: Lista de valores permitidos quando a coluna é um enum.
    /// </summary>
    public HashSet<string> EnumValues { get; private set; }

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
    public Func<IReadOnlyDictionary<int, object?>, ITableMock, object?>? GetGenValue { get; set; }

    /// <summary>
    /// EN: When true, stores the generated value in the row and keeps it updated on writes.
    /// PT: Quando true, armazena o valor calculado na linha e o mantém atualizado em escritas.
    /// </summary>
    public bool PersistComputedValue { get; set; }
}
