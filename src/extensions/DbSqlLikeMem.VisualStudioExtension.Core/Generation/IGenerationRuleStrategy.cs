namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// Defines database-type mapping rules for code generation.
/// Define regras de mapeamento de tipo de banco para geracao de codigo.
/// </summary>
public interface IGenerationRuleStrategy
{
    /// <summary>
    /// Maps a database column type to a C# type name.
    /// Mapeia um tipo de coluna do banco para um nome de tipo C#.
    /// </summary>
    string MapDbType(GenerationTypeContext context);
}

/// <summary>
/// Provides type mapping context for a database column.
/// Fornece o contexto de mapeamento de tipo para uma coluna do banco.
/// </summary>
public readonly record struct GenerationTypeContext
{
    /// <summary>
    /// Initializes a new generation type context.
    /// Inicializa um novo contexto de tipo para geracao.
    /// </summary>
    public GenerationTypeContext(string dataType, long? charMaxLen, int? numPrecision, int? numScale, string columnName)
    {
        DataType = dataType;
        CharMaxLen = charMaxLen;
        NumPrecision = numPrecision;
        NumScale = numScale;
        ColumnName = columnName;
    }

    /// <summary>
    /// Gets the database data type name.
    /// Obtem o nome do tipo de dado do banco.
    /// </summary>
    public string DataType { get; }

    /// <summary>
    /// Gets the character max length when available.
    /// Obtem o tamanho maximo de caracteres quando disponivel.
    /// </summary>
    public long? CharMaxLen { get; }

    /// <summary>
    /// Gets numeric precision when available.
    /// Obtem a precisao numerica quando disponivel.
    /// </summary>
    public int? NumPrecision { get; }

    /// <summary>
    /// Gets numeric scale when available.
    /// Obtem a escala numerica quando disponivel.
    /// </summary>
    public int? NumScale { get; }

    /// <summary>
    /// Gets the source column name.
    /// Obtem o nome da coluna de origem.
    /// </summary>
    public string ColumnName { get; }
}
