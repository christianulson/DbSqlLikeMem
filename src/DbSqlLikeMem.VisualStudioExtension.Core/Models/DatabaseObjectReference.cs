namespace DbSqlLikeMem.VisualStudioExtension.Core.Models;

/// <summary>
/// Represents a database object reference.
/// Representa uma referência a objeto de banco de dados.
/// </summary>
public sealed record DatabaseObjectReference
{
    /// <summary>
    /// Initializes a new object reference.
    /// Inicializa uma nova referência de objeto.
    /// </summary>
    public DatabaseObjectReference(string schema, string name, DatabaseObjectType type, IReadOnlyDictionary<string, string>? properties = null)
    {
        Schema = schema;
        Name = name;
        Type = type;
        Properties = properties;
    }

    /// <summary>
    /// Gets the schema name.
    /// Obtém o nome do schema.
    /// </summary>
    public string Schema { get; }

    /// <summary>
    /// Gets the object name.
    /// Obtém o nome do objeto.
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the database object type.
    /// Obtém o tipo do objeto de banco.
    /// </summary>
    public DatabaseObjectType Type { get; }

    /// <summary>
    /// Gets optional metadata properties.
    /// Obtém propriedades de metadados opcionais.
    /// </summary>
    public IReadOnlyDictionary<string, string>? Properties { get; init; }

    /// <summary>
    /// Gets the fully qualified object name.
    /// Obtém o nome completo do objeto.
    /// </summary>
    public string FullName => string.IsNullOrWhiteSpace(Schema) ? Name : $"{Schema}.{Name}";
}
