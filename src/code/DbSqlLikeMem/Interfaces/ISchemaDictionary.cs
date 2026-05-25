namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines a read-only dictionary of schemas by name.
/// PT-br: Define um dicionário somente leitura de schemas por nome.
/// </summary>
public interface ISchemaDictionary : IReadOnlyDictionary<string, ISchemaMock>
{ }
