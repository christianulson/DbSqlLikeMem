namespace DbSqlLikeMem;

/// <summary>
/// Define um dicionário somente leitura de schemas por nome.
/// </summary>
public interface ISchemaDictionary : IReadOnlyDictionary<string, ISchemaMock>
{ }
