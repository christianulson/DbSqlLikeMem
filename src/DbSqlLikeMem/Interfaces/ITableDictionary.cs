namespace DbSqlLikeMem;

/// <summary>
/// Define um dicionário de tabelas acessível por nome.
/// </summary>
public interface ITableDictionary : IDictionary<string, ITableMock>
{ }
