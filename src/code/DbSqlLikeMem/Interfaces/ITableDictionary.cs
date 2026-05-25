namespace DbSqlLikeMem;

/// <summary>
/// EN: Defines a table dictionary accessible by name.
/// PT-br: Define um dicionário de tabelas acessível por nome.
/// </summary>
public interface ITableDictionary : IDictionary<string, ITableMock>
{ }
