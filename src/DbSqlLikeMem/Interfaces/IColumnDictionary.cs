namespace DbSqlLikeMem;
/// <summary>
/// Define um dicionário de colunas acessível por nome.
/// </summary>
public interface IColumnDictionary : IDictionary<string, ColumnDef>
{
}
