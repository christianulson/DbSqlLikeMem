namespace DbSqlLikeMem.Db2;

internal interface IDb2CommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
}
