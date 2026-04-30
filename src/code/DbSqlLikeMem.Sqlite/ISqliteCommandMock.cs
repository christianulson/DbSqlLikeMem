namespace DbSqlLikeMem.Sqlite;

internal interface ISqliteCommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
}
