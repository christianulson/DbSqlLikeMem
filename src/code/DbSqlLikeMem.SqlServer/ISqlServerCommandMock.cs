namespace DbSqlLikeMem.SqlServer;

internal interface ISqlServerCommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
}
