namespace DbSqlLikeMem.Oracle;

internal interface IOracleCommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
}
