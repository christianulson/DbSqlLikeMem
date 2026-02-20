namespace DbSqlLikeMem.Npgsql;

internal interface INpgsqlCommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
}
