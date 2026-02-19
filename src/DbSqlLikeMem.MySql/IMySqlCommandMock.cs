namespace DbSqlLikeMem.MySql;

internal interface IMySqlCommandMock
{
    string? CommandText { get; }
    CommandType CommandType { get; }
    bool AllowUserVariables { get; }
    CommandBehavior CommandBehavior { get; }
    MySqlParameterCollection? RawParameters { get; }
    MySqlAttributeCollection? RawAttributes { get; }
    //PreparedStatements? TryGetPreparedStatements();
    MySqlConnectionMock? Connection { get; }
    long LastInsertedId { get; }
    void SetLastInsertedId(long lastInsertedId);
    MySqlParameterCollection? OutParameters { get; set; }
    MySqlParameter? ReturnParameter { get; set; }
    //ICancellableCommand CancellableCommand { get; }
    //ILogger Logger { get; }
}