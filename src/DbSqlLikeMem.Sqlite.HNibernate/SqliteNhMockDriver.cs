namespace DbSqlLikeMem.Sqlite.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Sqlite mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock Sqlite do DbSqlLikeMem.
/// </summary>
public sealed class SqliteNhMockDriver : ReflectionBasedDriver
{
    public SqliteNhMockDriver()
        : base(
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.Sqlite",
            "DbSqlLikeMem.Sqlite.SqliteConnectionMock",
            "DbSqlLikeMem.Sqlite.SqliteCommandMock")
    {
    }
}
