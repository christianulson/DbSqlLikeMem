namespace DbSqlLikeMem.SqlServer.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem SqlServer mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock SqlServer do DbSqlLikeMem.
/// </summary>
public sealed class SqlServerNhMockDriver : ReflectionBasedDriver
{
    public SqlServerNhMockDriver()
        : base(
            "DbSqlLikeMem.SqlServer",
            "DbSqlLikeMem.SqlServer",
            "DbSqlLikeMem.SqlServer.SqlServerConnectionMock",
            "DbSqlLikeMem.SqlServer.SqlServerCommandMock")
    {
    }
}
