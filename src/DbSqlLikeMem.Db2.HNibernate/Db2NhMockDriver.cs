namespace DbSqlLikeMem.Db2.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem DB2 mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock DB2 do DbSqlLikeMem.
/// </summary>
public sealed class Db2NhMockDriver : ReflectionBasedDriver
{
    public Db2NhMockDriver()
        : base(
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Db2",
            "DbSqlLikeMem.Db2.Db2ConnectionMock",
            "DbSqlLikeMem.Db2.Db2CommandMock")
    {
    }
}
