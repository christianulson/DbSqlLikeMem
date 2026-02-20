namespace DbSqlLikeMem.MySql.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem MySql mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock MySql do DbSqlLikeMem.
/// </summary>
public sealed class MySqlNhMockDriver : ReflectionBasedDriver
{
    public MySqlNhMockDriver()
        : base(
            "DbSqlLikeMem.MySql",
            "DbSqlLikeMem.MySql",
            "DbSqlLikeMem.MySql.MySqlConnectionMock",
            "DbSqlLikeMem.MySql.MySqlCommandMock")
    {
    }

    public override bool UseNamedPrefixInSql => throw new NotImplementedException();

    public override bool UseNamedPrefixInParameter => throw new NotImplementedException();

    public override string NamedPrefix => throw new NotImplementedException();
}
