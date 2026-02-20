namespace DbSqlLikeMem.Oracle.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Oracle mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock Oracle do DbSqlLikeMem.
/// </summary>
public sealed class OracleNhMockDriver : ReflectionBasedDriver
{
    public OracleNhMockDriver()
        : base(
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Oracle",
            "DbSqlLikeMem.Oracle.OracleConnectionMock",
            "DbSqlLikeMem.Oracle.OracleCommandMock")
    {
    }

    public override bool UseNamedPrefixInSql => throw new NotImplementedException();

    public override bool UseNamedPrefixInParameter => throw new NotImplementedException();

    public override string NamedPrefix => throw new NotImplementedException();
}
