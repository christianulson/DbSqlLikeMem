namespace DbSqlLikeMem.Npgsql.HNibernate;

/// <summary>
/// EN: NHibernate driver bound to DbSqlLikeMem Npgsql mock ADO.NET types.
/// PT: Driver NHibernate ligado aos tipos ADO.NET mock Npgsql do DbSqlLikeMem.
/// </summary>
public sealed class NpgsqlNhMockDriver : ReflectionBasedDriver
{
    public NpgsqlNhMockDriver()
        : base(
            "DbSqlLikeMem.Npgsql",
            "DbSqlLikeMem.Npgsql",
            "DbSqlLikeMem.Npgsql.NpgsqlConnectionMock",
            "DbSqlLikeMem.Npgsql.NpgsqlCommandMock")
    {
    }
}
