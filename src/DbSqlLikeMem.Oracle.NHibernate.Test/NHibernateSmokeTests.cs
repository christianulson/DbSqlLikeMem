using DbSqlLikeMem.Oracle.NHibernate;

namespace DbSqlLikeMem.Oracle.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.Oracle10gDialect, NHibernate";

    protected override string NhDriverClass => typeof(OracleNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new OracleConnectionMock([]);
        connection.Open();
        return connection;
    }
}
