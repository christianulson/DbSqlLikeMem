using DbSqlLikeMem.SqlServer.HNibernate;
using System.Data.Common;

namespace DbSqlLikeMem.SqlServer.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.MsSql2012Dialect, NHibernate";

    protected override string NhDriverClass => typeof(SqlServerNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqlServerConnectionMock(new SqlServerDbMock());
        connection.Open();
        return connection;
    }
}
