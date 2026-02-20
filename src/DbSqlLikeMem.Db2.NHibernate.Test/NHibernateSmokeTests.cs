using System.Data.Common;

namespace DbSqlLikeMem.Db2.NHibernate.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.DB2Dialect, NHibernate";

    protected override string NhDriverClass => typeof(Db2NhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock(new Db2DbMock());
        connection.Open();
        return connection;
    }
}
