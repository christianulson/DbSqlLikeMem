using System.Data.Common;

namespace DbSqlLikeMem.Db2.Test;

public sealed class NHibernateSmokeTests : DbSqlLikeMem.Test.NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.DB2Dialect, NHibernate";

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new Db2ConnectionMock(new Db2DbMock());
        connection.Open();
        return connection;
    }
}
