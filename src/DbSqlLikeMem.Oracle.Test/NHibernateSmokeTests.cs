using System.Data.Common;

namespace DbSqlLikeMem.Oracle.Test;

public sealed class NHibernateSmokeTests : DbSqlLikeMem.Test.NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.Oracle10gDialect, NHibernate";

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new OracleConnectionMock(new OracleDbMock());
        connection.Open();
        return connection;
    }
}
