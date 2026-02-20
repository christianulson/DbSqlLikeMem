using System.Data.Common;

namespace DbSqlLikeMem.MySql.Test;

public sealed class NHibernateSmokeTests : DbSqlLikeMem.Test.NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.MySQLDialect, NHibernate";

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new MySqlConnectionMock(new MySqlDbMock());
        connection.Open();
        return connection;
    }
}
