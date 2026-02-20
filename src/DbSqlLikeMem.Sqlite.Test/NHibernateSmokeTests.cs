using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.Test;

public sealed class NHibernateSmokeTests : DbSqlLikeMem.Test.NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.SQLiteDialect, NHibernate";

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock(new SqliteDbMock());
        connection.Open();
        return connection;
    }
}
