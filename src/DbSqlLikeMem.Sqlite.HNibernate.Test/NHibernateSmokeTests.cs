using System.Data.Common;

namespace DbSqlLikeMem.Sqlite.HNibernate.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.SQLiteDialect, NHibernate";

    protected override string NhDriverClass => typeof(SqliteNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock(new SqliteDbMock());
        connection.Open();
        return connection;
    }
}
