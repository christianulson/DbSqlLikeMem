using DbSqlLikeMem.Npgsql.NHibernate;
using System.Data.Common;

namespace DbSqlLikeMem.Npgsql.Test;

public sealed class NHibernateSmokeTests : DbSqlLikeMem.Test.NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.PostgreSQL83Dialect, NHibernate";

    protected override string NhDriverClass => typeof(NpgsqlNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new NpgsqlConnectionMock(new NpgsqlDbMock());
        connection.Open();
        return connection;
    }
}
