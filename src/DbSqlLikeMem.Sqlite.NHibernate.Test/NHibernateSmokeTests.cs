namespace DbSqlLikeMem.Sqlite.NHibernate.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.SQLiteDialect, NHibernate";

    protected override string NhDriverClass => typeof(SqliteNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new SqliteConnectionMock([]);
        connection.Open();
        return connection;
    }
}
