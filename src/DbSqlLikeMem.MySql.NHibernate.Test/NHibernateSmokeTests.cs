namespace DbSqlLikeMem.MySql.NHibernate.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.MySQLDialect, NHibernate";

    protected override string NhDriverClass => typeof(MySqlNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new MySqlConnectionMock([]);
        connection.Open();
        return connection;
    }
}
