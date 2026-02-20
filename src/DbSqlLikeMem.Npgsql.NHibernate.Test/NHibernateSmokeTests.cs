namespace DbSqlLikeMem.Npgsql.NHibernate.Test;

public sealed class NHibernateSmokeTests : NHibernateSupportTestsBase
{
    protected override string NhDialectClass => "NHibernate.Dialect.PostgreSQL83Dialect, NHibernate";

    protected override string NhDriverClass => typeof(NpgsqlNhMockDriver).AssemblyQualifiedName!;

    protected override DbConnection CreateOpenConnection()
    {
        var connection = new NpgsqlConnectionMock([]);
        connection.Open();
        return connection;
    }
}
