using System.Data.Common;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using Environment = NHibernate.Cfg.Environment;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Shared NHibernate integration contract tests for provider mock connections.
/// PT: Testes de contrato de integração NHibernate compartilhados para conexões mock por provedor.
/// </summary>
public abstract class NHibernateSupportTestsBase
{
    /// <summary>
    /// EN: NHibernate dialect class full name used by this provider contract run.
    /// PT: Nome completo da classe de dialeto do NHibernate usada nesta execução por provedor.
    /// </summary>
    protected abstract string NhDialectClass { get; }

    /// <summary>
    /// EN: Creates and opens a provider-specific mock connection.
    /// PT: Cria e abre uma conexão mock específica de provedor.
    /// </summary>
    protected abstract DbConnection CreateOpenConnection();

    /// <summary>
    /// EN: Optional NHibernate driver class for provider-specific mocked connections.
    /// PT: Classe opcional de driver NHibernate para conexões mock específicas do provedor.
    /// </summary>
    protected virtual string? NhDriverClass => null;

    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_NativeSql_WithParameter_ShouldReturnExpectedRow()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "INSERT INTO users (id, name) VALUES (1, 'Alice')");

        using var sessionFactory = BuildConfiguration().BuildSessionFactory();
        using var session = sessionFactory.WithOptions().Connection(connection).OpenSession();

        var rows = session
            .CreateSQLQuery("SELECT name FROM users WHERE id = :id")
            .SetParameter("id", 1)
            .List();

        Assert.Single(rows);
        Assert.Equal("Alice", rows[0]);
    }

    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_SaveAndGet_ShouldWork()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 2, Name = "Bob" });
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var loaded = verifySession.Get<NhTestUser>(2);

        Assert.NotNull(loaded);
        Assert.Equal("Bob", loaded!.Name);
    }

    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_Update_ShouldPersistChanges()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 4, Name = "Before" });
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var user = session.Get<NhTestUser>(4);
            Assert.NotNull(user);
            user!.Name = "After";
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var updated = verifySession.Get<NhTestUser>(4);

        Assert.NotNull(updated);
        Assert.Equal("After", updated!.Name);
    }

    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_TransactionRollback_ShouldDiscardChanges()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration().BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session
                .CreateSQLQuery("INSERT INTO users (id, name) VALUES (:id, :name)")
                .SetParameter("id", 3)
                .SetParameter("name", "Rollback")
                .ExecuteUpdate();

            tx.Rollback();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var count = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) FROM users WHERE id = :id")
                .SetParameter("id", 3)
                .UniqueResult());

        Assert.Equal(0, count);
    }

    private Configuration BuildConfiguration(bool withMappings = false)
    {
        var configuration = new Configuration();
        configuration.SetProperty(Environment.Dialect, NhDialectClass);
        configuration.SetProperty(Environment.ConnectionProvider, typeof(UserSuppliedConnectionProvider).AssemblyQualifiedName!);
        configuration.SetProperty(Environment.ReleaseConnections, "on_close");
        if (!string.IsNullOrWhiteSpace(NhDriverClass))
            configuration.SetProperty(Environment.ConnectionDriver, NhDriverClass);

        if (withMappings)
        {
            var mapper = new ModelMapper();
            mapper.AddMapping<NhTestUserMap>();
            configuration.AddMapping(mapper.CompileMappingForAllExplicitlyAddedEntities());
        }

        return configuration;
    }

    private static void ExecuteNonQuery(DbConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        _ = command.ExecuteNonQuery();
    }

    private class NhTestUser
    {
        public virtual int Id { get; set; }

        public virtual string Name { get; set; } = string.Empty;
    }

    private sealed class NhTestUserMap : ClassMapping<NhTestUser>
    {
        public NhTestUserMap()
        {
            Table("users");

            Id(x => x.Id, map =>
            {
                map.Column("id");
                map.Generator(Generators.Assigned);
            });

            Property(x => x.Name, map =>
            {
                map.Column("name");
                map.NotNullable(true);
            });
        }
    }
}
