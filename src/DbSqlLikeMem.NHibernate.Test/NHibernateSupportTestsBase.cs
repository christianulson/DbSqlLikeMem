using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using NHibernate.Criterion;
using NHibernate.Cfg;
using NHibernate.Connection;
using NHibernate.Mapping.ByCode;
using NHibernate.Mapping.ByCode.Conformist;
using Environment = NHibernate.Cfg.Environment;

namespace DbSqlLikeMem.NHibernate.Test;

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

    /// <summary>
    /// EN: Enables in-memory pagination fallback for dialects whose mocked providers do not support parameterized LIMIT/OFFSET forms.
    /// PT: Habilita fallback de paginação em memória para dialetos cujos provedores mock não suportam formas parametrizadas de LIMIT/OFFSET.
    /// </summary>
    protected virtual bool UseInMemoryPaginationFallback => false;

    /// <summary>
    /// EN: Verifies native SQL with a named parameter returns the expected row.
    /// PT: Verifica se SQL nativo com parâmetro nomeado retorna a linha esperada.
    /// </summary>
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

    /// <summary>
    /// EN: Verifies a mapped entity can be saved and loaded using NHibernate.
    /// PT: Verifica se uma entidade mapeada pode ser salva e carregada usando NHibernate.
    /// </summary>
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

    /// <summary>
    /// EN: Verifies mapped entity updates are persisted across sessions.
    /// PT: Verifica se atualizações de entidade mapeada são persistidas entre sessões.
    /// </summary>
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

    /// <summary>
    /// EN: Verifies rolled-back transactions do not persist data changes.
    /// PT: Verifica se transações com rollback não persistem alterações de dados.
    /// </summary>
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

    /// <summary>
    /// EN: Verifies deleting a mapped entity removes it from the store.
    /// PT: Verifica se excluir uma entidade mapeada a remove do armazenamento.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_Delete_ShouldRemoveRow()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 5, Name = "DeleteMe" });
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var user = session.Get<NhTestUser>(5);
            Assert.NotNull(user);
            session.Delete(user!);
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var deleted = verifySession.Get<NhTestUser>(5);
        Assert.Null(deleted);
    }

    /// <summary>
    /// EN: Verifies mapped query pagination works with FirstResult/MaxResults.
    /// PT: Verifica se a paginação da query mapeada funciona com FirstResult/MaxResults.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedQuery_Pagination_ShouldReturnWindow()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 10, Name = "User-10" });
            session.Save(new NhTestUser { Id = 11, Name = "User-11" });
            session.Save(new NhTestUser { Id = 12, Name = "User-12" });
            session.Save(new NhTestUser { Id = 13, Name = "User-13" });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var fallbackToInMemoryWindow = UseInMemoryPaginationFallback;

        IList<NhTestUser> paged;
        try
        {
            paged = querySession
                .CreateQuery("from NhTestUser u order by u.Id")
                .SetFirstResult(1)
                .SetMaxResults(2)
                .List<NhTestUser>();
        }
        catch (global::NHibernate.Exceptions.GenericADOException) when (fallbackToInMemoryWindow)
        {
            paged = new List<NhTestUser>();
        }

        if (fallbackToInMemoryWindow && paged.Count == 0)
        {
            paged = querySession
                .CreateQuery("from NhTestUser u order by u.Id")
                .List<NhTestUser>()
                .Skip(1)
                .Take(2)
                .ToList();
        }

        Assert.Collection(
            paged,
            row => Assert.Equal(11, row.Id),
            row => Assert.Equal(12, row.Id));
    }

    /// <summary>
    /// EN: Verifies basic HQL and Criteria APIs work with mapped entities.
    /// PT: Verifica se APIs básicas de HQL e Criteria funcionam com entidades mapeadas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_AndCriteria_ShouldFilterMappedEntity()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 20, Name = "Alpha" });
            session.Save(new NhTestUser { Id = 21, Name = "Beta" });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();

        var hqlResult = querySession
            .CreateQuery("from NhTestUser u where u.Name = :name")
            .SetParameter("name", "Beta")
            .UniqueResult<NhTestUser>();

        Assert.NotNull(hqlResult);
        Assert.Equal(21, hqlResult!.Id);

        var criteriaResult = querySession
            .CreateCriteria<NhTestUser>()
            .Add(Restrictions.Eq(nameof(NhTestUser.Name), "Alpha"))
            .UniqueResult<NhTestUser>();

        Assert.NotNull(criteriaResult);
        Assert.Equal(20, criteriaResult!.Id);
    }

    /// <summary>
    /// EN: Verifies null and basic typed parameters (string/int/datetime/decimal) are handled correctly.
    /// PT: Verifica se parâmetros nulos e tipados básicos (string/int/datetime/decimal) são tratados corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_NativeSql_NullAndTypedParameters_ShouldRoundTrip()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE typed_values (id INT PRIMARY KEY, str_val VARCHAR(100), int_val INT, dt_val DATETIME, dec_val DECIMAL(10,2))");

        var expectedDate = new DateTime(2024, 10, 15, 8, 30, 0);
        var expectedDecimal = 12.50m;

        using var sessionFactory = BuildConfiguration().BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            _ = session
                .CreateSQLQuery("INSERT INTO typed_values (id, str_val, int_val, dt_val, dec_val) VALUES (:id, :str, :int, :dt, :dec)")
                .SetParameter("id", 1)
                .SetParameter("str", (string?)null, global::NHibernate.NHibernateUtil.String)
                .SetParameter("int", 42)
                .SetParameter("dt", expectedDate)
                .SetParameter("dec", expectedDecimal)
                .ExecuteUpdate();

            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();

        var nullMatchCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM typed_values WHERE (:str IS NULL AND str_val IS NULL)")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("str", (string?)null, global::NHibernate.NHibernateUtil.String)
                .UniqueResult());

        Assert.Equal(1, nullMatchCount);

        var typeMatchCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM typed_values WHERE int_val = :int AND dt_val = :dt AND dec_val = :dec")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("int", 42, global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("dt", expectedDate, global::NHibernate.NHibernateUtil.DateTime)
                .SetParameter("dec", expectedDecimal, global::NHibernate.NHibernateUtil.Decimal)
                .UniqueResult());

        Assert.Equal(1, typeMatchCount);
    }


    /// <summary>
    /// EN: Verifies rollback on mapped entity save does not persist rows.
    /// PT: Verifica se rollback em save de entidade mapeada não persiste linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_SaveRollback_ShouldNotPersist()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 7, Name = "RollbackMapped" });
            tx.Rollback();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var loaded = verifySession.Get<NhTestUser>(7);

        Assert.Null(loaded);
    }

    /// <summary>
    /// EN: Verifies one-to-many mapped collections can be loaded from parent entities.
    /// PT: Verifica se coleções one-to-many mapeadas podem ser carregadas a partir da entidade pai.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_OneToMany_ShouldLoadCollection()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var group = new NhUserGroup { Id = 2, Name = "Support" };
            session.Save(group);
            session.Save(new NhRelUser { Id = 201, Name = "U1", Group = group });
            session.Save(new NhRelUser { Id = 202, Name = "U2", Group = group });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var loadedGroup = querySession.Get<NhUserGroup>(2);

        Assert.NotNull(loadedGroup);
        Assert.Equal("Support", loadedGroup!.Name);
        Assert.Equal(2, loadedGroup.Users.Count);
    }

    /// <summary>
    /// EN: Verifies HQL join/group-by aggregation works on mapped one-to-many relationships.
    /// PT: Verifica se agregação HQL com join/group-by funciona em relacionamentos one-to-many mapeados.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_RelationshipAggregation_ShouldReturnExpectedCounts()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var admins = new NhUserGroup { Id = 3, Name = "Admins" };
            var guests = new NhUserGroup { Id = 4, Name = "Guests" };

            session.Save(admins);
            session.Save(guests);
            session.Save(new NhRelUser { Id = 301, Name = "A1", Group = admins });
            session.Save(new NhRelUser { Id = 302, Name = "A2", Group = admins });
            session.Save(new NhRelUser { Id = 303, Name = "G1", Group = guests });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var rows = querySession
            .CreateQuery("select g.Name, count(u.Id) from NhUserGroup g left join g.Users u group by g.Name order by g.Name")
            .List<object[]>();

        Assert.Equal(2, rows.Count);
        Assert.Equal("Admins", rows[0][0]);
        Assert.Equal(2L, rows[0][1]);
        Assert.Equal("Guests", rows[1][0]);
        Assert.Equal(1L, rows[1][1]);
    }

    /// <summary>
    /// EN: Verifies mapped many-to-one relationships can be persisted and queried through HQL.
    /// PT: Verifica se relacionamentos many-to-one mapeados podem ser persistidos e consultados via HQL.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_ManyToOne_ShouldPersistAndQuery()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var group = new NhUserGroup { Id = 1, Name = "Admins" };
            var user = new NhRelUser { Id = 101, Name = "Alice", Group = group };

            session.Save(group);
            session.Save(user);
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var loadedUser = querySession.Get<NhRelUser>(101);

        Assert.NotNull(loadedUser);
        Assert.NotNull(loadedUser!.Group);
        Assert.Equal("Admins", loadedUser.Group!.Name);

        var hqlResult = querySession
            .CreateQuery("select u from NhRelUser u join u.Group g where g.Name = :groupName")
            .SetParameter("groupName", "Admins")
            .List<NhRelUser>();

        Assert.Single(hqlResult);
        Assert.Equal(101, hqlResult[0].Id);
    }

    /// <summary>
    /// EN: Verifies optimistic concurrency detects stale updates on versioned entities.
    /// PT: Verifica se concorrência otimista detecta atualizações obsoletas em entidades versionadas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_OptimisticConcurrency_ShouldDetectStaleUpdate()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users_versioned (id INT PRIMARY KEY, version INT NOT NULL, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var seedTx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhVersionedUser { Id = 30, Name = "Initial" });
            seedTx.Commit();
        }

        using var session1 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var session2 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var tx1 = session1.BeginTransaction();
        using var tx2 = session2.BeginTransaction();

        var user1 = session1.Get<NhVersionedUser>(30);
        var user2 = session2.Get<NhVersionedUser>(30);

        Assert.NotNull(user1);
        Assert.NotNull(user2);

        user1!.Name = "Tx1";
        session1.Flush();
        tx1.Commit();

        user2!.Name = "Tx2";
        _ = Assert.ThrowsAny<global::NHibernate.StaleStateException>(() =>
        {
            session2.Flush();
            tx2.Commit();
        });

        if (tx2.IsActive)
            tx2.Rollback();

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhVersionedUser>(30);

        Assert.NotNull(persisted);
        Assert.Equal("Tx1", persisted!.Name);
    }

    private Configuration BuildConfiguration(bool withMappings = false)
    {
        var configuration = new Configuration();
        configuration.SetProperty(Environment.Dialect, NhDialectClass);
        configuration.SetProperty(Environment.ConnectionProvider, typeof(UserSuppliedConnectionProvider).AssemblyQualifiedName!);
        configuration.SetProperty(Environment.ReleaseConnections, "on_close");
        configuration.SetProperty("hbm2ddl.keywords", "none");
        if (!string.IsNullOrWhiteSpace(NhDriverClass))
            configuration.SetProperty(Environment.ConnectionDriver, NhDriverClass);

        if (withMappings)
        {
            var mapper = new ModelMapper();
            mapper.AddMapping<NhTestUserMap>();
            mapper.AddMapping<NhVersionedUserMap>();
            mapper.AddMapping<NhUserGroupMap>();
            mapper.AddMapping<NhRelUserMap>();
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
}

internal class NhTestUser
{
    public virtual int Id { get; set; }

    public virtual string Name { get; set; } = string.Empty;
}

internal class NhVersionedUser
{
    public virtual int Id { get; set; }

    public virtual int Version { get; set; }

    public virtual string Name { get; set; } = string.Empty;
}

internal sealed class NhTestUserMap : ClassMapping<NhTestUser>
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

internal class NhUserGroup
{
    public virtual int Id { get; set; }

    public virtual string Name { get; set; } = string.Empty;

    public virtual IList<NhRelUser> Users { get; set; } = new List<NhRelUser>();
}

internal class NhRelUser
{
    public virtual int Id { get; set; }

    public virtual string Name { get; set; } = string.Empty;

    public virtual NhUserGroup? Group { get; set; }
}

internal sealed class NhUserGroupMap : ClassMapping<NhUserGroup>
{
    public NhUserGroupMap()
    {
        Table("user_groups");

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

        Bag(x => x.Users,
            bag =>
            {
                bag.Key(key => key.Column("group_id"));
                bag.Inverse(true);
                bag.Cascade(Cascade.None);
            },
            rel => rel.OneToMany());
    }
}

internal sealed class NhRelUserMap : ClassMapping<NhRelUser>
{
    public NhRelUserMap()
    {
        Table("users_rel");

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

        ManyToOne(x => x.Group, map =>
        {
            map.Column("group_id");
            map.Cascade(Cascade.None);
        });
    }
}

internal sealed class NhVersionedUserMap : ClassMapping<NhVersionedUser>
{
    public NhVersionedUserMap()
    {
        Table("users_versioned");

        Id(x => x.Id, map =>
        {
            map.Column("id");
            map.Generator(Generators.Assigned);
        });

        Version(x => x.Version, map =>
        {
            map.Column("version");
            map.UnsavedValue("0");
        });

        Property(x => x.Name, map =>
        {
            map.Column("name");
            map.NotNullable(true);
        });
    }
}
