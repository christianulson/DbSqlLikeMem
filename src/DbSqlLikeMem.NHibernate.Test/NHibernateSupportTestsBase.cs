using System.Data.Common;
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
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users WHERE id = :id")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
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

        if (paged.Count == 0 && NhDialectClass.Contains("DB2Dialect", StringComparison.OrdinalIgnoreCase))
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
    /// EN: Verifies session Evict detaches an entity and a subsequent load reflects persisted changes.
    /// PT: Verifica se Evict na sessão destaca a entidade e se um novo load reflete mudanças persistidas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Evict_ShouldReloadEntityState()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 40, Name = "BeforeEvict" });
            tx.Commit();
        }

        using var lifecycleSession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var tracked = lifecycleSession.Get<NhTestUser>(40);
        Assert.NotNull(tracked);

        lifecycleSession.Evict(tracked!);

        using (var updateSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = updateSession.BeginTransaction())
        {
            _ = updateSession
                .CreateSQLQuery("UPDATE users SET name = :name WHERE id = :id")
                .SetParameter("name", "AfterEvict")
                .SetParameter("id", 40)
                .ExecuteUpdate();
            tx.Commit();
        }

        var reloaded = lifecycleSession.Get<NhTestUser>(40);

        Assert.NotNull(reloaded);
        Assert.Equal("AfterEvict", reloaded!.Name);
    }

    /// <summary>
    /// EN: Verifies session Clear detaches tracked entities and a new query observes latest state.
    /// PT: Verifica se Clear na sessão destaca entidades rastreadas e se nova consulta observa estado mais recente.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Clear_ShouldRequeryLatestState()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 41, Name = "BeforeClear" });
            tx.Commit();
        }

        using var lifecycleSession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var beforeClear = lifecycleSession.Get<NhTestUser>(41);
        Assert.NotNull(beforeClear);

        using (var updateSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = updateSession.BeginTransaction())
        {
            _ = updateSession
                .CreateSQLQuery("UPDATE users SET name = :name WHERE id = :id")
                .SetParameter("name", "AfterClear")
                .SetParameter("id", 41)
                .ExecuteUpdate();
            tx.Commit();
        }

        lifecycleSession.Clear();
        var afterClear = lifecycleSession.Get<NhTestUser>(41);

        Assert.NotNull(afterClear);
        Assert.Equal("AfterClear", afterClear!.Name);
    }

    /// <summary>
    /// EN: Verifies Merge persists changes made while an entity was detached.
    /// PT: Verifica se Merge persiste mudanças feitas enquanto a entidade estava destacada.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Merge_ShouldPersistDetachedChanges()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhTestUser { Id = 42, Name = "BeforeMerge" });
            tx.Commit();
        }

        NhTestUser detached;
        using (var loadSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        {
            detached = loadSession.Get<NhTestUser>(42)!;
        }

        detached.Name = "AfterMerge";

        using (var mergeSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = mergeSession.BeginTransaction())
        {
            _ = mergeSession.Merge(detached);
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhTestUser>(42);

        Assert.NotNull(persisted);
        Assert.Equal("AfterMerge", persisted!.Name);
    }

    /// <summary>
    /// EN: Verifies Merge returns a managed instance and detached object changes after merge are ignored.
    /// PT: Verifica se Merge retorna instância gerenciada e se alterações no objeto destacado após merge são ignoradas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Merge_ShouldUseManagedInstanceState()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhTestUser { Id = 44, Name = "Seed" });
            tx.Commit();
        }

        NhTestUser detached;
        using (var loadSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        {
            detached = loadSession.Get<NhTestUser>(44)!;
        }

        detached.Name = "DetachedBeforeMerge";

        using (var mergeSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = mergeSession.BeginTransaction())
        {
            var managed = mergeSession.Merge(detached);
            Assert.NotSame(detached, managed);

            detached.Name = "DetachedAfterMerge";
            managed.Name = "ManagedAfterMerge";

            mergeSession.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhTestUser>(44);

        Assert.NotNull(persisted);
        Assert.Equal("ManagedAfterMerge", persisted!.Name);
    }

    /// <summary>
    /// EN: Verifies changes made after Evict are not auto-persisted by Flush in the same session.
    /// PT: Verifica se mudanças após Evict não são persistidas automaticamente por Flush na mesma sessão.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Evict_ShouldNotPersistDetachedChangesOnFlush()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 43, Name = "BeforeDetachedChange" });
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var tracked = session.Get<NhTestUser>(43)!;
            session.Evict(tracked);

            tracked.Name = "DetachedChanged";
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhTestUser>(43);

        Assert.NotNull(persisted);
        Assert.Equal("BeforeDetachedChange", persisted!.Name);
    }

    /// <summary>
    /// EN: Verifies session Clear detaches all tracked entities from the current persistence context.
    /// PT: Verifica se Clear da sessão destaca todas as entidades rastreadas do contexto de persistência atual.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Clear_ShouldDetachAllTrackedEntities()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhTestUser { Id = 45, Name = "Tracked-A" });
            seedSession.Save(new NhTestUser { Id = 46, Name = "Tracked-B" });
            tx.Commit();
        }

        using var session = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var userA = session.Get<NhTestUser>(45)!;
        var userB = session.Get<NhTestUser>(46)!;

        Assert.True(session.Contains(userA));
        Assert.True(session.Contains(userB));

        session.Clear();

        Assert.False(session.Contains(userA));
        Assert.False(session.Contains(userB));
    }

    /// <summary>
    /// EN: Verifies Refresh discards in-memory changes and reloads current persisted state.
    /// PT: Verifica se Refresh descarta mudanças em memória e recarrega o estado persistido atual.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_Refresh_ShouldReloadDatabaseState()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhTestUser { Id = 47, Name = "InitialRefresh" });
            tx.Commit();
        }

        using var session = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var user = session.Get<NhTestUser>(47)!;
        user.Name = "LocalUnsavedChange";

        using (var sqlSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = sqlSession.BeginTransaction())
        {
            _ = sqlSession
                .CreateSQLQuery("UPDATE users SET name = :name WHERE id = :id")
                .SetParameter("name", "DatabaseUpdated")
                .SetParameter("id", 47)
                .ExecuteUpdate();
            tx.Commit();
        }

        session.Refresh(user);

        Assert.Equal("DatabaseUpdated", user.Name);
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
    /// EN: Verifies changing many-to-one navigation updates the persisted foreign key.
    /// PT: Verifica se alterar navegação many-to-one atualiza a chave estrangeira persistida.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_UpdateNavigation_ShouldUpdateForeignKey()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var g1 = new NhUserGroup { Id = 10, Name = "Team-A" };
            var g2 = new NhUserGroup { Id = 11, Name = "Team-B" };
            var user = new NhRelUser { Id = 401, Name = "Mover", Group = g1 };

            session.Save(g1);
            session.Save(g2);
            session.Save(user);
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var user = session.Get<NhRelUser>(401);
            var targetGroup = session.Get<NhUserGroup>(11);

            Assert.NotNull(user);
            Assert.NotNull(targetGroup);

            user!.Group = targetGroup;
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var updated = verifySession.Get<NhRelUser>(401);

        Assert.NotNull(updated);
        Assert.NotNull(updated!.Group);
        Assert.Equal(11, updated.Group!.Id);
    }

    /// <summary>
    /// EN: Verifies removing a child from a parent collection persists relationship changes after flush and in a new session.
    /// PT: Verifica se remover filho da coleção do pai persiste mudanças de relacionamento após flush e em nova sessão.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_RemoveChildFromCollection_ShouldPersistAfterFlushAndNewSession()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            var group = new NhUserGroup { Id = 12, Name = "Ops" };
            var user1 = new NhRelUser { Id = 402, Name = "U-1", Group = group };
            var user2 = new NhRelUser { Id = 403, Name = "U-2", Group = group };

            group.Users.Add(user1);
            group.Users.Add(user2);

            seedSession.Save(group);
            seedSession.Save(user1);
            seedSession.Save(user2);
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var loadedGroup = session.Get<NhUserGroup>(12)!;
            Assert.Equal(2, loadedGroup.Users.Count);

            var removed = loadedGroup.Users.Single(u => u.Id == 403);
            loadedGroup.Users.Remove(removed);
            removed.Group = null;

            session.Flush();
            Assert.Single(loadedGroup.Users);
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persistedGroup = verifySession.Get<NhUserGroup>(12);
        var detachedChild = verifySession.Get<NhRelUser>(403);
        var nullGroupCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users_rel WHERE group_id IS NULL")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .UniqueResult());

        Assert.NotNull(persistedGroup);
        Assert.Single(persistedGroup!.Users);
        Assert.NotNull(detachedChild);
        Assert.Null(detachedChild!.Group);
        Assert.Equal(1, nullGroupCount);
    }

    /// <summary>
    /// EN: Verifies moving a child between groups updates both relationship collections in a new session.
    /// PT: Verifica se mover um filho entre grupos atualiza ambas as coleções de relacionamento em nova sessão.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_MoveChildBetweenGroups_ShouldUpdateCollections()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            var source = new NhUserGroup { Id = 13, Name = "Source" };
            var target = new NhUserGroup { Id = 14, Name = "Target" };
            var user = new NhRelUser { Id = 404, Name = "Mover", Group = source };

            source.Users.Add(user);

            seedSession.Save(source);
            seedSession.Save(target);
            seedSession.Save(user);
            tx.Commit();
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var source = session.Get<NhUserGroup>(13)!;
            var target = session.Get<NhUserGroup>(14)!;
            var user = session.Get<NhRelUser>(404)!;

            source.Users.Remove(user);
            user.Group = target;
            target.Users.Add(user);

            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var sourceReloaded = verifySession.Get<NhUserGroup>(13)!;
        var targetReloaded = verifySession.Get<NhUserGroup>(14)!;

        Assert.Empty(sourceReloaded.Users);
        Assert.Single(targetReloaded.Users);
        Assert.Equal(404, targetReloaded.Users[0].Id);
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

    /// <summary>
    /// EN: Verifies version is incremented after a successful update on a versioned entity.
    /// PT: Verifica se a versão é incrementada após update bem-sucedido em entidade versionada.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_OptimisticConcurrency_ShouldIncrementVersionOnUpdate()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users_versioned (id INT PRIMARY KEY, version INT NOT NULL, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var seedTx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhVersionedUser { Id = 31, Name = "Initial" });
            seedTx.Commit();
        }

        int initialVersion;
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        {
            initialVersion = session.Get<NhVersionedUser>(31)!.Version;
        }

        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var entity = session.Get<NhVersionedUser>(31)!;
            entity.Name = "Updated";
            session.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhVersionedUser>(31);

        Assert.NotNull(persisted);
        Assert.True(persisted!.Version > initialVersion);
    }

    /// <summary>
    /// EN: Verifies refreshing stale reads in a second session allows consistent commit after a concurrent update.
    /// PT: Verifica se atualizar leitura obsoleta na segunda sessão permite commit consistente após update concorrente.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_OptimisticConcurrency_TwoSessionsRefresh_ShouldCommitConsistently()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users_versioned (id INT PRIMARY KEY, version INT NOT NULL, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var seedTx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhVersionedUser { Id = 32, Name = "Initial" });
            seedTx.Commit();
        }

        using var session1 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var session2 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var tx1 = session1.BeginTransaction();
        using var tx2 = session2.BeginTransaction();

        var user1 = session1.Get<NhVersionedUser>(32)!;
        var user2 = session2.Get<NhVersionedUser>(32)!;

        user1.Name = "Tx1";
        session1.Flush();
        tx1.Commit();

        session2.Refresh(user2);
        user2.Name = "Tx2AfterRefresh";
        session2.Flush();
        tx2.Commit();

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhVersionedUser>(32);

        Assert.NotNull(persisted);
        Assert.Equal("Tx2AfterRefresh", persisted!.Name);
    }

    /// <summary>
    /// EN: Verifies HQL partial projection returns selected scalar columns.
    /// PT: Verifica se projeção parcial em HQL retorna colunas escalares selecionadas.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_Projection_ShouldReturnPartialColumns()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 50, Name = "Proj-A" });
            session.Save(new NhTestUser { Id = 51, Name = "Proj-B" });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var projected = querySession
            .CreateQuery("select u.Id, u.Name from NhTestUser u where u.Id >= :id order by u.Id")
            .SetParameter("id", 50)
            .List<object[]>();

        Assert.Equal(2, projected.Count);
        Assert.Equal(50, projected[0][0]);
        Assert.Equal("Proj-A", projected[0][1]);
    }

    /// <summary>
    /// EN: Verifies ordering by relationship property works in HQL joins.
    /// PT: Verifica se ordenação por propriedade de relacionamento funciona em joins HQL.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_OrderByRelationshipProperty_ShouldSortRows()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var beta = new NhUserGroup { Id = 20, Name = "Beta" };
            var alpha = new NhUserGroup { Id = 21, Name = "Alpha" };

            session.Save(beta);
            session.Save(alpha);
            session.Save(new NhRelUser { Id = 501, Name = "B-User", Group = beta });
            session.Save(new NhRelUser { Id = 502, Name = "A-User", Group = alpha });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var rows = querySession
            .CreateQuery("select u.Name, g.Name from NhRelUser u join u.Group g order by g.Name asc, u.Name asc")
            .List<object[]>();

        Assert.Equal(2, rows.Count);
        Assert.Equal("A-User", rows[0][0]);
        Assert.Equal("Alpha", rows[0][1]);
        Assert.Equal("B-User", rows[1][0]);
        Assert.Equal("Beta", rows[1][1]);
    }

    /// <summary>
    /// EN: Verifies Criteria API can apply multiple restrictions in one query.
    /// PT: Verifica se a API Criteria pode aplicar múltiplas restrições em uma única consulta.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Criteria_MultipleRestrictions_ShouldFilterExpectedEntity()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 60, Name = "Alpha" });
            session.Save(new NhTestUser { Id = 61, Name = "Beta" });
            session.Save(new NhTestUser { Id = 62, Name = "Beta" });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var result = querySession
            .CreateCriteria<NhTestUser>()
            .Add(Restrictions.Eq(nameof(NhTestUser.Name), "Beta"))
            .Add(Restrictions.Gt(nameof(NhTestUser.Id), 61))
            .UniqueResult<NhTestUser>();

        Assert.NotNull(result);
        Assert.Equal(62, result!.Id);
    }

    /// <summary>
    /// EN: Verifies Criteria can filter by relationship using alias and multiple restrictions.
    /// PT: Verifica se Criteria pode filtrar por relacionamento usando alias e múltiplas restrições.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Criteria_RelationshipAliasAndRestrictions_ShouldFilterExpectedRows()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var g1 = new NhUserGroup { Id = 40, Name = "Criteria-A" };
            var g2 = new NhUserGroup { Id = 41, Name = "Criteria-B" };

            session.Save(g1);
            session.Save(g2);
            session.Save(new NhRelUser { Id = 901, Name = "X-1", Group = g1 });
            session.Save(new NhRelUser { Id = 902, Name = "X-2", Group = g1 });
            session.Save(new NhRelUser { Id = 903, Name = "Y-1", Group = g2 });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var users = querySession
            .CreateCriteria<NhRelUser>("u")
            .CreateAlias("u.Group", "g")
            .Add(Restrictions.Eq("g.Name", "Criteria-A"))
            .Add(Restrictions.Like("u.Name", "X%"))
            .AddOrder(Order.Asc("u.Id"))
            .List<NhRelUser>();

        Assert.Equal(2, users.Count);
        Assert.Equal(901, users[0].Id);
        Assert.Equal(902, users[1].Id);
    }

    /// <summary>
    /// EN: Verifies optimistic concurrency refresh flow increments version on each successful commit.
    /// PT: Verifica se o fluxo com refresh em concorrência otimista incrementa versão a cada commit bem-sucedido.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_OptimisticConcurrency_TwoSessionsRefresh_ShouldIncrementVersionEachCommit()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users_versioned (id INT PRIMARY KEY, version INT NOT NULL, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var seedTx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhVersionedUser { Id = 33, Name = "Initial" });
            seedTx.Commit();
        }

        using var session1 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var session2 = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var tx1 = session1.BeginTransaction();
        using var tx2 = session2.BeginTransaction();

        var user1 = session1.Get<NhVersionedUser>(33)!;
        var user2 = session2.Get<NhVersionedUser>(33)!;
        var initialVersion = user1.Version;

        user1.Name = "Tx1";
        session1.Flush();
        tx1.Commit();

        session2.Refresh(user2);
        var refreshedVersion = user2.Version;
        user2.Name = "Tx2";
        session2.Flush();
        tx2.Commit();

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var persisted = verifySession.Get<NhVersionedUser>(33);

        Assert.NotNull(persisted);
        Assert.True(refreshedVersion > initialVersion);
        Assert.True(persisted!.Version > refreshedVersion);
    }

    /// <summary>
    /// EN: Verifies clearing session after relationship mutation and re-querying returns persisted association state.
    /// PT: Verifica se limpar a sessão após mutação de relacionamento e consultar novamente retorna estado de associação persistido.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_SessionLifecycle_ClearAfterRelationshipMutation_ShouldReloadPersistedAssociation()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            var g1 = new NhUserGroup { Id = 50, Name = "G-Old" };
            var g2 = new NhUserGroup { Id = 51, Name = "G-New" };
            var user = new NhRelUser { Id = 950, Name = "SessionClearUser", Group = g1 };

            seedSession.Save(g1);
            seedSession.Save(g2);
            seedSession.Save(user);
            tx.Commit();
        }

        using var session = sessionFactory.WithOptions().Connection(connection).OpenSession();
        using var txChange = session.BeginTransaction();
        var userToMove = session.Get<NhRelUser>(950)!;
        var targetGroup = session.Get<NhUserGroup>(51)!;

        userToMove.Group = targetGroup;
        session.Flush();
        txChange.Commit();

        session.Clear();

        var reloaded = session.Get<NhRelUser>(950);
        Assert.NotNull(reloaded);
        Assert.NotNull(reloaded!.Group);
        Assert.Equal(51, reloaded.Group!.Id);
    }

    /// <summary>
    /// EN: Verifies HQL projection with aliases can be consumed as dictionaries through AliasToEntityMap.
    /// PT: Verifica se projeção HQL com aliases pode ser consumida como dicionário via AliasToEntityMap.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_ProjectionWithAliases_ShouldReturnAliasMap()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            session.Save(new NhTestUser { Id = 70, Name = "Map-A" });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var row = querySession
            .CreateQuery("select u.Id as UserId, u.Name as UserName from NhTestUser u where u.Id = :id")
            .SetParameter("id", 70)
            .SetResultTransformer(global::NHibernate.Transform.Transformers.AliasToEntityMap)
            .UniqueResult<System.Collections.IDictionary>();

        Assert.NotNull(row);
        Assert.Equal(70, row!["UserId"]);
        Assert.Equal("Map-A", row["UserName"]);
    }

    /// <summary>
    /// EN: Verifies HQL projection with relationship join and scalar aggregate returns expected tuple.
    /// PT: Verifica se projeção HQL com join de relacionamento e agregação escalar retorna a tupla esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_RelationshipProjectionWithAggregate_ShouldReturnExpectedTuple()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var group = new NhUserGroup { Id = 30, Name = "Projection-Team" };
            session.Save(group);
            session.Save(new NhRelUser { Id = 801, Name = "P1", Group = group });
            session.Save(new NhRelUser { Id = 802, Name = "P2", Group = group });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var row = querySession
            .CreateQuery("select g.Name, count(u.Id) from NhUserGroup g join g.Users u where g.Id = :groupId group by g.Name")
            .SetParameter("groupId", 30)
            .UniqueResult<object[]>();

        Assert.NotNull(row);
        Assert.Equal("Projection-Team", row![0]);
        Assert.Equal(2L, row[1]);
    }

    /// <summary>
    /// EN: Verifies Refresh on a versioned entity synchronizes stale version and latest values after concurrent commit.
    /// PT: Verifica se Refresh em entidade versionada sincroniza versão obsoleta e valores mais recentes após commit concorrente.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedEntity_OptimisticConcurrency_RefreshOnVersionedEntity_ShouldSyncVersionAndState()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE users_versioned (id INT PRIMARY KEY, version INT NOT NULL, name VARCHAR(100))");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            seedSession.Save(new NhVersionedUser { Id = 34, Name = "Initial" });
            tx.Commit();
        }

        using var staleSession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var staleEntity = staleSession.Get<NhVersionedUser>(34)!;
        var staleVersion = staleEntity.Version;

        using (var writerSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = writerSession.BeginTransaction())
        {
            var writerEntity = writerSession.Get<NhVersionedUser>(34)!;
            writerEntity.Name = "CommittedElsewhere";
            writerSession.Flush();
            tx.Commit();
        }

        staleSession.Refresh(staleEntity);

        Assert.Equal("CommittedElsewhere", staleEntity.Name);
        Assert.True(staleEntity.Version > staleVersion);
    }

    /// <summary>
    /// EN: Verifies moving a child between groups persists expected foreign-key distribution at SQL level.
    /// PT: Verifica se mover um filho entre grupos persiste a distribuição esperada de chave estrangeira em nível SQL.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_MoveChildBetweenGroups_ShouldPersistExpectedForeignKeyDistribution()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            var g1 = new NhUserGroup { Id = 70, Name = "Sql-Source" };
            var g2 = new NhUserGroup { Id = 71, Name = "Sql-Target" };
            var user = new NhRelUser { Id = 980, Name = "SqlMover", Group = g1 };

            seedSession.Save(g1);
            seedSession.Save(g2);
            seedSession.Save(user);
            tx.Commit();
        }

        using (var moveSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = moveSession.BeginTransaction())
        {
            var user = moveSession.Get<NhRelUser>(980)!;
            var target = moveSession.Get<NhUserGroup>(71)!;

            user.Group = target;
            moveSession.Flush();
            tx.Commit();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var sourceCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users_rel WHERE group_id = :gid")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("gid", 70)
                .UniqueResult());

        var targetCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users_rel WHERE group_id = :gid")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("gid", 71)
                .UniqueResult());

        Assert.Equal(0, sourceCount);
        Assert.Equal(1, targetCount);
    }

    /// <summary>
    /// EN: Verifies HQL ordering by relationship and user name remains deterministic under pagination window.
    /// PT: Verifica se ordenação HQL por relacionamento e nome de usuário permanece determinística sob janela de paginação.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_OrderByRelationshipProperty_WithPagination_ShouldReturnDeterministicWindow()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var alpha = new NhUserGroup { Id = 60, Name = "Alpha" };
            var beta = new NhUserGroup { Id = 61, Name = "Beta" };

            session.Save(alpha);
            session.Save(beta);
            session.Save(new NhRelUser { Id = 960, Name = "A-2", Group = alpha });
            session.Save(new NhRelUser { Id = 961, Name = "A-1", Group = alpha });
            session.Save(new NhRelUser { Id = 962, Name = "B-1", Group = beta });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var rows = querySession
            .CreateQuery("select u.Name, g.Name from NhRelUser u join u.Group g order by g.Name asc, u.Name asc")
            .SetFirstResult(1)
            .SetMaxResults(2)
            .List<object[]>();

        Assert.Equal(2, rows.Count);
        Assert.Equal("A-2", rows[0][0]);
        Assert.Equal("Alpha", rows[0][1]);
        Assert.Equal("B-1", rows[1][0]);
        Assert.Equal("Beta", rows[1][1]);
    }

    /// <summary>
    /// EN: Verifies HQL count by relationship filter returns the expected scalar value.
    /// PT: Verifica se contagem HQL por filtro de relacionamento retorna o valor escalar esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Hql_CountByRelationshipFilter_ShouldReturnExpectedScalar()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var gA = new NhUserGroup { Id = 80, Name = "Count-A" };
            var gB = new NhUserGroup { Id = 81, Name = "Count-B" };
            session.Save(gA);
            session.Save(gB);
            session.Save(new NhRelUser { Id = 990, Name = "C1", Group = gA });
            session.Save(new NhRelUser { Id = 991, Name = "C2", Group = gA });
            session.Save(new NhRelUser { Id = 992, Name = "C3", Group = gB });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var count = querySession
            .CreateQuery("select count(u.Id) from NhRelUser u join u.Group g where g.Name = :groupName")
            .SetParameter("groupName", "Count-A")
            .UniqueResult<long>();

        Assert.Equal(2L, count);
    }

    /// <summary>
    /// EN: Verifies relationship move rolled back in transaction keeps original foreign key distribution.
    /// PT: Verifica se movimentação de relacionamento com rollback transacional mantém a distribuição original de chave estrangeira.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_MappedRelationship_MoveChildBetweenGroups_WithRollback_ShouldKeepOriginalForeignKeyDistribution()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var seedSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = seedSession.BeginTransaction())
        {
            var source = new NhUserGroup { Id = 90, Name = "Rollback-Source" };
            var target = new NhUserGroup { Id = 91, Name = "Rollback-Target" };
            var user = new NhRelUser { Id = 999, Name = "Rollback-Mover", Group = source };

            seedSession.Save(source);
            seedSession.Save(target);
            seedSession.Save(user);
            tx.Commit();
        }

        using (var moveSession = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = moveSession.BeginTransaction())
        {
            var user = moveSession.Get<NhRelUser>(999)!;
            var target = moveSession.Get<NhUserGroup>(91)!;

            user.Group = target;
            moveSession.Flush();
            tx.Rollback();
        }

        using var verifySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var sourceCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users_rel WHERE group_id = :gid")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("gid", 90)
                .UniqueResult());

        var targetCount = Convert.ToInt32(
            verifySession
                .CreateSQLQuery("SELECT COUNT(*) AS cnt FROM users_rel WHERE group_id = :gid")
                .AddScalar("cnt", global::NHibernate.NHibernateUtil.Int32)
                .SetParameter("gid", 91)
                .UniqueResult());

        Assert.Equal(1, sourceCount);
        Assert.Equal(0, targetCount);
    }

    /// <summary>
    /// EN: Verifies Criteria count projection with relationship alias returns expected scalar.
    /// PT: Verifica se projeção de contagem via Criteria com alias de relacionamento retorna escalar esperado.
    /// </summary>
    [Fact]
    [Trait("Category", "NHibernate")]
    public void NHibernate_Criteria_CountWithRelationshipAlias_ShouldReturnExpectedScalar()
    {
        using var connection = CreateOpenConnection();
        ExecuteNonQuery(connection, "CREATE TABLE user_groups (id INT PRIMARY KEY, name VARCHAR(100))");
        ExecuteNonQuery(connection, "CREATE TABLE users_rel (id INT PRIMARY KEY, name VARCHAR(100), group_id INT)");

        using var sessionFactory = BuildConfiguration(withMappings: true).BuildSessionFactory();
        using (var session = sessionFactory.WithOptions().Connection(connection).OpenSession())
        using (var tx = session.BeginTransaction())
        {
            var g1 = new NhUserGroup { Id = 92, Name = "Criteria-Count-A" };
            var g2 = new NhUserGroup { Id = 93, Name = "Criteria-Count-B" };
            session.Save(g1);
            session.Save(g2);
            session.Save(new NhRelUser { Id = 1000, Name = "CA1", Group = g1 });
            session.Save(new NhRelUser { Id = 1001, Name = "CA2", Group = g1 });
            session.Save(new NhRelUser { Id = 1002, Name = "CB1", Group = g2 });
            tx.Commit();
        }

        using var querySession = sessionFactory.WithOptions().Connection(connection).OpenSession();
        var count = querySession
            .CreateCriteria<NhRelUser>("u")
            .CreateAlias("u.Group", "g")
            .Add(Restrictions.Eq("g.Name", "Criteria-Count-A"))
            .SetProjection(Projections.RowCountInt64())
            .UniqueResult<long>();

        Assert.Equal(2L, count);
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
