namespace DbSqlLikeMem.EfCore.Test;

/// <summary>
/// EN: Defines shared EF Core-oriented provider contract tests for mock connections.
/// PT: Define testes de contrato compartilhados orientados a EF Core para conexões mock.
/// </summary>
public abstract class EfCoreSupportTestsBase
{
    /// <summary>
    /// EN: Creates and opens the provider mock connection factory under test.
    /// PT: Cria e abre a fábrica de conexão mock do provedor sob teste.
    /// </summary>
    protected abstract IDbSqlLikeMemEfCoreConnectionFactory CreateFactory();

    /// <summary>
    /// EN: Verifies the factory returns an opened connection.
    /// PT: Verifica se a fábrica retorna uma conexão aberta.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_Factory_ShouldReturnOpenConnection()
    {
        using var connection = CreateFactory().CreateOpenConnection();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    /// <summary>
    /// EN: Verifies basic command flow used by EF Core relational stacks works on provider mocks.
    /// PT: Verifica se o fluxo básico de comandos usado por stacks relacionais de EF Core funciona nos mocks de provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldExecuteBasicSqlWithParameters()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO ef_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "Alice";
            insert.Parameters.Add(name);

            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT name FROM ef_users WHERE id = @id";
        var selectId = select.CreateParameter();
        selectId.ParameterName = "@id";
        selectId.Value = 1;
        select.Parameters.Add(selectId);

        var result = select.ExecuteScalar();
        Assert.Equal("Alice", Convert.ToString(result));
    }


    /// <summary>
    /// EN: Verifies transaction rollback keeps data unchanged after an INSERT statement.
    /// PT: Verifica se o rollback da transação mantém os dados inalterados após um comando INSERT.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_TransactionRollback_ShouldUndoInsert()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_tx_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO ef_tx_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 10;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "Rollback";
            insert.Parameters.Add(name);

            _ = insert.ExecuteNonQuery();
            tx.Rollback();
        }

        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM ef_tx_users WHERE id = @id";

        var countId = count.CreateParameter();
        countId.ParameterName = "@id";
        countId.Value = 10;
        count.Parameters.Add(countId);

        var rows = Convert.ToInt32(count.ExecuteScalar());
        Assert.Equal(0, rows);
    }

    /// <summary>
    /// EN: Verifies null parameter values are accepted in inserts and can be queried back.
    /// PT: Verifica se valores de parâmetro nulos são aceitos em inserts e podem ser consultados depois.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldPersistNullParameterValues()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_nullable_users (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO ef_nullable_users (id, nickname) VALUES (@id, @nickname)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 11;
            insert.Parameters.Add(id);

            var nickname = insert.CreateParameter();
            nickname.ParameterName = "@nickname";
            nickname.Value = DBNull.Value;
            insert.Parameters.Add(nickname);

            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT nickname FROM ef_nullable_users WHERE id = @id";

        var selectId = select.CreateParameter();
        selectId.ParameterName = "@id";
        selectId.Value = 11;
        select.Parameters.Add(selectId);

        var value = select.ExecuteScalar();
        Assert.True(value is null || value is DBNull);
    }



    /// <summary>
    /// EN: Verifies transaction commit persists inserted data.
    /// PT: Verifica se o commit da transação persiste os dados inseridos.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_TransactionCommit_ShouldPersistInsert()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_tx_commit_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO ef_tx_commit_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 20;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "Committed";
            insert.Parameters.Add(name);

            _ = insert.ExecuteNonQuery();
            tx.Commit();
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT name FROM ef_tx_commit_users WHERE id = @id";

        var idParam = select.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 20;
        select.Parameters.Add(idParam);

        var result = Convert.ToString(select.ExecuteScalar());
        Assert.Equal("Committed", result);
    }

    /// <summary>
    /// EN: Verifies update and delete command flows work with provider factories.
    /// PT: Verifica se fluxos de comando de update e delete funcionam com as fábricas por provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldExecuteUpdateAndDelete()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_mutation_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO ef_mutation_users (id, name) VALUES (1, 'Before')";
            _ = insert.ExecuteNonQuery();
        }

        using (var update = connection.CreateCommand())
        {
            update.CommandText = "UPDATE ef_mutation_users SET name = @name WHERE id = @id";

            var name = update.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "After";
            update.Parameters.Add(name);

            var id = update.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            update.Parameters.Add(id);

            var updated = update.ExecuteNonQuery();
            Assert.Equal(1, updated);
        }

        using (var delete = connection.CreateCommand())
        {
            delete.CommandText = "DELETE FROM ef_mutation_users WHERE id = @id";

            var id = delete.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            delete.Parameters.Add(id);

            var deleted = delete.ExecuteNonQuery();
            Assert.Equal(1, deleted);
        }

        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM ef_mutation_users";
        var rows = Convert.ToInt32(count.ExecuteScalar());
        Assert.Equal(0, rows);
    }

    /// <summary>
    /// EN: Verifies decimal and datetime parameter bindings can be persisted and queried.
    /// PT: Verifica se bindings de parâmetros decimal e datetime podem ser persistidos e consultados.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldPersistDecimalAndDateTimeParameters()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_typed_values (id INT PRIMARY KEY, amount DECIMAL(18,2), created DATETIME)";
            _ = create.ExecuteNonQuery();
        }

        var now = new DateTime(2024, 12, 31, 13, 45, 10, DateTimeKind.Utc);
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO ef_typed_values (id, amount, created) VALUES (@id, @amount, @created)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            insert.Parameters.Add(id);

            var amount = insert.CreateParameter();
            amount.ParameterName = "@amount";
            amount.Value = 123.45m;
            insert.Parameters.Add(amount);

            var created = insert.CreateParameter();
            created.ParameterName = "@created";
            created.Value = now;
            insert.Parameters.Add(created);

            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT amount FROM ef_typed_values WHERE id = @id";

        var idParam = select.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 1;
        select.Parameters.Add(idParam);

        var result = Convert.ToDecimal(select.ExecuteScalar());
        Assert.Equal(123.45m, result);
    }



    /// <summary>
    /// EN: Verifies aggregate scalar queries over inserted rows return expected values.
    /// PT: Verifica se consultas escalares agregadas sobre linhas inseridas retornam valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportAggregateScalarQueries()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_agg_values (id INT PRIMARY KEY, score INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 3; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_agg_values (id, score) VALUES (@id, @score)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var score = insert.CreateParameter(); score.ParameterName = "@score"; score.Value = i * 10; insert.Parameters.Add(score);
            _ = insert.ExecuteNonQuery();
        }

        using var sum = connection.CreateCommand();
        sum.CommandText = "SELECT SUM(score) FROM ef_agg_values";
        Assert.Equal(60, Convert.ToInt32(sum.ExecuteScalar()));

        using var max = connection.CreateCommand();
        max.CommandText = "SELECT MAX(score) FROM ef_agg_values";
        Assert.Equal(30, Convert.ToInt32(max.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies `IN` filtering with parameterized values and ordered results.
    /// PT: Verifica filtro com `IN` usando valores parametrizados e resultados ordenados.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldFilterWithInAndOrderBy()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_filter_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 3; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_filter_users (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = $"User-{i}"; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT id FROM ef_filter_users WHERE id IN (@a, @b) ORDER BY id DESC";
        var a = select.CreateParameter(); a.ParameterName = "@a"; a.Value = 1; select.Parameters.Add(a);
        var b = select.CreateParameter(); b.ParameterName = "@b"; b.Value = 3; select.Parameters.Add(b);

        var rows = new List<int>();
        using var reader = select.ExecuteReader();
        while (reader.Read()) rows.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([3, 1], rows);
    }

    /// <summary>
    /// EN: Verifies inner join queries can be executed through provider mock connections.
    /// PT: Verifica se consultas com inner join podem ser executadas através das conexões mock de provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldExecuteInnerJoinQuery()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand())
        {
            createUsers.CommandText = "CREATE TABLE ef_join_users (id INT PRIMARY KEY, dept_id INT)";
            _ = createUsers.ExecuteNonQuery();
        }
        using (var createDepts = connection.CreateCommand())
        {
            createDepts.CommandText = "CREATE TABLE ef_join_depts (id INT PRIMARY KEY, title VARCHAR(100))";
            _ = createDepts.ExecuteNonQuery();
        }
        using (var insertDept = connection.CreateCommand())
        {
            insertDept.CommandText = "INSERT INTO ef_join_depts (id, title) VALUES (1, 'Engineering')";
            _ = insertDept.ExecuteNonQuery();
        }
        using (var insertUser = connection.CreateCommand())
        {
            insertUser.CommandText = "INSERT INTO ef_join_users (id, dept_id) VALUES (10, 1)";
            _ = insertUser.ExecuteNonQuery();
        }

        using var join = connection.CreateCommand();
        join.CommandText = "SELECT COUNT(*) FROM ef_join_users u INNER JOIN ef_join_depts d ON d.id = u.dept_id";
        Assert.Equal(1, Convert.ToInt32(join.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies grouped queries with HAVING return expected aggregate windows.
    /// PT: Verifica se consultas agrupadas com HAVING retornam janelas agregadas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportGroupByHaving()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_sales (id INT PRIMARY KEY, category VARCHAR(20), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        (int id, string category, int amount)[] rows = [(1,"A",30),(2,"A",40),(3,"B",20),(4,"B",15),(5,"C",10)];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_sales (id, category, amount) VALUES (@id, @category, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var c = insert.CreateParameter(); c.ParameterName = "@category"; c.Value = row.category; insert.Parameters.Add(c);
            var a = insert.CreateParameter(); a.ParameterName = "@amount"; a.Value = row.amount; insert.Parameters.Add(a);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT category, SUM(amount) total FROM ef_sales GROUP BY category HAVING SUM(amount) >= @minTotal ORDER BY total DESC";
        var min = query.CreateParameter(); min.ParameterName = "@minTotal"; min.Value = 35; query.Parameters.Add(min);

        var result = new List<(string category,int total)>();
        using var reader = query.ExecuteReader();
        while (reader.Read()) result.Add((Convert.ToString(reader[0])!, Convert.ToInt32(reader[1])));

        Assert.Equal(("A",70), result[0]);
        Assert.Equal(("B",35), result[1]);
    }

    /// <summary>
    /// EN: Verifies OFFSET/FETCH-style pagination returns deterministic windows.
    /// PT: Verifica se paginação estilo OFFSET/FETCH retorna janelas determinísticas.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportPaginationWindow()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_page_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        for (var i=1;i<=5;i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_page_users (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = $"User-{i}"; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var page = connection.CreateCommand();
        page.CommandText = "SELECT id FROM ef_page_users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var ids = new List<int>();
        using var reader = page.ExecuteReader();
        while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([2,3], ids);
    }

    /// <summary>
    /// EN: Verifies correlated EXISTS subqueries can be executed with parameterized predicates.
    /// PT: Verifica se subqueries correlacionadas com EXISTS podem ser executadas com predicados parametrizados.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportCorrelatedExists()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var u = connection.CreateCommand()) { u.CommandText = "CREATE TABLE ef_exists_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = u.ExecuteNonQuery(); }
        using (var o = connection.CreateCommand()) { o.CommandText = "CREATE TABLE ef_exists_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = o.ExecuteNonQuery(); }
        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO ef_exists_users (id, name) VALUES (1, 'Alice')"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO ef_exists_users (id, name) VALUES (2, 'Bob')"; _ = i2.ExecuteNonQuery(); }
        using (var i3 = connection.CreateCommand()) { i3.CommandText = "INSERT INTO ef_exists_orders (id, user_id, amount) VALUES (100, 1, 50)"; _ = i3.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT COUNT(*)
FROM ef_exists_users u
WHERE EXISTS (
  SELECT 1 FROM ef_exists_orders o
  WHERE o.user_id = u.id AND o.amount >= @minAmount
)";
        var min = query.CreateParameter(); min.ParameterName = "@minAmount"; min.Value = 40; query.Parameters.Add(min);

        Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies LEFT JOIN with IS NULL can detect rows without related matches.
    /// PT: Verifica se LEFT JOIN com IS NULL detecta linhas sem correspondências relacionadas.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportLeftJoinWithIsNullFilter()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var cu = connection.CreateCommand()) { cu.CommandText = "CREATE TABLE ef_left_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = cu.ExecuteNonQuery(); }
        using (var cp = connection.CreateCommand()) { cp.CommandText = "CREATE TABLE ef_left_profiles (id INT PRIMARY KEY, user_id INT)"; _ = cp.ExecuteNonQuery(); }
        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO ef_left_users (id, name) VALUES (1, 'HasProfile')"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO ef_left_users (id, name) VALUES (2, 'NoProfile')"; _ = i2.ExecuteNonQuery(); }
        using (var p = connection.CreateCommand()) { p.CommandText = "INSERT INTO ef_left_profiles (id, user_id) VALUES (10, 1)"; _ = p.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT COUNT(*)
FROM ef_left_users u
LEFT JOIN ef_left_profiles p ON p.user_id = u.id
WHERE p.id IS NULL";

        Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies CASE WHEN projections and CASE-based HAVING filters are evaluated correctly.
    /// PT: Verifica se projeções com CASE WHEN e filtros HAVING baseados em CASE são avaliados corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportCaseWhenInSelectAndHaving()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_case_sales (id INT PRIMARY KEY, category VARCHAR(20), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        (int id, string category, int amount)[] rows = [(1, "A", 80), (2, "A", 30), (3, "B", 20), (4, "B", 10)];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_case_sales (id, category, amount) VALUES (@id, @category, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var category = insert.CreateParameter(); category.ParameterName = "@category"; category.Value = row.category; insert.Parameters.Add(category);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.amount; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  category,
  SUM(CASE WHEN amount >= @cutoff THEN 1 ELSE 0 END) high_count,
  CASE WHEN SUM(amount) >= @target THEN 'TOP' ELSE 'REGULAR' END tier
FROM ef_case_sales
GROUP BY category
HAVING SUM(CASE WHEN amount >= @cutoff THEN 1 ELSE 0 END) >= @minHits
ORDER BY category";
        var cutoff = query.CreateParameter(); cutoff.ParameterName = "@cutoff"; cutoff.Value = 50; query.Parameters.Add(cutoff);
        var target = query.CreateParameter(); target.ParameterName = "@target"; target.Value = 100; query.Parameters.Add(target);
        var minHits = query.CreateParameter(); minHits.ParameterName = "@minHits"; minHits.Value = 1; query.Parameters.Add(minHits);

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("A", Convert.ToString(reader[0]));
        Assert.Equal(1, Convert.ToInt32(reader[1]));
        Assert.Equal("TOP", Convert.ToString(reader[2]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies parameterized LIKE predicates with wildcard values are applied correctly.
    /// PT: Verifica se predicados LIKE parametrizados com curingas são aplicados corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportParameterizedLikeWithWildcard()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_like_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        (int id, string name)[] rows = [(1, "Alice"), (2, "Aline"), (3, "Bruno")];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_like_users (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.name; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM ef_like_users WHERE name LIKE @pattern";
        var pattern = query.CreateParameter();
        pattern.ParameterName = "@pattern";
        pattern.Value = "Ali%";
        query.Parameters.Add(pattern);

        Assert.Equal(2, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies composite filters mixing IS NULL and parameterized alternatives are honored.
    /// PT: Verifica se filtros compostos combinando IS NULL e alternativas parametrizadas são respeitados.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportCompositeNullOrFilter()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_null_filter_users (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var insertNull = connection.CreateCommand()) { insertNull.CommandText = "INSERT INTO ef_null_filter_users (id, nickname) VALUES (1, NULL)"; _ = insertNull.ExecuteNonQuery(); }
        using (var insertAna = connection.CreateCommand()) { insertAna.CommandText = "INSERT INTO ef_null_filter_users (id, nickname) VALUES (2, 'Ana')"; _ = insertAna.ExecuteNonQuery(); }
        using (var insertBob = connection.CreateCommand()) { insertBob.CommandText = "INSERT INTO ef_null_filter_users (id, nickname) VALUES (3, 'Bob')"; _ = insertBob.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM ef_null_filter_users WHERE nickname IS NULL OR nickname = @nickname";
        var nickname = query.CreateParameter();
        nickname.ParameterName = "@nickname";
        nickname.Value = "Ana";
        query.Parameters.Add(nickname);

        Assert.Equal(2, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies scalar subqueries can be projected in the SELECT list.
    /// PT: Verifica se subqueries escalares podem ser projetadas na lista SELECT.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldSupportScalarSubqueryProjection()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE ef_scalar_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE ef_scalar_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var user = connection.CreateCommand()) { user.CommandText = "INSERT INTO ef_scalar_users (id, name) VALUES (1, 'Alice')"; _ = user.ExecuteNonQuery(); }
        using (var order1 = connection.CreateCommand()) { order1.CommandText = "INSERT INTO ef_scalar_orders (id, user_id, amount) VALUES (10, 1, 25)"; _ = order1.ExecuteNonQuery(); }
        using (var order2 = connection.CreateCommand()) { order2.CommandText = "INSERT INTO ef_scalar_orders (id, user_id, amount) VALUES (11, 1, 30)"; _ = order2.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.name,
  (SELECT SUM(o.amount) FROM ef_scalar_orders o WHERE o.user_id = u.id) total_amount
FROM ef_scalar_users u
WHERE u.id = 1";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader[0]));
        Assert.Equal(55, Convert.ToInt32(reader[1]));
    }

    /// <summary>
    /// EN: Verifies multiple commands executed within the same transaction scope remain consistent after commit.
    /// PT: Verifica se múltiplos comandos executados no mesmo escopo transacional permanecem consistentes após commit.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_TransactionScope_ShouldKeepMultipleCommandsConsistent()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_tx_scope_accounts (id INT PRIMARY KEY, balance INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO ef_tx_scope_accounts (id, balance) VALUES (1, 100), (2, 50)";
            _ = seed.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var debit = connection.CreateCommand();
            debit.Transaction = tx;
            debit.CommandText = "UPDATE ef_tx_scope_accounts SET balance = balance - @amount WHERE id = @id";
            var debitAmount = debit.CreateParameter(); debitAmount.ParameterName = "@amount"; debitAmount.Value = 20; debit.Parameters.Add(debitAmount);
            var debitId = debit.CreateParameter(); debitId.ParameterName = "@id"; debitId.Value = 1; debit.Parameters.Add(debitId);
            _ = debit.ExecuteNonQuery();

            using var credit = connection.CreateCommand();
            credit.Transaction = tx;
            credit.CommandText = "UPDATE ef_tx_scope_accounts SET balance = balance + @amount WHERE id = @id";
            var creditAmount = credit.CreateParameter(); creditAmount.ParameterName = "@amount"; creditAmount.Value = 20; credit.Parameters.Add(creditAmount);
            var creditId = credit.CreateParameter(); creditId.ParameterName = "@id"; creditId.Value = 2; credit.Parameters.Add(creditId);
            _ = credit.ExecuteNonQuery();

            tx.Commit();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT SUM(balance) FROM ef_tx_scope_accounts";
        Assert.Equal(150, Convert.ToInt32(verify.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies pagination ordering remains stable when using a deterministic tie-breaker.
    /// PT: Verifica se a ordenação da paginação permanece estável ao usar um critério de desempate determinístico.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ShouldProvideStableOrderingForPagination()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_page_stable (id INT PRIMARY KEY, grp INT, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        (int id, int grp, string name)[] rows = [(1, 1, "A"), (2, 1, "B"), (3, 1, "C"), (4, 2, "D")];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_page_stable (id, grp, name) VALUES (@id, @grp, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = row.grp; insert.Parameters.Add(grp);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.name; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var page = connection.CreateCommand();
        page.CommandText = "SELECT id FROM ef_page_stable ORDER BY grp, id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var ids = new List<int>();
        using var reader = page.ExecuteReader();
        while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([2, 3], ids);
    }



    /// <summary>
    /// EN: Verifies a multi-command transaction rollback restores the state before all changes in the scope.
    /// PT: Verifica se o rollback de transação com múltiplos comandos restaura o estado anterior a todas as mudanças do escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_TransactionScopeRollback_ShouldUndoAllCommands()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_tx_scope_rollback (id INT PRIMARY KEY, balance INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO ef_tx_scope_rollback (id, balance) VALUES (1, 100), (2, 50)";
            _ = seed.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var debit = connection.CreateCommand();
            debit.Transaction = tx;
            debit.CommandText = "UPDATE ef_tx_scope_rollback SET balance = balance - 20 WHERE id = 1";
            _ = debit.ExecuteNonQuery();

            using var credit = connection.CreateCommand();
            credit.Transaction = tx;
            credit.CommandText = "UPDATE ef_tx_scope_rollback SET balance = balance + 20 WHERE id = 2";
            _ = credit.ExecuteNonQuery();

            tx.Rollback();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT balance FROM ef_tx_scope_rollback WHERE id = @id";

        var id = verify.CreateParameter();
        id.ParameterName = "@id";
        id.Value = 1;
        verify.Parameters.Add(id);

        Assert.Equal(100, Convert.ToInt32(verify.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies deterministic pagination returns the same window across repeated executions.
    /// PT: Verifica se a paginação determinística retorna a mesma janela em execuções repetidas.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_PaginationWithDeterministicOrder_ShouldBeStableAcrossExecutions()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_page_repeatable (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 6; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_page_repeatable (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 3 ? 1 : 2; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadWindow()
        {
            using var page = connection.CreateCommand();
            page.CommandText = "SELECT id FROM ef_page_repeatable ORDER BY grp, id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY";

            var ids = new List<int>();
            using var reader = page.ExecuteReader();
            while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));
            return ids;
        }

        var first = ReadWindow();
        var second = ReadWindow();

        Assert.Equal([3, 4], first);
        Assert.Equal(first, second);
    }



    /// <summary>
    /// EN: Verifies scalar subquery projection returns null when no matching rows are found.
    /// PT: Verifica se a projeção com subquery escalar retorna nulo quando não existem linhas correspondentes.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_ScalarSubqueryProjectionWithoutMatches_ShouldReturnNull()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE ef_scalar_null_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE ef_scalar_null_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var user = connection.CreateCommand()) { user.CommandText = "INSERT INTO ef_scalar_null_users (id, name) VALUES (1, 'Alice')"; _ = user.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.name,
  (SELECT SUM(o.amount) FROM ef_scalar_null_orders o WHERE o.user_id = u.id) total_amount
FROM ef_scalar_null_users u
WHERE u.id = 1";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader[0]));
        Assert.True(reader[1] is null || reader[1] is DBNull);
    }

    /// <summary>
    /// EN: Verifies transaction-scoped reads observe intermediate writes before commit.
    /// PT: Verifica se leituras no escopo transacional observam escritas intermediárias antes do commit.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_TransactionScope_ReadAfterWrite_ShouldObserveCurrentTransactionState()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_tx_read_write (id INT PRIMARY KEY, value INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO ef_tx_read_write (id, value) VALUES (1, 10)";
            _ = seed.ExecuteNonQuery();
        }

        using var tx = connection.BeginTransaction();

        using (var update = connection.CreateCommand())
        {
            update.Transaction = tx;
            update.CommandText = "UPDATE ef_tx_read_write SET value = value + 5 WHERE id = 1";
            _ = update.ExecuteNonQuery();
        }

        using var read = connection.CreateCommand();
        read.Transaction = tx;
        read.CommandText = "SELECT value FROM ef_tx_read_write WHERE id = 1";

        Assert.Equal(15, Convert.ToInt32(read.ExecuteScalar()));
        tx.Rollback();
    }



    /// <summary>
    /// EN: Verifies composite null filters remain correct when the comparison parameter is null.
    /// PT: Verifica se filtros compostos com nulo permanecem corretos quando o parâmetro de comparação é nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_CompositeNullOrFilter_WithNullParameter_ShouldOnlyMatchNullRows()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_null_param_filter (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO ef_null_param_filter (id, nickname) VALUES (1, NULL)"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO ef_null_param_filter (id, nickname) VALUES (2, 'Ana')"; _ = i2.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM ef_null_param_filter WHERE nickname IS NULL OR nickname = @nickname";
        var nickname = query.CreateParameter();
        nickname.ParameterName = "@nickname";
        nickname.Value = DBNull.Value;
        query.Parameters.Add(nickname);

        Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies deterministic ordering keeps pagination windows disjoint across consecutive pages.
    /// PT: Verifica se a ordenação determinística mantém janelas de paginação sem sobreposição entre páginas consecutivas.
    /// </summary>
    [Fact]
    [Trait("Category", "EfCore")]
    public void EfCore_FactoryConnection_PaginationWithDeterministicOrder_ShouldReturnDisjointConsecutivePages()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE ef_page_disjoint (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 8; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO ef_page_disjoint (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 4 ? 1 : 2; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadPage(int offset)
        {
            using var page = connection.CreateCommand();
            page.CommandText = $"SELECT id FROM ef_page_disjoint ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY";

            var ids = new List<int>();
            using var reader = page.ExecuteReader();
            while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));
            return ids;
        }

        var firstPage = ReadPage(0);
        var secondPage = ReadPage(3);

        Assert.Equal([1, 2, 3], firstPage);
        Assert.Equal([4, 5, 6], secondPage);
        Assert.DoesNotContain(secondPage[0], firstPage);
    }

}
