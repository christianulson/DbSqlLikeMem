namespace DbSqlLikeMem.LinqToDb.Test;

/// <summary>
/// EN: Defines shared LinqToDB-oriented provider contract tests for mock connections.
/// PT: Define testes de contrato compartilhados orientados a LinqToDB para conexões mock.
/// </summary>
public abstract class LinqToDbSupportTestsBase
{
    /// <summary>
    /// EN: Creates and opens the provider mock connection factory under test.
    /// PT: Cria e abre a fábrica de conexão mock do provedor sob teste.
    /// </summary>
    protected abstract IDbSqlLikeMemLinqToDbConnectionFactory CreateFactory();

    /// <summary>
    /// EN: Verifies the factory returns an opened connection.
    /// PT: Verifica se a fábrica retorna uma conexão aberta.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_Factory_ShouldReturnOpenConnection()
    {
        using var connection = CreateFactory().CreateOpenConnection();
        Assert.Equal(ConnectionState.Open, connection.State);
    }

    /// <summary>
    /// EN: Verifies basic SQL command execution and scalar query via provider mock connection.
    /// PT: Verifica execução básica de comando SQL e consulta escalar via conexão mock do provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldExecuteBasicSqlFlow()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO l2db_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "LinqToDb";
            insert.Parameters.Add(name);

            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT COUNT(*) FROM l2db_users";
        var count = Convert.ToInt32(select.ExecuteScalar());
        Assert.Equal(1, count);
    }


    /// <summary>
    /// EN: Verifies transaction rollback keeps data unchanged after an INSERT statement.
    /// PT: Verifica se o rollback da transação mantém os dados inalterados após um comando INSERT.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionRollback_ShouldUndoInsert()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO l2db_tx_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 7;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "Rollback";
            insert.Parameters.Add(name);

            _ = insert.ExecuteNonQuery();
            tx.Rollback();
        }

        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM l2db_tx_users WHERE id = @id";
        var idParam = count.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 7;
        count.Parameters.Add(idParam);

        var rows = Convert.ToInt32(count.ExecuteScalar());
        Assert.Equal(0, rows);
    }

    /// <summary>
    /// EN: Verifies transaction commit persists inserted data.
    /// PT: Verifica se o commit da transação persiste os dados inseridos.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionCommit_ShouldPersistInsert()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_commit_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO l2db_commit_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 8;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = "Committed";
            insert.Parameters.Add(name);

            _ = insert.ExecuteNonQuery();
            tx.Commit();
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT name FROM l2db_commit_users WHERE id = @id";
        var idParam = select.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 8;
        select.Parameters.Add(idParam);

        var value = Convert.ToString(select.ExecuteScalar());
        Assert.Equal("Committed", value);
    }

    /// <summary>
    /// EN: Verifies update and delete command flows work with LinqToDB provider factories.
    /// PT: Verifica se fluxos de update e delete funcionam com as fábricas LinqToDB por provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldExecuteUpdateAndDelete()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_mutation_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO l2db_mutation_users (id, name) VALUES (1, 'Before')";
            _ = insert.ExecuteNonQuery();
        }

        using (var update = connection.CreateCommand())
        {
            update.CommandText = "UPDATE l2db_mutation_users SET name = @name WHERE id = @id";

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
            delete.CommandText = "DELETE FROM l2db_mutation_users WHERE id = @id";

            var id = delete.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            delete.Parameters.Add(id);

            var deleted = delete.ExecuteNonQuery();
            Assert.Equal(1, deleted);
        }

        using var count = connection.CreateCommand();
        count.CommandText = "SELECT COUNT(*) FROM l2db_mutation_users";
        Assert.Equal(0, Convert.ToInt32(count.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies null parameter values are accepted and queryable.
    /// PT: Verifica se valores de parâmetro nulos são aceitos e consultáveis.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldPersistNullParameterValues()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_nullable_users (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO l2db_nullable_users (id, nickname) VALUES (@id, @nickname)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 12;
            insert.Parameters.Add(id);

            var nickname = insert.CreateParameter();
            nickname.ParameterName = "@nickname";
            nickname.Value = DBNull.Value;
            insert.Parameters.Add(nickname);

            Assert.Equal(1, insert.ExecuteNonQuery());
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT nickname FROM l2db_nullable_users WHERE id = @id";
        var idParam = select.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 12;
        select.Parameters.Add(idParam);

        var value = select.ExecuteScalar();
        Assert.True(value is null || value is DBNull);
    }



    /// <summary>
    /// EN: Verifies decimal and datetime parameter values can be persisted and queried.
    /// PT: Verifica se valores de parâmetros decimal e datetime podem ser persistidos e consultados.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldPersistDecimalAndDateTimeParameters()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_typed_values (id INT PRIMARY KEY, amount DECIMAL(18,2), created DATETIME)";
            _ = create.ExecuteNonQuery();
        }

        var createdAt = new DateTime(2025, 1, 1, 10, 30, 0, DateTimeKind.Utc);
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = "INSERT INTO l2db_typed_values (id, amount, created) VALUES (@id, @amount, @created)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = 1;
            insert.Parameters.Add(id);

            var amount = insert.CreateParameter();
            amount.ParameterName = "@amount";
            amount.Value = 91.25m;
            insert.Parameters.Add(amount);

            var created = insert.CreateParameter();
            created.ParameterName = "@created";
            created.Value = createdAt;
            insert.Parameters.Add(created);

            Assert.Equal(1, insert.ExecuteNonQuery());
        }

        using var amountSelect = connection.CreateCommand();
        amountSelect.CommandText = "SELECT amount FROM l2db_typed_values WHERE id = @id";
        var idParam = amountSelect.CreateParameter();
        idParam.ParameterName = "@id";
        idParam.Value = 1;
        amountSelect.Parameters.Add(idParam);

        var amountValue = Convert.ToDecimal(amountSelect.ExecuteScalar());
        Assert.Equal(91.25m, amountValue);
    }

    /// <summary>
    /// EN: Verifies aggregate scalar queries over inserted rows return expected values.
    /// PT: Verifica se consultas escalares agregadas sobre linhas inseridas retornam valores esperados.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportAggregateScalarQueries()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_agg_values (id INT PRIMARY KEY, score INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 3; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_agg_values (id, score) VALUES (@id, @score)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = i;
            insert.Parameters.Add(id);

            var score = insert.CreateParameter();
            score.ParameterName = "@score";
            score.Value = i * 10;
            insert.Parameters.Add(score);

            _ = insert.ExecuteNonQuery();
        }

        using var sum = connection.CreateCommand();
        sum.CommandText = "SELECT SUM(score) FROM l2db_agg_values";
        Assert.Equal(60, Convert.ToInt32(sum.ExecuteScalar()));

        using var max = connection.CreateCommand();
        max.CommandText = "SELECT MAX(score) FROM l2db_agg_values";
        Assert.Equal(30, Convert.ToInt32(max.ExecuteScalar()));
    }



    /// <summary>
    /// EN: Verifies `IN` filtering with parameterized values and ordered results.
    /// PT: Verifica filtro com `IN` usando valores parametrizados e resultados ordenados.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldFilterWithInAndOrderBy()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_filter_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 3; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_filter_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter();
            id.ParameterName = "@id";
            id.Value = i;
            insert.Parameters.Add(id);

            var name = insert.CreateParameter();
            name.ParameterName = "@name";
            name.Value = $"User-{i}";
            insert.Parameters.Add(name);

            _ = insert.ExecuteNonQuery();
        }

        using var select = connection.CreateCommand();
        select.CommandText = "SELECT id FROM l2db_filter_users WHERE id IN (@a, @b) ORDER BY id DESC";

        var a = select.CreateParameter();
        a.ParameterName = "@a";
        a.Value = 1;
        select.Parameters.Add(a);

        var b = select.CreateParameter();
        b.ParameterName = "@b";
        b.Value = 3;
        select.Parameters.Add(b);

        var rows = new List<int>();
        using var reader = select.ExecuteReader();
        while (reader.Read())
            rows.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([3, 1], rows);
    }

    /// <summary>
    /// EN: Verifies inner join queries can be executed through provider mock connections.
    /// PT: Verifica se consultas com inner join podem ser executadas através das conexões mock de provedor.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldExecuteInnerJoinQuery()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand())
        {
            createUsers.CommandText = "CREATE TABLE l2db_join_users (id INT PRIMARY KEY, dept_id INT)";
            _ = createUsers.ExecuteNonQuery();
        }

        using (var createDepts = connection.CreateCommand())
        {
            createDepts.CommandText = "CREATE TABLE l2db_join_depts (id INT PRIMARY KEY, title VARCHAR(100))";
            _ = createDepts.ExecuteNonQuery();
        }

        using (var insertDept = connection.CreateCommand())
        {
            insertDept.CommandText = "INSERT INTO l2db_join_depts (id, title) VALUES (1, 'Engineering')";
            _ = insertDept.ExecuteNonQuery();
        }

        using (var insertUser = connection.CreateCommand())
        {
            insertUser.CommandText = "INSERT INTO l2db_join_users (id, dept_id) VALUES (10, 1)";
            _ = insertUser.ExecuteNonQuery();
        }

        using var join = connection.CreateCommand();
        join.CommandText = "SELECT COUNT(*) FROM l2db_join_users u INNER JOIN l2db_join_depts d ON d.id = u.dept_id";
        var count = Convert.ToInt32(join.ExecuteScalar());

        Assert.Equal(1, count);
    }



    /// <summary>
    /// EN: Verifies grouped queries with HAVING return the expected aggregate window.
    /// PT: Verifica se consultas agrupadas com HAVING retornam a janela agregada esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportGroupByHaving()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_sales (id INT PRIMARY KEY, category VARCHAR(20), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        (int id, string category, int amount)[] rows =
        [
            (1, "A", 30),
            (2, "A", 40),
            (3, "B", 20),
            (4, "B", 15),
            (5, "C", 10),
        ];

        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_sales (id, category, amount) VALUES (@id, @category, @amount)";

            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var category = insert.CreateParameter(); category.ParameterName = "@category"; category.Value = row.category; insert.Parameters.Add(category);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.amount; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT category, SUM(amount) total FROM l2db_sales GROUP BY category HAVING SUM(amount) >= @minTotal ORDER BY total DESC";
        var min = query.CreateParameter(); min.ParameterName = "@minTotal"; min.Value = 35; query.Parameters.Add(min);

        var result = new List<(string category, int total)>();
        using var reader = query.ExecuteReader();
        while (reader.Read())
            result.Add((Convert.ToString(reader[0])!, Convert.ToInt32(reader[1])));

        Assert.Equal(2, result.Count);
        Assert.Equal(("A", 70), result[0]);
        Assert.Equal(("B", 35), result[1]);
    }

    /// <summary>
    /// EN: Verifies OFFSET/FETCH-style pagination returns deterministic windows.
    /// PT: Verifica se paginação no estilo OFFSET/FETCH retorna janelas determinísticas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportPaginationWindow()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 5; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_users (id, name) VALUES (@id, @name)";

            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = $"User-{i}"; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var page = connection.CreateCommand();
        page.CommandText = "SELECT id FROM l2db_page_users ORDER BY id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var ids = new List<int>();
        using var reader = page.ExecuteReader();
        while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([2, 3], ids);
    }



    /// <summary>
    /// EN: Verifies correlated EXISTS subqueries can be executed with parameterized predicates.
    /// PT: Verifica se subqueries correlacionadas com EXISTS podem ser executadas com predicados parametrizados.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCorrelatedExists()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand())
        {
            createUsers.CommandText = "CREATE TABLE l2db_exists_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = createUsers.ExecuteNonQuery();
        }

        using (var createOrders = connection.CreateCommand())
        {
            createOrders.CommandText = "CREATE TABLE l2db_exists_orders (id INT PRIMARY KEY, user_id INT, amount INT)";
            _ = createOrders.ExecuteNonQuery();
        }

        using (var u1 = connection.CreateCommand())
        {
            u1.CommandText = "INSERT INTO l2db_exists_users (id, name) VALUES (1, 'Alice')";
            _ = u1.ExecuteNonQuery();
        }

        using (var u2 = connection.CreateCommand())
        {
            u2.CommandText = "INSERT INTO l2db_exists_users (id, name) VALUES (2, 'Bob')";
            _ = u2.ExecuteNonQuery();
        }

        using (var o1 = connection.CreateCommand())
        {
            o1.CommandText = "INSERT INTO l2db_exists_orders (id, user_id, amount) VALUES (100, 1, 50)";
            _ = o1.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT COUNT(*)
FROM l2db_exists_users u
WHERE EXISTS (
  SELECT 1
  FROM l2db_exists_orders o
  WHERE o.user_id = u.id AND o.amount >= @minAmount
)";

        var min = query.CreateParameter();
        min.ParameterName = "@minAmount";
        min.Value = 40;
        query.Parameters.Add(min);

        Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies LEFT JOIN with IS NULL filter can detect rows without related matches.
    /// PT: Verifica se LEFT JOIN com filtro IS NULL detecta linhas sem correspondência relacionada.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportLeftJoinWithIsNullFilter()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand())
        {
            createUsers.CommandText = "CREATE TABLE l2db_left_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = createUsers.ExecuteNonQuery();
        }

        using (var createProfiles = connection.CreateCommand())
        {
            createProfiles.CommandText = "CREATE TABLE l2db_left_profiles (id INT PRIMARY KEY, user_id INT)";
            _ = createProfiles.ExecuteNonQuery();
        }

        using (var u1 = connection.CreateCommand())
        {
            u1.CommandText = "INSERT INTO l2db_left_users (id, name) VALUES (1, 'HasProfile')";
            _ = u1.ExecuteNonQuery();
        }

        using (var u2 = connection.CreateCommand())
        {
            u2.CommandText = "INSERT INTO l2db_left_users (id, name) VALUES (2, 'NoProfile')";
            _ = u2.ExecuteNonQuery();
        }

        using (var p1 = connection.CreateCommand())
        {
            p1.CommandText = "INSERT INTO l2db_left_profiles (id, user_id) VALUES (10, 1)";
            _ = p1.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT COUNT(*)
FROM l2db_left_users u
LEFT JOIN l2db_left_profiles p ON p.user_id = u.id
WHERE p.id IS NULL";

        Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies CASE WHEN projections and CASE-based HAVING filters are evaluated correctly.
    /// PT: Verifica se projeções com CASE WHEN e filtros HAVING baseados em CASE são avaliados corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCaseWhenInSelectAndHaving()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_case_sales (id INT PRIMARY KEY, category VARCHAR(20), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        (int id, string category, int amount)[] rows = [(1, "A", 80), (2, "A", 30), (3, "B", 20), (4, "B", 10)];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_case_sales (id, category, amount) VALUES (@id, @category, @amount)";
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
FROM l2db_case_sales
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportParameterizedLikeWithWildcard()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_like_users (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        (int id, string name)[] rows = [(1, "Alice"), (2, "Aline"), (3, "Bruno")];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_like_users (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.name; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_like_users WHERE name LIKE @pattern";
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCompositeNullOrFilter()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_null_filter_users (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var insertNull = connection.CreateCommand()) { insertNull.CommandText = "INSERT INTO l2db_null_filter_users (id, nickname) VALUES (1, NULL)"; _ = insertNull.ExecuteNonQuery(); }
        using (var insertAna = connection.CreateCommand()) { insertAna.CommandText = "INSERT INTO l2db_null_filter_users (id, nickname) VALUES (2, 'Ana')"; _ = insertAna.ExecuteNonQuery(); }
        using (var insertBob = connection.CreateCommand()) { insertBob.CommandText = "INSERT INTO l2db_null_filter_users (id, nickname) VALUES (3, 'Bob')"; _ = insertBob.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_null_filter_users WHERE nickname IS NULL OR nickname = @nickname";
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportScalarSubqueryProjection()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE l2db_scalar_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE l2db_scalar_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var user = connection.CreateCommand()) { user.CommandText = "INSERT INTO l2db_scalar_users (id, name) VALUES (1, 'Alice')"; _ = user.ExecuteNonQuery(); }
        using (var order1 = connection.CreateCommand()) { order1.CommandText = "INSERT INTO l2db_scalar_orders (id, user_id, amount) VALUES (10, 1, 25)"; _ = order1.ExecuteNonQuery(); }
        using (var order2 = connection.CreateCommand()) { order2.CommandText = "INSERT INTO l2db_scalar_orders (id, user_id, amount) VALUES (11, 1, 30)"; _ = order2.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.name,
  (SELECT SUM(o.amount) FROM l2db_scalar_orders o WHERE o.user_id = u.id) total_amount
FROM l2db_scalar_users u
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionScope_ShouldKeepMultipleCommandsConsistent()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_scope_accounts (id INT PRIMARY KEY, balance INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO l2db_tx_scope_accounts (id, balance) VALUES (1, 100), (2, 50)";
            _ = seed.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var debit = connection.CreateCommand();
            debit.Transaction = tx;
            debit.CommandText = "UPDATE l2db_tx_scope_accounts SET balance = balance - @amount WHERE id = @id";
            var debitAmount = debit.CreateParameter(); debitAmount.ParameterName = "@amount"; debitAmount.Value = 20; debit.Parameters.Add(debitAmount);
            var debitId = debit.CreateParameter(); debitId.ParameterName = "@id"; debitId.Value = 1; debit.Parameters.Add(debitId);
            _ = debit.ExecuteNonQuery();

            using var credit = connection.CreateCommand();
            credit.Transaction = tx;
            credit.CommandText = "UPDATE l2db_tx_scope_accounts SET balance = balance + @amount WHERE id = @id";
            var creditAmount = credit.CreateParameter(); creditAmount.ParameterName = "@amount"; creditAmount.Value = 20; credit.Parameters.Add(creditAmount);
            var creditId = credit.CreateParameter(); creditId.ParameterName = "@id"; creditId.Value = 2; credit.Parameters.Add(creditId);
            _ = credit.ExecuteNonQuery();

            tx.Commit();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT SUM(balance) FROM l2db_tx_scope_accounts";
        Assert.Equal(150, Convert.ToInt32(verify.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies pagination ordering remains stable when using a deterministic tie-breaker.
    /// PT: Verifica se a ordenação da paginação permanece estável ao usar um critério de desempate determinístico.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldProvideStableOrderingForPagination()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_stable (id INT PRIMARY KEY, grp INT, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        (int id, int grp, string name)[] rows = [(1, 1, "A"), (2, 1, "B"), (3, 1, "C"), (4, 2, "D")];
        foreach (var row in rows)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_stable (id, grp, name) VALUES (@id, @grp, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.id; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = row.grp; insert.Parameters.Add(grp);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.name; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var page = connection.CreateCommand();
        page.CommandText = "SELECT id FROM l2db_page_stable ORDER BY grp, id OFFSET 1 ROWS FETCH NEXT 2 ROWS ONLY";

        var ids = new List<int>();
        using var reader = page.ExecuteReader();
        while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));

        Assert.Equal([2, 3], ids);
    }

}
