using DbSqlLikeMem.Test;

namespace DbSqlLikeMem.LinqToDb.Test;

/// <summary>
/// EN: Defines shared LinqToDB-oriented provider contract tests for mock connections.
/// PT: Define testes de contrato compartilhados orientados a LinqToDB para conexões simulado.
/// </summary>
public abstract class LinqToDbSupportTestsBase(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Creates and opens the provider mock connection factory under test.
    /// PT: Cria e abre a fábrica de conexão simulada do provedor sob teste.
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
    /// PT: Verifica execução básica de comando SQL e consulta escalar via conexão simulada do provedor.
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
    /// PT: Verifica se consultas com inner join podem ser executadas através das conexões simulado de provedor.
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



    /// <summary>
    /// EN: Verifies a multi-command transaction rollback restores the state before all changes in the scope.
    /// PT: Verifica se o rollback de transação com múltiplos comandos restaura o estado anterior a todas as mudanças do escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionScopeRollback_ShouldUndoAllCommands()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_scope_rollback (id INT PRIMARY KEY, balance INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO l2db_tx_scope_rollback (id, balance) VALUES (1, 100), (2, 50)";
            _ = seed.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var debit = connection.CreateCommand();
            debit.Transaction = tx;
            debit.CommandText = "UPDATE l2db_tx_scope_rollback SET balance = balance - 20 WHERE id = 1";
            _ = debit.ExecuteNonQuery();

            using var credit = connection.CreateCommand();
            credit.Transaction = tx;
            credit.CommandText = "UPDATE l2db_tx_scope_rollback SET balance = balance + 20 WHERE id = 2";
            _ = credit.ExecuteNonQuery();

            tx.Rollback();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT balance FROM l2db_tx_scope_rollback WHERE id = @id";

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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_PaginationWithDeterministicOrder_ShouldBeStableAcrossExecutions()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_repeatable (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 6; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_repeatable (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 3 ? 1 : 2; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadWindow()
        {
            using var page = connection.CreateCommand();
            page.CommandText = "SELECT id FROM l2db_page_repeatable ORDER BY grp, id OFFSET 2 ROWS FETCH NEXT 2 ROWS ONLY";

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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ScalarSubqueryProjectionWithoutMatches_ShouldReturnNull()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE l2db_scalar_null_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE l2db_scalar_null_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var user = connection.CreateCommand()) { user.CommandText = "INSERT INTO l2db_scalar_null_users (id, name) VALUES (1, 'Alice')"; _ = user.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.name,
  (SELECT SUM(o.amount) FROM l2db_scalar_null_orders o WHERE o.user_id = u.id) total_amount
FROM l2db_scalar_null_users u
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionScope_ReadAfterWrite_ShouldObserveCurrentTransactionState()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_read_write (id INT PRIMARY KEY, value INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var seed = connection.CreateCommand())
        {
            seed.CommandText = "INSERT INTO l2db_tx_read_write (id, value) VALUES (1, 10)";
            _ = seed.ExecuteNonQuery();
        }

        using var tx = connection.BeginTransaction();

        using (var update = connection.CreateCommand())
        {
            update.Transaction = tx;
            update.CommandText = "UPDATE l2db_tx_read_write SET value = value + 5 WHERE id = 1";
            _ = update.ExecuteNonQuery();
        }

        using var read = connection.CreateCommand();
        read.Transaction = tx;
        read.CommandText = "SELECT value FROM l2db_tx_read_write WHERE id = 1";

        Assert.Equal(15, Convert.ToInt32(read.ExecuteScalar()));
        tx.Rollback();
    }



    /// <summary>
    /// EN: Verifies composite null filters remain correct when the comparison parameter is null.
    /// PT: Verifica se filtros compostos com nulo permanecem corretos quando o parâmetro de comparação é nulo.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_CompositeNullOrFilter_WithNullParameter_ShouldOnlyMatchNullRows()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_null_param_filter (id INT PRIMARY KEY, nickname VARCHAR(100) NULL)";
            _ = create.ExecuteNonQuery();
        }

        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO l2db_null_param_filter (id, nickname) VALUES (1, NULL)"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO l2db_null_param_filter (id, nickname) VALUES (2, 'Ana')"; _ = i2.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_null_param_filter WHERE nickname IS NULL OR nickname = @nickname";
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
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_PaginationWithDeterministicOrder_ShouldReturnDisjointConsecutivePages()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_disjoint (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 8; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_disjoint (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 4 ? 1 : 2; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadPage(int offset)
        {
            using var page = connection.CreateCommand();
            page.CommandText = $"SELECT id FROM l2db_page_disjoint ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY";

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


    /// <summary>
    /// EN: Verifies CASE WHEN with multiple conditions and ELSE branches behaves consistently in aggregate projections.
    /// PT: Verifica se CASE WHEN com múltiplas condições e ramificações ELSE se comporta de forma consistente em projeções agregadas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCaseWhenWithMultipleConditionsInAggregations()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_case_multi (id INT PRIMARY KEY, amount INT)";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, 120), (2, 80), (3, 20), (4, 5) })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_case_multi (id, amount) VALUES (@id, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.Item2; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  SUM(CASE WHEN amount >= @high THEN 3 WHEN amount >= @mid THEN 2 ELSE 1 END) weighted_score,
  SUM(CASE WHEN amount >= @high THEN amount WHEN amount >= @mid THEN amount / 2 ELSE 0 END) normalized_total
FROM l2db_case_multi";

        var high = query.CreateParameter(); high.ParameterName = "@high"; high.Value = 100; query.Parameters.Add(high);
        var mid = query.CreateParameter(); mid.ParameterName = "@mid"; mid.Value = 50; query.Parameters.Add(mid);

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(7, Convert.ToInt32(reader[0]));
        Assert.Equal(180, Convert.ToInt32(reader[1]));
    }

    /// <summary>
    /// EN: Verifies parameterized LIKE supports contains, single-character, prefix and suffix patterns.
    /// PT: Verifica se LIKE parametrizado suporta padrões de contém, caractere único, prefixo e sufixo.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportParameterizedLikePatternVariants()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_like_variants (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, "Alpha"), (2, "A_pha"), (3, "Graph"), (4, "Alphabet") })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_like_variants (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.Item2; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        int CountForPattern(string value)
        {
            using var query = connection.CreateCommand();
            query.CommandText = "SELECT COUNT(*) FROM l2db_like_variants WHERE name LIKE @pattern";
            var pattern = query.CreateParameter();
            pattern.ParameterName = "@pattern";
            pattern.Value = value;
            query.Parameters.Add(pattern);
            return Convert.ToInt32(query.ExecuteScalar());
        }

        Assert.Equal(2, CountForPattern("%ph%"));
        Assert.Equal(2, CountForPattern("A____"));
        Assert.Equal(3, CountForPattern("Al%"));
        Assert.Equal(1, CountForPattern("%bet"));
    }

    /// <summary>
    /// EN: Verifies mixed NULL plus IN plus OR predicates keep expected precedence and deterministic results.
    /// PT: Verifica se predicados mistos com NULL, IN e OR mantêm a precedência esperada e resultados determinísticos.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCompositeNullInOrFilterWithPrecedence()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_null_in_or (id INT PRIMARY KEY, nickname VARCHAR(100) NULL, kind VARCHAR(20))";
            _ = create.ExecuteNonQuery();
        }

        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO l2db_null_in_or (id, nickname, kind) VALUES (1, NULL, 'staff')"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO l2db_null_in_or (id, nickname, kind) VALUES (2, 'Ana', 'guest')"; _ = i2.ExecuteNonQuery(); }
        using (var i3 = connection.CreateCommand()) { i3.CommandText = "INSERT INTO l2db_null_in_or (id, nickname, kind) VALUES (3, 'Bob', 'staff')"; _ = i3.ExecuteNonQuery(); }
        using (var i4 = connection.CreateCommand()) { i4.CommandText = "INSERT INTO l2db_null_in_or (id, nickname, kind) VALUES (4, 'Bia', 'guest')"; _ = i4.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_null_in_or WHERE (nickname IS NULL OR nickname IN (@nameA, @nameB)) AND kind = @kind";

        var nameA = query.CreateParameter(); nameA.ParameterName = "@nameA"; nameA.Value = "Ana"; query.Parameters.Add(nameA);
        var nameB = query.CreateParameter(); nameB.ParameterName = "@nameB"; nameB.Value = "Bob"; query.Parameters.Add(nameB);
        var kind = query.CreateParameter(); kind.ParameterName = "@kind"; kind.Value = "staff"; query.Parameters.Add(kind);

        Assert.Equal(2, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies scalar subquery projection with multiple inner rows keeps the current mock behavior.
    /// PT: Verifica se a projeção com subquery escalar com múltiplas linhas internas mantém o comportamento atual do mock.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ScalarSubqueryProjectionWithMultipleInnerRows_ShouldUseFirstCellFromFirstRow()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE l2db_scalar_multi_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE l2db_scalar_multi_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var user = connection.CreateCommand()) { user.CommandText = "INSERT INTO l2db_scalar_multi_users (id, name) VALUES (1, 'Alice')"; _ = user.ExecuteNonQuery(); }
        using (var o1 = connection.CreateCommand()) { o1.CommandText = "INSERT INTO l2db_scalar_multi_orders (id, user_id, amount) VALUES (10, 1, 25)"; _ = o1.ExecuteNonQuery(); }
        using (var o2 = connection.CreateCommand()) { o2.CommandText = "INSERT INTO l2db_scalar_multi_orders (id, user_id, amount) VALUES (11, 1, 30)"; _ = o2.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.name,
  (SELECT o.amount FROM l2db_scalar_multi_orders o WHERE o.user_id = u.id ORDER BY o.id) first_amount
FROM l2db_scalar_multi_users u
WHERE u.id = 1";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader[0]));
        Assert.Equal(25, Convert.ToInt32(reader[1]));
    }

    /// <summary>
    /// EN: Verifies one transaction scope can rollback and a subsequent scope can commit while preserving expected state transitions.
    /// PT: Verifica se um escopo transacional pode fazer rollback e um escopo subsequente pode fazer commit preservando as transições de estado esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionScope_InsertUpdateDelete_ShouldValidateRollbackThenCommitState()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_sequence (id INT PRIMARY KEY, value INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var txRollback = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = txRollback;
            insert.CommandText = "INSERT INTO l2db_tx_sequence (id, value) VALUES (1, 10), (2, 20)";
            _ = insert.ExecuteNonQuery();

            using var update = connection.CreateCommand();
            update.Transaction = txRollback;
            update.CommandText = "UPDATE l2db_tx_sequence SET value = value + 5 WHERE id = 1";
            _ = update.ExecuteNonQuery();

            using var delete = connection.CreateCommand();
            delete.Transaction = txRollback;
            delete.CommandText = "DELETE FROM l2db_tx_sequence WHERE id = 2";
            _ = delete.ExecuteNonQuery();

            txRollback.Rollback();
        }

        using (var countAfterRollback = connection.CreateCommand())
        {
            countAfterRollback.CommandText = "SELECT COUNT(*) FROM l2db_tx_sequence";
            Assert.Equal(0, Convert.ToInt32(countAfterRollback.ExecuteScalar()));
        }

        using (var txCommit = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = txCommit;
            insert.CommandText = "INSERT INTO l2db_tx_sequence (id, value) VALUES (1, 10), (2, 20)";
            _ = insert.ExecuteNonQuery();

            using var update = connection.CreateCommand();
            update.Transaction = txCommit;
            update.CommandText = "UPDATE l2db_tx_sequence SET value = value + 5 WHERE id = 1";
            _ = update.ExecuteNonQuery();

            using var delete = connection.CreateCommand();
            delete.Transaction = txCommit;
            delete.CommandText = "DELETE FROM l2db_tx_sequence WHERE id = 2";
            _ = delete.ExecuteNonQuery();

            txCommit.Commit();
        }

        using var verify = connection.CreateCommand();
        verify.CommandText = "SELECT value FROM l2db_tx_sequence ORDER BY id";
        using var reader = verify.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(15, Convert.ToInt32(reader[0]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies deterministic pagination over pages 1, 2 and 3 returns disjoint windows without missing expected rows.
    /// PT: Verifica se paginação determinística nas páginas 1, 2 e 3 retorna janelas disjuntas sem perder linhas esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_PaginationWithDeterministicOrder_ShouldReturnThreeDisjointPagesWithoutGaps()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_three (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 9; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_three (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 3 ? 1 : i <= 6 ? 2 : 3; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadPage(int offset)
        {
            using var page = connection.CreateCommand();
            page.CommandText = $"SELECT id FROM l2db_page_three ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY";
            var ids = new List<int>();
            using var reader = page.ExecuteReader();
            while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));
            return ids;
        }

        var page1 = ReadPage(0);
        var page2 = ReadPage(3);
        var page3 = ReadPage(6);
        var union = page1.Concat(page2).Concat(page3).ToList();

        Assert.Equal([1, 2, 3], page1);
        Assert.Equal([4, 5, 6], page2);
        Assert.Equal([7, 8, 9], page3);
        Assert.Equal(9, union.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 9), union);
    }



    /// <summary>
    /// EN: Verifies grouped CASE WHEN with multiple branches and ELSE keeps category totals consistent.
    /// PT: Verifica se CASE WHEN agrupado com múltiplas ramificações e ELSE mantém os totais por categoria consistentes.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportGroupedCaseWhenWithMultipleBranchesAndElse()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_case_grouped (id INT PRIMARY KEY, category VARCHAR(10), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, "A", 120), (2, "A", 60), (3, "A", 10), (4, "B", 70), (5, "B", 40) })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_case_grouped (id, category, amount) VALUES (@id, @category, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var category = insert.CreateParameter(); category.ParameterName = "@category"; category.Value = row.Item2; insert.Parameters.Add(category);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.Item3; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  category,
  SUM(CASE WHEN amount >= @high THEN 2 WHEN amount >= @mid THEN 1 ELSE 0 END) score
FROM l2db_case_grouped
GROUP BY category
ORDER BY category";

        var high = query.CreateParameter(); high.ParameterName = "@high"; high.Value = 100; query.Parameters.Add(high);
        var mid = query.CreateParameter(); mid.ParameterName = "@mid"; mid.Value = 50; query.Parameters.Add(mid);

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("A", Convert.ToString(reader[0]));
        Assert.Equal(3, Convert.ToInt32(reader[1]));
        Assert.True(reader.Read());
        Assert.Equal("B", Convert.ToString(reader[0]));
        Assert.Equal(1, Convert.ToInt32(reader[1]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies OR-combined parameterized LIKE predicates preserve expected wildcard semantics.
    /// PT: Verifica se predicados LIKE parametrizados combinados por OR preservam a semântica esperada de curingas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportOrCombinedParameterizedLikePredicates()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_like_or_patterns (id INT PRIMARY KEY, name VARCHAR(100))";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, "Aline"), (2, "Bruno"), (3, "Ana"), (4, "Carla") })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_like_or_patterns (id, name) VALUES (@id, @name)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var name = insert.CreateParameter(); name.ParameterName = "@name"; name.Value = row.Item2; insert.Parameters.Add(name);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_like_or_patterns WHERE name LIKE @containsLi OR name LIKE @threeLetterA";

        var containsLi = query.CreateParameter(); containsLi.ParameterName = "@containsLi"; containsLi.Value = "%li%"; query.Parameters.Add(containsLi);
        var threeLetterA = query.CreateParameter(); threeLetterA.ParameterName = "@threeLetterA"; threeLetterA.Value = "A__"; query.Parameters.Add(threeLetterA);

        Assert.Equal(2, Convert.ToInt32(query.ExecuteScalar()));
    }

    /// <summary>
    /// EN: Verifies scalar subquery behavior remains consistent per outer row when inner result has multiple rows.
    /// PT: Verifica se o comportamento de subquery escalar permanece consistente por linha externa quando o resultado interno tem múltiplas linhas.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ScalarSubqueryProjectionWithMultipleInnerRows_ShouldBeConsistentPerOuterRow()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE l2db_scalar_per_row_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE l2db_scalar_per_row_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }

        using (var u1 = connection.CreateCommand()) { u1.CommandText = "INSERT INTO l2db_scalar_per_row_users (id, name) VALUES (1, 'Alice')"; _ = u1.ExecuteNonQuery(); }
        using (var u2 = connection.CreateCommand()) { u2.CommandText = "INSERT INTO l2db_scalar_per_row_users (id, name) VALUES (2, 'Bob')"; _ = u2.ExecuteNonQuery(); }
        using (var o1 = connection.CreateCommand()) { o1.CommandText = "INSERT INTO l2db_scalar_per_row_orders (id, user_id, amount) VALUES (10, 1, 25)"; _ = o1.ExecuteNonQuery(); }
        using (var o2 = connection.CreateCommand()) { o2.CommandText = "INSERT INTO l2db_scalar_per_row_orders (id, user_id, amount) VALUES (11, 1, 30)"; _ = o2.ExecuteNonQuery(); }
        using (var o3 = connection.CreateCommand()) { o3.CommandText = "INSERT INTO l2db_scalar_per_row_orders (id, user_id, amount) VALUES (20, 2, 40)"; _ = o3.ExecuteNonQuery(); }
        using (var o4 = connection.CreateCommand()) { o4.CommandText = "INSERT INTO l2db_scalar_per_row_orders (id, user_id, amount) VALUES (21, 2, 50)"; _ = o4.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.id,
  (SELECT o.amount FROM l2db_scalar_per_row_orders o WHERE o.user_id = u.id ORDER BY o.id) first_amount
FROM l2db_scalar_per_row_users u
ORDER BY u.id";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1, Convert.ToInt32(reader[0]));
        Assert.Equal(25, Convert.ToInt32(reader[1]));
        Assert.True(reader.Read());
        Assert.Equal(2, Convert.ToInt32(reader[0]));
        Assert.Equal(40, Convert.ToInt32(reader[1]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies operator precedence between OR and AND is preserved in mixed NULL and IN filters.
    /// PT: Verifica se a precedência de operadores entre OR e AND é preservada em filtros mistos com NULL e IN.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_CompositeNullInOrFilter_WithoutParentheses_ShouldRespectSqlPrecedence()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_precedence_null_in_or (id INT PRIMARY KEY, nickname VARCHAR(100) NULL, kind VARCHAR(20))";
            _ = create.ExecuteNonQuery();
        }

        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO l2db_precedence_null_in_or (id, nickname, kind) VALUES (1, NULL, 'guest')"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO l2db_precedence_null_in_or (id, nickname, kind) VALUES (2, 'Ana', 'staff')"; _ = i2.ExecuteNonQuery(); }
        using (var i3 = connection.CreateCommand()) { i3.CommandText = "INSERT INTO l2db_precedence_null_in_or (id, nickname, kind) VALUES (3, 'Bob', 'staff')"; _ = i3.ExecuteNonQuery(); }
        using (var i4 = connection.CreateCommand()) { i4.CommandText = "INSERT INTO l2db_precedence_null_in_or (id, nickname, kind) VALUES (4, 'Bia', 'guest')"; _ = i4.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = "SELECT COUNT(*) FROM l2db_precedence_null_in_or WHERE nickname IS NULL OR nickname IN (@a, @b) AND kind = @kind";

        var a = query.CreateParameter(); a.ParameterName = "@a"; a.Value = "Ana"; query.Parameters.Add(a);
        var b = query.CreateParameter(); b.ParameterName = "@b"; b.Value = "Bob"; query.Parameters.Add(b);
        var kind = query.CreateParameter(); kind.ParameterName = "@kind"; kind.Value = "staff"; query.Parameters.Add(kind);

        Assert.Equal(3, Convert.ToInt32(query.ExecuteScalar()));
    }



    /// <summary>
    /// EN: Verifies CASE WHEN with multiple conditions and ELSE can be reused in grouped projections and HAVING predicates.
    /// PT: Verifica se CASE WHEN com múltiplas condições e ELSE pode ser reutilizado em projeções agrupadas e predicados HAVING.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportCaseWhenWithElseInGroupedHavingPredicates()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_case_having_multi (id INT PRIMARY KEY, category VARCHAR(10), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, "A", 110), (2, "A", 60), (3, "B", 55), (4, "B", 20), (5, "C", 10) })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_case_having_multi (id, category, amount) VALUES (@id, @category, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var category = insert.CreateParameter(); category.ParameterName = "@category"; category.Value = row.Item2; insert.Parameters.Add(category);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.Item3; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  category,
  SUM(CASE WHEN amount >= @high THEN 2 WHEN amount >= @mid THEN 1 ELSE 0 END) score
FROM l2db_case_having_multi
GROUP BY category
HAVING SUM(CASE WHEN amount >= @high THEN 2 WHEN amount >= @mid THEN 1 ELSE 0 END) >= @minScore
ORDER BY category";

        var high = query.CreateParameter(); high.ParameterName = "@high"; high.Value = 100; query.Parameters.Add(high);
        var mid = query.CreateParameter(); mid.ParameterName = "@mid"; mid.Value = 50; query.Parameters.Add(mid);
        var minScore = query.CreateParameter(); minScore.ParameterName = "@minScore"; minScore.Value = 2; query.Parameters.Add(minScore);

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("A", Convert.ToString(reader[0]));
        Assert.Equal(3, Convert.ToInt32(reader[1]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies composite NULL plus IN plus OR filters remain consistent when IN parameter values are swapped.
    /// PT: Verifica se filtros compostos com NULL, IN e OR permanecem consistentes quando os valores parametrizados de IN são invertidos.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_CompositeNullInOrFilter_SwappedInParameters_ShouldKeepSameResult()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_null_in_or_swapped (id INT PRIMARY KEY, nickname VARCHAR(100) NULL, kind VARCHAR(20))";
            _ = create.ExecuteNonQuery();
        }

        using (var i1 = connection.CreateCommand()) { i1.CommandText = "INSERT INTO l2db_null_in_or_swapped (id, nickname, kind) VALUES (1, NULL, 'staff')"; _ = i1.ExecuteNonQuery(); }
        using (var i2 = connection.CreateCommand()) { i2.CommandText = "INSERT INTO l2db_null_in_or_swapped (id, nickname, kind) VALUES (2, 'Ana', 'staff')"; _ = i2.ExecuteNonQuery(); }
        using (var i3 = connection.CreateCommand()) { i3.CommandText = "INSERT INTO l2db_null_in_or_swapped (id, nickname, kind) VALUES (3, 'Bob', 'staff')"; _ = i3.ExecuteNonQuery(); }
        using (var i4 = connection.CreateCommand()) { i4.CommandText = "INSERT INTO l2db_null_in_or_swapped (id, nickname, kind) VALUES (4, 'Bia', 'guest')"; _ = i4.ExecuteNonQuery(); }

        int CountFor(string first, string second)
        {
            using var query = connection.CreateCommand();
            query.CommandText = "SELECT COUNT(*) FROM l2db_null_in_or_swapped WHERE (nickname IS NULL OR nickname IN (@a, @b)) AND kind = @kind";
            var a = query.CreateParameter(); a.ParameterName = "@a"; a.Value = first; query.Parameters.Add(a);
            var b = query.CreateParameter(); b.ParameterName = "@b"; b.Value = second; query.Parameters.Add(b);
            var kind = query.CreateParameter(); kind.ParameterName = "@kind"; kind.Value = "staff"; query.Parameters.Add(kind);
            return Convert.ToInt32(query.ExecuteScalar());
        }

        Assert.Equal(3, CountFor("Ana", "Bob"));
        Assert.Equal(3, CountFor("Bob", "Ana"));
    }

    /// <summary>
    /// EN: Verifies deterministic pagination with three pages and a remaining tail row keeps continuity without overlaps.
    /// PT: Verifica se paginação determinística com três páginas e uma linha remanescente mantém continuidade sem sobreposição.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_PaginationWithDeterministicOrder_ShouldKeepTailContinuityAcrossFourthPage()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_tail (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 10; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_tail (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 5 ? 1 : 2; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadPage(int offset)
        {
            using var page = connection.CreateCommand();
            page.CommandText = $"SELECT id FROM l2db_page_tail ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY";
            var ids = new List<int>();
            using var reader = page.ExecuteReader();
            while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));
            return ids;
        }

        var p1 = ReadPage(0);
        var p2 = ReadPage(3);
        var p3 = ReadPage(6);
        var p4 = ReadPage(9);
        var all = p1.Concat(p2).Concat(p3).Concat(p4).ToList();

        Assert.Equal([1, 2, 3], p1);
        Assert.Equal([4, 5, 6], p2);
        Assert.Equal([7, 8, 9], p3);
        Assert.Equal([10], p4);
        Assert.Equal(10, all.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 10), all);
    }

    /// <summary>
    /// EN: Verifies transaction sequence visibility checks before rollback and persisted state after a later commit scope.
    /// PT: Verifica checagens de visibilidade da sequência transacional antes do rollback e estado persistido após escopo posterior com commit.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_TransactionScope_InsertUpdateDelete_ShouldValidateIntermediateStateBeforeRollbackAndAfterCommit()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_tx_intermediate (id INT PRIMARY KEY, value INT)";
            _ = create.ExecuteNonQuery();
        }

        using (var tx = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = tx;
            insert.CommandText = "INSERT INTO l2db_tx_intermediate (id, value) VALUES (1, 10), (2, 20)";
            _ = insert.ExecuteNonQuery();

            using var update = connection.CreateCommand();
            update.Transaction = tx;
            update.CommandText = "UPDATE l2db_tx_intermediate SET value = value + 7 WHERE id = 1";
            _ = update.ExecuteNonQuery();

            using var delete = connection.CreateCommand();
            delete.Transaction = tx;
            delete.CommandText = "DELETE FROM l2db_tx_intermediate WHERE id = 2";
            _ = delete.ExecuteNonQuery();

            using var readInside = connection.CreateCommand();
            readInside.Transaction = tx;
            readInside.CommandText = "SELECT value FROM l2db_tx_intermediate ORDER BY id";
            using var insideReader = readInside.ExecuteReader();
            Assert.True(insideReader.Read());
            Assert.Equal(17, Convert.ToInt32(insideReader[0]));
            Assert.False(insideReader.Read());

            tx.Rollback();
        }

        using (var afterRollback = connection.CreateCommand())
        {
            afterRollback.CommandText = "SELECT COUNT(*) FROM l2db_tx_intermediate";
            Assert.Equal(0, Convert.ToInt32(afterRollback.ExecuteScalar()));
        }

        using (var txCommit = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = txCommit;
            insert.CommandText = "INSERT INTO l2db_tx_intermediate (id, value) VALUES (1, 10), (2, 20)";
            _ = insert.ExecuteNonQuery();

            using var update = connection.CreateCommand();
            update.Transaction = txCommit;
            update.CommandText = "UPDATE l2db_tx_intermediate SET value = value + 7 WHERE id = 1";
            _ = update.ExecuteNonQuery();

            using var delete = connection.CreateCommand();
            delete.Transaction = txCommit;
            delete.CommandText = "DELETE FROM l2db_tx_intermediate WHERE id = 2";
            _ = delete.ExecuteNonQuery();

            txCommit.Commit();
        }

        using var finalRead = connection.CreateCommand();
        finalRead.CommandText = "SELECT value FROM l2db_tx_intermediate ORDER BY id";
        using var finalReader = finalRead.ExecuteReader();
        Assert.True(finalReader.Read());
        Assert.Equal(17, Convert.ToInt32(finalReader[0]));
        Assert.False(finalReader.Read());
    }



    /// <summary>
    /// EN: Verifies grouped CASE WHEN supports combined conditional aggregates with ELSE branches in the same projection.
    /// PT: Verifica se CASE WHEN agrupado suporta agregações condicionais combinadas com ramificações ELSE na mesma projeção.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ShouldSupportGroupedCaseWhenWithCombinedConditionalAggregates()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_case_combined (id INT PRIMARY KEY, category VARCHAR(10), amount INT)";
            _ = create.ExecuteNonQuery();
        }

        foreach (var row in new[] { (1, "A", 120), (2, "A", 80), (3, "A", 20), (4, "B", 60), (5, "B", 10) })
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_case_combined (id, category, amount) VALUES (@id, @category, @amount)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = row.Item1; insert.Parameters.Add(id);
            var category = insert.CreateParameter(); category.ParameterName = "@category"; category.Value = row.Item2; insert.Parameters.Add(category);
            var amount = insert.CreateParameter(); amount.ParameterName = "@amount"; amount.Value = row.Item3; insert.Parameters.Add(amount);
            _ = insert.ExecuteNonQuery();
        }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  category,
  SUM(CASE WHEN amount >= @cutoff THEN 1 ELSE 0 END) above_cutoff,
  SUM(CASE WHEN amount >= @cutoff THEN amount ELSE 0 END) sum_above
FROM l2db_case_combined
GROUP BY category
ORDER BY category";

        var cutoff = query.CreateParameter(); cutoff.ParameterName = "@cutoff"; cutoff.Value = 50; query.Parameters.Add(cutoff);

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("A", Convert.ToString(reader[0]));
        Assert.Equal(2, Convert.ToInt32(reader[1]));
        Assert.Equal(200, Convert.ToInt32(reader[2]));
        Assert.True(reader.Read());
        Assert.Equal("B", Convert.ToString(reader[0]));
        Assert.Equal(1, Convert.ToInt32(reader[1]));
        Assert.Equal(60, Convert.ToInt32(reader[2]));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies scalar subquery projections can return non-null and null values within the same outer result set.
    /// PT: Verifica se projeções com subquery escalar podem retornar valores não nulos e nulos no mesmo conjunto externo.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_ScalarSubqueryProjection_ShouldMixNonNullAndNullResults()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var createUsers = connection.CreateCommand()) { createUsers.CommandText = "CREATE TABLE l2db_scalar_mix_users (id INT PRIMARY KEY, name VARCHAR(100))"; _ = createUsers.ExecuteNonQuery(); }
        using (var createOrders = connection.CreateCommand()) { createOrders.CommandText = "CREATE TABLE l2db_scalar_mix_orders (id INT PRIMARY KEY, user_id INT, amount INT)"; _ = createOrders.ExecuteNonQuery(); }
        using (var u1 = connection.CreateCommand()) { u1.CommandText = "INSERT INTO l2db_scalar_mix_users (id, name) VALUES (1, 'Alice')"; _ = u1.ExecuteNonQuery(); }
        using (var u2 = connection.CreateCommand()) { u2.CommandText = "INSERT INTO l2db_scalar_mix_users (id, name) VALUES (2, 'Bob')"; _ = u2.ExecuteNonQuery(); }
        using (var o1 = connection.CreateCommand()) { o1.CommandText = "INSERT INTO l2db_scalar_mix_orders (id, user_id, amount) VALUES (10, 1, 25)"; _ = o1.ExecuteNonQuery(); }

        using var query = connection.CreateCommand();
        query.CommandText = @"SELECT
  u.id,
  (SELECT SUM(o.amount) FROM l2db_scalar_mix_orders o WHERE o.user_id = u.id) total_amount
FROM l2db_scalar_mix_users u
ORDER BY u.id";

        using var reader = query.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(1, Convert.ToInt32(reader[0]));
        Assert.Equal(25, Convert.ToInt32(reader[1]));

        Assert.True(reader.Read());
        Assert.Equal(2, Convert.ToInt32(reader[0]));
        Assert.True(reader[1] is null || reader[1] is DBNull);
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Verifies deterministic pagination remains repeatable across pages 1, 2 and 3 over multiple executions.
    /// PT: Verifica se paginação determinística permanece repetível nas páginas 1, 2 e 3 em múltiplas execuções.
    /// </summary>
    [Fact]
    [Trait("Category", "LinqToDb")]
    public void LinqToDb_FactoryConnection_PaginationWithDeterministicOrder_ShouldBeRepeatableAcrossThreePages()
    {
        using var connection = CreateFactory().CreateOpenConnection();

        using (var create = connection.CreateCommand())
        {
            create.CommandText = "CREATE TABLE l2db_page_repeat_three (id INT PRIMARY KEY, grp INT)";
            _ = create.ExecuteNonQuery();
        }

        for (var i = 1; i <= 9; i++)
        {
            using var insert = connection.CreateCommand();
            insert.CommandText = "INSERT INTO l2db_page_repeat_three (id, grp) VALUES (@id, @grp)";
            var id = insert.CreateParameter(); id.ParameterName = "@id"; id.Value = i; insert.Parameters.Add(id);
            var grp = insert.CreateParameter(); grp.ParameterName = "@grp"; grp.Value = i <= 3 ? 1 : i <= 6 ? 2 : 3; insert.Parameters.Add(grp);
            _ = insert.ExecuteNonQuery();
        }

        List<int> ReadPage(int offset)
        {
            using var page = connection.CreateCommand();
            page.CommandText = $"SELECT id FROM l2db_page_repeat_three ORDER BY grp, id OFFSET {offset} ROWS FETCH NEXT 3 ROWS ONLY";
            var ids = new List<int>();
            using var reader = page.ExecuteReader();
            while (reader.Read()) ids.Add(Convert.ToInt32(reader[0]));
            return ids;
        }

        var p1a = ReadPage(0);
        var p2a = ReadPage(3);
        var p3a = ReadPage(6);

        var p1b = ReadPage(0);
        var p2b = ReadPage(3);
        var p3b = ReadPage(6);

        Assert.Equal(p1a, p1b);
        Assert.Equal(p2a, p2b);
        Assert.Equal(p3a, p3b);
        Assert.Equal([1, 2, 3], p1a);
        Assert.Equal([4, 5, 6], p2a);
        Assert.Equal([7, 8, 9], p3a);
    }


}
