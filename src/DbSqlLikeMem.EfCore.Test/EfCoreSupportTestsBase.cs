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

}