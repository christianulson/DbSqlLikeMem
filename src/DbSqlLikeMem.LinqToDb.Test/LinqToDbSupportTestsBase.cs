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

}