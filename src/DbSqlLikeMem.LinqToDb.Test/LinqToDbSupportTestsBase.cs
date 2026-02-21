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
}
