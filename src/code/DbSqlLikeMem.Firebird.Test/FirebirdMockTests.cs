namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Verifies Firebird command execution, transaction handling, and query helpers against the mock engine.
/// PT: Verifica a execucao de comandos Firebird, o tratamento de transacoes e os helpers de consulta no motor mock.
/// </summary>
public sealed class FirebirdMockTests
    : XUnitTestBase
{
    private readonly FirebirdConnectionMock _connection;

    /// <summary>
    /// EN: Creates the Firebird mock connection used by the test suite.
    /// PT: Cria a conexao mock Firebird usada pela suite de testes.
    /// </summary>
    public FirebirdMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new FirebirdDbMock();
        db.AddTable("Users", [
            new("Id", DbType.Int32, false),
            new("Name", DbType.String, false),
            new("Email", DbType.String, true)
        ]);
        db.AddTable("Orders", [
            new("OrderId", DbType.Int32, false),
            new("UserId", DbType.Int32, false),
            new("Amount", DbType.Decimal, false, decimalPlaces: 2)
        ]);

        _connection = new FirebirdConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Verifies INSERT statements persist rows into the Firebird mock table.
    /// PT: Verifica se instrucoes INSERT persistem linhas na tabela mock do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TestInsert()
    {
        using var command = new FirebirdCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("John Doe", _connection.GetTable("users")[0][1]);
    }

    /// <summary>
    /// EN: Verifies UPDATE statements modify the targeted Firebird row.
    /// PT: Verifica se instrucoes UPDATE modificam a linha alvo no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TestUpdate()
    {
        using var command = new FirebirdCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("Jane Doe", _connection.GetTable("users")[0][1]);
    }

    /// <summary>
    /// EN: Verifies DELETE statements remove the targeted Firebird row.
    /// PT: Verifica se instrucoes DELETE removem a linha alvo no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TestDelete()
    {
        using var command = new FirebirdCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "DELETE FROM Users WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Empty(_connection.GetTable("users"));
    }

    /// <summary>
    /// EN: Verifies committed transactions preserve inserted Firebird rows.
    /// PT: Verifica se transacoes confirmadas preservam as linhas inseridas no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new FirebirdCommandMock(_connection, (FirebirdTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new FirebirdCommandMock(_connection)
        {
            CommandText = "SELECT * FROM Users"
        };
        using var reader = queryCommand.ExecuteReader();
        var users = new List<Dictionary<int, object>>();
        while (reader.Read())
        {
            var user = new Dictionary<int, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                user[i] = reader.GetValue(i);
            }
            users.Add(user);
        }
        Assert.Single(users);
    }

    /// <summary>
    /// EN: Verifies rolled back transactions discard inserted Firebird rows.
    /// PT: Verifica se transacoes revertidas descartam as linhas inseridas no Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new FirebirdCommandMock(_connection, (FirebirdTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new FirebirdCommandMock(_connection)
        {
            CommandText = "SELECT * FROM Users"
        };
        using var reader = queryCommand.ExecuteReader();
        var users = new List<Dictionary<int, object>>();
        while (reader.Read())
        {
            var user = new Dictionary<int, object>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                user[i] = reader.GetValue(i);
            }
            users.Add(user);
        }
        Assert.Empty(users);
    }
}
