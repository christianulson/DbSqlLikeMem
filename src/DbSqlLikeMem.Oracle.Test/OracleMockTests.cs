namespace DbSqlLikeMem.Oracle.Test;

public sealed class OracleMockTests
    : XUnitTestBase
{
    private readonly OracleConnectionMock _connection;

    public OracleMockTests(
        ITestOutputHelper helper
        ) : base(helper)
    {
        var db = new OracleDbMock();
        db.AddTable("Users", new ColumnDictionary {
            { "Id", new(0, DbType.Int32, false) },
            { "Name", new(1, DbType.String, false) },
            { "Email", new(2, DbType.String, true) }
        });
        db.AddTable("Orders", new ColumnDictionary {
            { "OrderId", new(0, DbType.Int32, false) },
            { "UserId", new(1, DbType.Int32, false) },
            { "Amount", new(0, DbType.Decimal, false) }
        });

        _connection = new OracleConnectionMock(db);
        _connection.Open();
    }

    [Fact]
    public void TestInsert()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("John Doe",_connection.GetTable("user")[0][1]);
    }

    [Fact]
    public void TestUpdate()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Equal("Jane Doe",_connection.GetTable("user")[0][1]);
    }

    [Fact]
    public void TestDelete()
    {
        using var command = new OracleCommandMock(_connection)
        {
            CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
        };
        command.ExecuteNonQuery();

        command.CommandText = "DELETE FROM Users WHERE Id = 1";
        var rowsAffected = command.ExecuteNonQuery();
        Assert.Equal(1, rowsAffected);
        Assert.Empty(_connection.GetTable("user"));
    }

    [Fact]
    public void TestTransactionCommit()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new OracleCommandMock(_connection, (OracleTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Commit();
        }

        using var queryCommand = new OracleCommandMock(_connection)
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

    [Fact]
    public void TestTransactionCommitInsertUpdate()
    {
        using var cmd = _connection.CreateCommand();
        cmd.CommandText = "INSERT INTO users (id, name) VALUES (1, 'Alice')";
        cmd.ExecuteNonQuery();

        _connection.BeginTransaction();
        cmd.CommandText = "UPDATE users SET name = 'Bob' WHERE id = 1";
        cmd.ExecuteNonQuery();
        _connection.CommitTransaction();

        cmd.CommandText = "SELECT name FROM users WHERE id = 1";
        var name = (string?)cmd.ExecuteScalar();

        Assert.Equal("Bob", name);
    }

    [Fact]
    public void TestTransactionRollback()
    {
        using (var transaction = _connection.BeginTransaction())
        {
            using var command = new OracleCommandMock(_connection, (OracleTransactionMock)transaction)
            {
                CommandText = "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')"
            };
            command.ExecuteNonQuery();
            transaction.Rollback();
        }

        using var queryCommand = new OracleCommandMock(_connection)
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

    protected override void Dispose(bool disposing)
    {
        _connection.Dispose();
        base.Dispose(disposing);
    }
}