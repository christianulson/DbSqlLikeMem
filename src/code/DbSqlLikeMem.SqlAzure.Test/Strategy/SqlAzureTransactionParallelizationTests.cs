using DbSqlLikeMem.SqlServer;

namespace DbSqlLikeMem.SqlAzure.Test.Strategy;

/// <summary>
/// EN: Covers multi-table rollback behavior when SQL Azure runs in thread-safe mode.
/// PT: Cobre o comportamento de rollback com multiplas tabelas quando o SQL Azure executa em modo thread-safe.
/// </summary>
public sealed class SqlAzureTransactionParallelizationTests
{
    /// <summary>
    /// EN: Ensures rollback restores multiple tables correctly when the database runs in thread-safe mode.
    /// PT: Garante que rollback restaure varias tabelas corretamente quando o banco executa em modo thread-safe.
    /// </summary>
    [Fact]
    [Trait("Category", "Strategy")]
    public void TransactionRollback_ShouldRestoreMultipleTables_WhenThreadSafe()
    {
        var db = new SqlAzureDbMock
        {
            ThreadSafe = true
        };

        var users = AddUsersTable(db);
        users.AddPrimaryKeyIndexes("id");

        var orders = db.AddTable("orders");
        orders.AddColumn("id", DbType.Int32, false);
        orders.AddColumn("user_id", DbType.Int32, false);
        orders.AddPrimaryKeyIndexes("id");

        using var connection = CreateOpenConnection(db);
        using var transaction = Assert.IsType<SqlServerTransactionMock>(connection.BeginTransaction());
        using var command = new SqlAzureCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO users (id, name) VALUES (1, 'Ana')"
        };
        using var orderCommand = new SqlAzureCommandMock(connection, transaction)
        {
            CommandText = "INSERT INTO orders (id, user_id) VALUES (10, 1)"
        };

        command.ExecuteNonQuery();
        orderCommand.ExecuteNonQuery();

        transaction.Rollback();

        Assert.Empty(users);
        Assert.Empty(orders);
        Assert.False(connection.HasActiveTransaction);
        Assert.Equal(IsolationLevel.Unspecified, connection.CurrentIsolationLevel);
    }

    private static SqlAzureConnectionMock CreateOpenConnection(SqlAzureDbMock? db = null)
    {
        var connection = new SqlAzureConnectionMock(db ?? new SqlAzureDbMock());
        connection.Open();
        return connection;
    }

    private static ITableMock AddUsersTable(SqlAzureDbMock db)
    {
        var table = db.AddTable("users");
        table.AddColumn("id", DbType.Int32, false);
        table.AddColumn("name", DbType.String, false);
        return table;
    }
}

