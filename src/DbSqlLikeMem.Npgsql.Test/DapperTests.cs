namespace DbSqlLikeMem.Npgsql.Test;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class DapperTests : XUnitTestBase
{
    private readonly NpgsqlConnectionMock _connection;

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DapperTests(
        ITestOutputHelper helper
    ) : base(helper)
    {
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);
        table.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table.Columns["TestGuidNull"] = new(6, DbType.Guid, true);

        _connection = new NpgsqlConnectionMock(db);
        _connection.Open();
    }

    /// <summary>
    /// EN: Tests TestSelectQuery behavior.
    /// PT: Testa o comportamento de TestSelectQuery.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void TestSelectQuery()
    {
        var users = _connection.Query<UserObjectTest>("SELECT * FROM Users").ToList();
        Assert.NotNull(users);
    }

    /// <summary>
    /// EN: Tests QueryShouldReturnCorrectData behavior.
    /// PT: Testa o comportamento de QueryShouldReturnCorrectData.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void QueryShouldReturnCorrectData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);
        table.Columns["CreatedDate"] = new(2, DbType.DateTime, false);

        var dt = DateTime.UtcNow;
        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, dt } });

        using var connection = new NpgsqlConnectionMock(db);
        using var command = new NpgsqlCommandMock(connection)
        {
            CommandText = "SELECT * FROM users"
        };

        // Act
        IEnumerable<dynamic> result;
        using (var reader = command.ExecuteReader())
        {
            result = [.. reader.Parse<dynamic>()];
        }

        // Assert
        Assert.Single(result);
        Assert.Equal(1, result.First().id);
        Assert.Equal("John Doe", result.First().name);
        Assert.Equal(dt, result.First().CreatedDate);
    }

    /// <summary>
    /// EN: Tests ExecuteShouldInsertData behavior.
    /// PT: Testa o comportamento de ExecuteShouldInsertData.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ExecuteShouldInsertData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);
        table.Columns["CreatedDate"] = new(2, DbType.DateTime, false);

        var dt = DateTime.UtcNow;

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();

        // Act
        var rowsAffected = connection.Execute("INSERT INTO users (id, name, createdDate) VALUES (@id, @name, @dt)", new { id = 1, name = "John Doe", dt });

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal(1, table[0][0]);
        Assert.Equal("John Doe", table[0][1]);
        Assert.Equal(dt, table[0][2]);
    }

    /// <summary>
    /// EN: Tests ExecuteShouldUpdateData behavior.
    /// PT: Testa o comportamento de ExecuteShouldUpdateData.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ExecuteShouldUpdateData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);
        table.Columns["CreatedDate"] = new(2, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(3, DbType.DateTime, true);

        var dtInsert = DateTime.UtcNow.AddDays(-1);
        var dtUpdate = DateTime.UtcNow;

        table.AddItem(new { id = 1, name = "John Doe", CreatedDate = dtInsert });

        Assert.Single(table);
        Assert.Equal("John Doe", table[0][1]);
        Assert.Equal(dtInsert, table[0][2]);
        Assert.Null(table[0][3]);

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();

        // Act
        var rowsAffected = connection.Execute(@"
UPDATE users 
   SET name = @name
     , UpdatedData = @dtUpdate 
 WHERE id = @id", new { id = 1, name = "Jane Doe", dtUpdate });

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        Assert.Equal("Jane Doe", table[0][1]);
        Assert.Equal(dtInsert, table[0][2]);
        Assert.Equal(dtUpdate, table[0][3]);
    }

    /// <summary>
    /// EN: Tests ExecuteShouldDeleteData behavior.
    /// PT: Testa o comportamento de ExecuteShouldDeleteData.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ExecuteShouldDeleteData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["id"] = new(0, DbType.Int32, false);
        table.Columns["name"] = new(1, DbType.String, false);

        table.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();

        // Act
        var rowsAffected = connection.Execute("DELETE FROM users WHERE id = @id", new { id = 1 });

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests QueryMultipleShouldReturnMultipleResultSets behavior.
    /// PT: Testa o comportamento de QueryMultipleShouldReturnMultipleResultSets.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void QueryMultipleShouldReturnMultipleResultSets()
    {
        var dt = DateTime.UtcNow;
        var dt2 = DateTime.UtcNow.AddDays(-1);

        // Arrange
        var db = new NpgsqlDbMock();
        var table1 = db.AddTable("users");
        table1.Columns["id"] = new(0, DbType.Int32, false);
        table1.Columns["name"] = new(1, DbType.String, false);
        table1.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" } });


        var table2 = db.AddTable("emails");
        table2.Columns["id"] = new(0, DbType.Int32, false);
        table2.Columns["email"] = new(1, DbType.String, false);
        table2.Columns["CreatedDate"] = new(2, DbType.DateTime, false);
        table2.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "john.doe@example.com" }, { 2, dt } });
        table2.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "jane.doe@example.com" }, { 2, dt2 } });

        using var connection = new NpgsqlConnectionMock(db);

        using var command = new NpgsqlCommandMock(connection)
        {
            CommandText = "SELECT * FROM users; SELECT * FROM emails ORDER BY CreatedDate DESC;"
        };

        // Act
        var resultSets = new List<IEnumerable<dynamic>>();
        using (var reader = command.ExecuteReader())
        {
            do
            {
                var resultSet = reader.Parse<dynamic>().ToList();
                resultSets.Add(resultSet);
            } while (reader.NextResult());
        }

        // Assert
        Assert.Equal(2, resultSets.Count);

        var users = resultSets[0].ToList();
        Assert.Single(users);
        Assert.Equal(1, users[0].id);
        Assert.Equal("John Doe", users[0].name);

        var emails = resultSets[1].ToList();
        Assert.Equal(2, emails.Count);

        Assert.Equal(1, emails[0].id);
        Assert.Equal("john.doe@example.com", emails[0].email);
        Assert.Equal(dt, emails[0].CreatedDate);

        Assert.Equal(2, emails[1].id);
        Assert.Equal("jane.doe@example.com", emails[1].email);
        Assert.Equal(dt2, emails[1].CreatedDate);
    }

    /// <summary>
    /// EN: Disposes test resources.
    /// PT: Descarta os recursos do teste.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _connection.Dispose();
        }
        base.Dispose(disposing);
    }
}

/// <summary>
/// EN: Test DTO used by Dapper scenarios.
/// PT: DTO de teste usado nos cenários do Dapper.
/// </summary>
public class UserObjectTest
{
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public int Id { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public string Name { get; set; } = default!;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public string Email { get; set; } = default!;
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DateTime CreatedDate { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public DateTime? UpdatedData { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Guid TestGuid { get; set; }
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public Guid? TestGuidNull { get; set; }
}
