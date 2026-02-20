namespace DbSqlLikeMem.Sqlite.Dapper.Test;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class DapperUserTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public int Id { get; set; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public required string Name { get; set; }
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public string? Email { get; set; }
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

    /// <summary>
    /// EN: Tests InsertUserShouldAddUserToTable behavior.
    /// PT: Testa o comportamento de InsertUserShouldAddUserToTable.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void InsertUserShouldAddUserToTable()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        var user = new User
        {
            Id = 1,
            Name = "John Doe",
            Email = "john.doe@example.com",
            CreatedDate = DateTime.Now,
            UpdatedData = null,
            TestGuid = Guid.NewGuid(),
            TestGuidNull = null
        };

        // Act
        var rowsAffected = connection.Execute("INSERT INTO Users (Id, Name, Email, CreatedDate, UpdatedData, TestGuid, TestGuidNull) VALUES (@Id, @Name, @Email, @CreatedDate, @UpdatedData, @TestGuid, @TestGuidNull)", user);

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        var insertedRow = table[0];
        Assert.Equal(user.Id, insertedRow[0]);
        Assert.Equal(user.Name, insertedRow[1]);
        Assert.Equal(user.Email, insertedRow[2]);
        Assert.Equal(user.CreatedDate, insertedRow[3]);
        Assert.Equal(user.UpdatedData, insertedRow[4]);
        Assert.Equal(user.TestGuid, insertedRow[5]);
        Assert.Equal(user.TestGuidNull, insertedRow[6]);
    }

    /// <summary>
    /// EN: Tests QueryUserShouldReturnCorrectData behavior.
    /// PT: Testa o comportamento de QueryUserShouldReturnCorrectData.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void QueryUserShouldReturnCorrectData()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);

        var user = new User
        {
            Id = 1,
            Name = "John Doe",
            Email = "john.doe@example.com",
            CreatedDate = DateTime.Now,
            UpdatedData = null,
            TestGuid = Guid.NewGuid(),
            TestGuidNull = null
        };

        table.Add(new Dictionary<int, object?>
        {
            { 0, user.Id },
            { 1, user.Name },
            { 2, user.Email },
            { 3, user.CreatedDate },
            { 4, user.UpdatedData },
            { 5, user.TestGuid },
            { 6, user.TestGuidNull }
        });

        using var connection = new SqliteConnectionMock(db);

        // Act
        var result = connection.Query<User>("SELECT * FROM Users WHERE Id = @Id", new { user.Id }).FirstOrDefault();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(user.Id, result.Id);
        Assert.Equal(user.Name, result.Name);
        Assert.Equal(user.Email, result.Email);
        Assert.Equal(user.CreatedDate, result.CreatedDate);
        Assert.Equal(user.UpdatedData, result.UpdatedData);
        Assert.Equal(user.TestGuid, result.TestGuid);
        Assert.Equal(user.TestGuidNull, result.TestGuidNull);
    }

    /// <summary>
    /// EN: Tests UpdateUserShouldModifyUserInTable behavior.
    /// PT: Testa o comportamento de UpdateUserShouldModifyUserInTable.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void UpdateUserShouldModifyUserInTable()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);

        var user = new User
        {
            Id = 1,
            Name = "John Doe",
            Email = "john.doe@example.com",
            CreatedDate = DateTime.Now,
            UpdatedData = null,
            TestGuid = Guid.NewGuid(),
            TestGuidNull = null
        };

        table.Add(new Dictionary<int, object?>
        {
            { 0, user.Id },
            { 1, user.Name },
            { 2, user.Email },
            { 3, user.CreatedDate },
            { 4, user.UpdatedData },
            { 5, user.TestGuid },
            { 6, user.TestGuidNull }
        });

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        var updatedUser = new User
        {
            Id = 1,
            Name = "Jane Doe",
            Email = "jane.doe@example.com",
            CreatedDate = user.CreatedDate,
            UpdatedData = DateTime.Now,
            TestGuid = user.TestGuid,
            TestGuidNull = Guid.NewGuid()
        };

        // Act
        var rowsAffected = connection.Execute("UPDATE Users SET Name = @Name, Email = @Email, UpdatedData = @UpdatedData, TestGuidNull = @TestGuidNull WHERE Id = @Id", updatedUser);

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Single(table);
        var updatedRow = table[0];
        Assert.Equal(updatedUser.Id, updatedRow[0]);
        Assert.Equal(updatedUser.Name, updatedRow[1]);
        Assert.Equal(updatedUser.Email, updatedRow[2]);
        Assert.Equal(updatedUser.CreatedDate, updatedRow[3]);
        Assert.Equal(updatedUser.UpdatedData, updatedRow[4]);
        Assert.Equal(updatedUser.TestGuid, updatedRow[5]);
        Assert.Equal(updatedUser.TestGuidNull, updatedRow[6]);
    }

    /// <summary>
    /// EN: Tests DeleteUserShouldRemoveUserFromTable behavior.
    /// PT: Testa o comportamento de DeleteUserShouldRemoveUserFromTable.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void DeleteUserShouldRemoveUserFromTable()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table = db.AddTable("Users");
        table.AddColumn("Id", DbType.Int32, false);
        table.AddColumn("Name", DbType.String, false);
        table.AddColumn("Email", DbType.String, false);
        table.AddColumn("CreatedDate", DbType.DateTime, false);
        table.AddColumn("UpdatedData", DbType.DateTime, true);
        table.AddColumn("TestGuid", DbType.Guid, false);
        table.AddColumn("TestGuidNull", DbType.Guid, true);

        var user = new User
        {
            Id = 1,
            Name = "John Doe",
            Email = "john.doe@example.com",
            CreatedDate = DateTime.Now,
            UpdatedData = null,
            TestGuid = Guid.NewGuid(),
            TestGuidNull = null
        };

        table.Add(new Dictionary<int, object?>
        {
            { 0, user.Id },
            { 1, user.Name },
            { 2, user.Email },
            { 3, user.CreatedDate },
            { 4, user.UpdatedData },
            { 5, user.TestGuid },
            { 6, user.TestGuidNull }
        });

        using var connection = new SqliteConnectionMock(db);
        connection.Open();

        // Act
        var rowsAffected = connection.Execute("DELETE FROM Users WHERE Id = @Id", new { user.Id });

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Empty(table);
    }

    /// <summary>
    /// EN: Tests QueryMultipleShouldReturnMultipleUserResultSets behavior.
    /// PT: Testa o comportamento de QueryMultipleShouldReturnMultipleUserResultSets.
    /// </summary>
    [Fact]
    [Trait("Category", "DapperUser")]
    public void QueryMultipleShouldReturnMultipleUserResultSets()
    {
        // Arrange
        var db = new SqliteDbMock();
        var table1 = db.AddTable("Users1");
        table1.AddColumn("Id", DbType.Int32, false);
        table1.AddColumn("Name", DbType.String, false);
        table1.AddColumn("Email", DbType.String, false);
        table1.AddColumn("CreatedDate", DbType.DateTime, false);
        table1.AddColumn("UpdatedData", DbType.DateTime, true);
        table1.AddColumn("TestGuid", DbType.Guid, false);
        table1.AddColumn("TestGuidNull", DbType.Guid, true);
        table1.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, "john.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        var table2 = db.AddTable("Users2");
        table2.AddColumn("Id", DbType.Int32, false);
        table2.AddColumn("Name", DbType.String, false);
        table2.AddColumn("Email", DbType.String, false);
        table2.AddColumn("CreatedDate", DbType.DateTime, false);
        table2.AddColumn("UpdatedData", DbType.DateTime, true);
        table2.AddColumn("TestGuid", DbType.Guid, false);
        table2.AddColumn("TestGuidNull", DbType.Guid, true);
        table2.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Jane Doe" }, { 2, "jane.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        using var connection = new SqliteConnectionMock(db);

        using var command = new SqliteCommandMock(connection)
        {
            CommandText = "SELECT * FROM Users1; SELECT * FROM Users2;"
        };

        // Act
        var resultSets = new List<IEnumerable<User>>();
        using (var reader = command.ExecuteReader())
        {
            do
            {
                var resultSet = reader.Parse<User>().ToList();
                resultSets.Add(resultSet);
            } while (reader.NextResult());
        }

        // Assert
        Assert.Equal(2, resultSets.Count);

        var users1 = resultSets[0].ToList();
        Assert.Single(users1);
        Assert.Equal(1, users1[0].Id);
        Assert.Equal("John Doe", users1[0].Name);
        Assert.Equal("john.doe@example.com", users1[0].Email);

        var users2 = resultSets[1].ToList();
        Assert.Single(users2);
        Assert.Equal(2, users2[0].Id);
        Assert.Equal("Jane Doe", users2[0].Name);
        Assert.Equal("jane.doe@example.com", users2[0].Email);
    }
}
