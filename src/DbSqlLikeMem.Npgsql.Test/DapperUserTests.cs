namespace DbSqlLikeMem.Npgsql.Test;
public sealed class DapperUserTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    private sealed class User
    {
        public int Id { get; set; }
        public required string Name { get; set; }
        public string? Email { get; set; }
        public DateTime CreatedDate { get; set; }
        public DateTime? UpdatedData { get; set; }
        public Guid TestGuid { get; set; }
        public Guid? TestGuidNull { get; set; }
    }

    [Fact]
    public void InsertUserShouldAddUserToTable()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);
        table.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table.Columns["TestGuidNull"] = new(6, DbType.Guid, true);

        using var connection = new NpgsqlConnectionMock(db);
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

    [Fact]
    public void QueryUserShouldReturnCorrectData()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);
        table.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table.Columns["TestGuidNull"] = new(6, DbType.Guid, true);

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

        using var connection = new NpgsqlConnectionMock(db);

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

    [Fact]
    public void UpdateUserShouldModifyUserInTable()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);
        table.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table.Columns["TestGuidNull"] = new(6, DbType.Guid, true);

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

        using var connection = new NpgsqlConnectionMock(db);
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

    [Fact]
    public void DeleteUserShouldRemoveUserFromTable()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table = db.AddTable("Users");
        table.Columns["Id"] = new(0, DbType.Int32, false);
        table.Columns["Name"] = new(1, DbType.String, false);
        table.Columns["Email"] = new(2, DbType.String, false);
        table.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table.Columns["TestGuidNull"] = new(6, DbType.Guid, true);

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

        using var connection = new NpgsqlConnectionMock(db);
        connection.Open();

        // Act
        var rowsAffected = connection.Execute("DELETE FROM Users WHERE Id = @Id", new { user.Id });

        // Assert
        Assert.Equal(1, rowsAffected);
        Assert.Empty(table);
    }

    [Fact]
    public void QueryMultipleShouldReturnMultipleUserResultSets()
    {
        // Arrange
        var db = new NpgsqlDbMock();
        var table1 = db.AddTable("Users1");
        table1.Columns["Id"] = new(0, DbType.Int32, false);
        table1.Columns["Name"] = new(1, DbType.String, false);
        table1.Columns["Email"] = new(2, DbType.String, false);
        table1.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table1.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table1.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table1.Columns["TestGuidNull"] = new(6, DbType.Guid, true);
        table1.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, "john.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        var table2 = db.AddTable("Users2");
        table2.Columns["Id"] = new(0, DbType.Int32, false);
        table2.Columns["Name"] = new(1, DbType.String, false);
        table2.Columns["Email"] = new(2, DbType.String, false);
        table2.Columns["CreatedDate"] = new(3, DbType.DateTime, false);
        table2.Columns["UpdatedData"] = new(4, DbType.DateTime, true);
        table2.Columns["TestGuid"] = new(5, DbType.Guid, false);
        table2.Columns["TestGuidNull"] = new(6, DbType.Guid, true);
        table2.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Jane Doe" }, { 2, "jane.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        using var connection = new NpgsqlConnectionMock(db);

        using var command = new NpgsqlCommandMock(connection)
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