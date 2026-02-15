namespace DbSqlLikeMem.Db2.Test;

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class DapperUserTests2(
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
        /// <summary>
        /// Auto-generated summary.
        /// </summary>
        public List<int> Tenants { get; set; } = [];
    }

    /// <summary>
    /// EN: Tests QueryUserShouldReturnCorrectData behavior.
    /// PT: Testa o comportamento de QueryUserShouldReturnCorrectData.
    /// </summary>
    [Fact]
    public void QueryUserShouldReturnCorrectData()
    {
        // Arrange
        var db = new Db2DbMock();
        var table = db.AddTable("users");
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

        table.Add(new()
        {
            { 0, user.Id },
            { 1, user.Name },
            { 2, user.Email },
            { 3, user.CreatedDate },
            { 4, user.UpdatedData },
            { 5, user.TestGuid },
            { 6, user.TestGuidNull }
        });

        using var connection = new Db2ConnectionMock(db);

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
    /// EN: Tests QueryMultipleShouldReturnMultipleUserResultSets behavior.
    /// PT: Testa o comportamento de QueryMultipleShouldReturnMultipleUserResultSets.
    /// </summary>
    [Fact]
    public void QueryMultipleShouldReturnMultipleUserResultSets()
    {
        // Arrange
        var db = new Db2DbMock();
        var table1 = db.AddTable("users1");
        table1.AddColumn("Id", DbType.Int32, false);
        table1.AddColumn("Name", DbType.String, false);
        table1.AddColumn("Email", DbType.String, false);
        table1.AddColumn("CreatedDate", DbType.DateTime, false);
        table1.AddColumn("UpdatedData", DbType.DateTime, true);
        table1.AddColumn("TestGuid", DbType.Guid, false);
        table1.AddColumn("TestGuidNull", DbType.Guid, true);
        table1.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, "john.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        var table2 = db.AddTable("users2");
        table2.AddColumn("Id", DbType.Int32, false);
        table2.AddColumn("Name", DbType.String, false);
        table2.AddColumn("Email", DbType.String, false);
        table2.AddColumn("CreatedDate", DbType.DateTime, false);
        table2.AddColumn("UpdatedData", DbType.DateTime, true);
        table2.AddColumn("TestGuid", DbType.Guid, false);
        table2.AddColumn("TestGuidNull", DbType.Guid, true);
        table2.Add(new Dictionary<int, object?> { { 0, 2 }, { 1, "Jane Doe" }, { 2, "jane.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        using var connection = new Db2ConnectionMock(db);

        using var command = new Db2CommandMock(connection)
        {
            CommandText = "SELECT * FROM \"Users1\"; SELECT * FROM \"Users2\";"
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

    /// <summary>
    /// EN: Tests QueryWithJoinShouldReturnJoinedData behavior.
    /// PT: Testa o comportamento de QueryWithJoinShouldReturnJoinedData.
    /// </summary>
    [Fact]
    public void QueryWithJoinShouldReturnJoinedData()
    {
        // Arrange
        var db = new Db2DbMock();
        var userTable = db.AddTable("user");
        userTable.AddColumn("Id", DbType.Int32, false);
        userTable.AddColumn("Name", DbType.String, false);
        userTable.AddColumn("Email", DbType.String, false);
        userTable.AddColumn("CreatedDate", DbType.DateTime, false);
        userTable.AddColumn("UpdatedData", DbType.DateTime, true);
        userTable.AddColumn("TestGuid", DbType.Guid, false);
        userTable.AddColumn("TestGuidNull", DbType.Guid, true);
        userTable.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, "John Doe" }, { 2, "john.doe@example.com" }, { 3, DateTime.Now }, { 4, null }, { 5, Guid.NewGuid() }, { 6, null } });

        var userTenantTable = db.AddTable("usertenant");
        userTenantTable.AddColumn("UserId", DbType.Int32, false);
        userTenantTable.AddColumn("TenantId", DbType.Int32, false);
        userTenantTable.Add(new Dictionary<int, object?> { { 0, 1 }, { 1, 1 } });

        using var connection = new Db2ConnectionMock(db);

        const string sql = @"
                SELECT U.*, UT.TenantId 
                FROM ""User"" U
                JOIN ""UserTenant"" UT ON U.Id = UT.UserId
                WHERE U.Id = @Id";

        using var command = new Db2CommandMock(connection)
        {
            CommandText = sql
        };

        using var r = command.ExecuteReader();
        for (int i = 0; i < r.FieldCount; i++)
            Console.WriteLine($"{i}: {r.GetName(i)}");

        // Act
        var result = connection.Query<User, int, User>(sql, (user, tenantId) =>
        {
            user.Tenants = [tenantId]; // Just an example to map the joined field
            return user;
        }, new { Id = 1 }, splitOn: "TenantId").FirstOrDefault();

        // Assert
        Assert.NotNull(result);
        Assert.Equal(1, result.Id);
        Assert.Equal("John Doe", result.Name);
        Assert.Equal("john.doe@example.com", result.Email);
        Assert.Equal(1, result.Tenants?.Count);
        Assert.Equal(1, result.Tenants?[0]);
    }
}
