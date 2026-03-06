namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperUserTests.
/// PT: Define a classe DapperUserTests.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new MySqlDbMock(),
    connectionFactory: static db => new MySqlConnectionMock((MySqlDbMock)db),
    commandFactory: static connection => new MySqlCommandMock((MySqlConnectionMock)connection),
    queryMultipleUsersSql: "SELECT * FROM `Users1`; SELECT * FROM `Users2`;",
    queryWithJoinSql: """
                SELECT U.*, UT.TenantId 
                FROM `User` U
                JOIN `UserTenant` UT ON U.Id = UT.UserId
                WHERE U.Id = @Id
                """);
