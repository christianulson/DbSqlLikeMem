namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class DapperUserTests.
/// PT: Define a classe DapperUserTests.
/// </summary>
public sealed class DapperUserTests(
    ITestOutputHelper helper
) : DapperUserTestsBase(
    helper,
    dbFactory: static () => new Db2DbMock(),
    connectionFactory: static db => new Db2ConnectionMock((Db2DbMock)db),
    commandFactory: static connection => new Db2CommandMock((Db2ConnectionMock)connection),
    queryMultipleUsersSql: "SELECT * FROM \"Users1\"; SELECT * FROM \"Users2\";",
    queryWithJoinSql: """
                SELECT U.*, UT.TenantId 
                FROM "User" U
                JOIN "UserTenant" UT ON U.Id = UT.UserId
                WHERE U.Id = @Id
                """);
