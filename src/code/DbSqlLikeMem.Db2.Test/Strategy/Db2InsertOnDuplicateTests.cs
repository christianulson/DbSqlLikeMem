namespace DbSqlLikeMem.Db2.Test.Strategy;

/// <summary>
/// EN: Covers INSERT ... ON DUPLICATE scenarios in the Db2 mock.
/// PT-br: Cobre cenarios de INSERT ... ON DUPLICATE no mock Db2.
/// </summary>
public class Db2InsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: DB2 dialect must reject ON DUPLICATE KEY UPDATE syntax.
    /// PT-br: O dialeto DB2 deve rejeitar sintaxe ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [Trait("Category", "Strategy")]
    [MemberDataDb2Version]
    public void Insert_OnDuplicate_ShouldThrowNotSupported(int version)
    {
        var db = new Db2DbMock(version);

        var ex = FluentActions.Invoking(() =>
            SqlQueryParser.Parse(
                "INSERT INTO users (Id, Name) VALUES (1, 'A') ON DUPLICATE KEY UPDATE Name = VALUES(Name)",
                db,
                db.Dialect)).Should().Throw<NotSupportedException>().Which;

        ex.Message.Should().Contain("ON DUPLICATE KEY UPDATE");
    }
}
