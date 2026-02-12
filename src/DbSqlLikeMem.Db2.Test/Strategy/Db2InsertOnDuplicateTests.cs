namespace DbSqlLikeMem.Db2.Test.Strategy;

/// <summary>
/// EN: Tests for INSERT ... ON DUPLICATE behavior.
/// PT: Testes para comportamento de INSERT ... ON DUPLICATE.
/// </summary>
public class Db2InsertOnDuplicateTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: DB2 dialect must reject ON DUPLICATE KEY UPDATE syntax.
    /// PT: O dialeto DB2 deve rejeitar sintaxe ON DUPLICATE KEY UPDATE.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    public void Insert_OnDuplicate_ShouldThrowNotSupported(int version)
    {
        var db = new Db2DbMock(version);

        var ex = Assert.Throws<InvalidOperationException>(() =>
            SqlQueryParser.Parse(
                "INSERT INTO users (Id, Name) VALUES (1, 'A') ON DUPLICATE KEY UPDATE Name = VALUES(Name)",
                db.Dialect));

        Assert.Contains("ON DUPLICATE KEY UPDATE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
