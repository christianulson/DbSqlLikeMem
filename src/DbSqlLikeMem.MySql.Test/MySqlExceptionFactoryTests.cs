namespace DbSqlLikeMem.MySql.Test;

/// <summary>
/// EN: Verifies MySqlExceptionFactory creates provider-specific exceptions with the expected error codes.
/// PT: Verifica se MySqlExceptionFactory cria excecoes especificas do provedor com os codigos esperados.
/// </summary>
public sealed class MySqlExceptionFactoryTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies duplicate key, unknown column, and nullability exceptions preserve their MySQL-style codes.
    /// PT: Verifica se excecoes de chave duplicada, coluna desconhecida e nulabilidade preservam seus codigos no estilo MySQL.
    /// </summary>
    [Fact]
    public void CoreFactoryMethods_ShouldReturnExpectedCodes()
    {
        var duplicate = Assert.IsType<MySqlMockException>(MySqlExceptionFactory.DuplicateKey("users", "PK_users", 1));
        var unknownColumn = Assert.IsType<MySqlMockException>(MySqlExceptionFactory.UnknownColumn("missing_col"));
        var columnCannotBeNull = Assert.IsType<MySqlMockException>(MySqlExceptionFactory.ColumnCannotBeNull("name"));

        duplicate.ErrorCode.Should().Be(1062);
        duplicate.Message.Should().Contain("1");
        duplicate.Message.Should().Contain("PK_users");
        unknownColumn.ErrorCode.Should().Be(1054);
        unknownColumn.Message.Should().Contain("missing_col");
        columnCannotBeNull.ErrorCode.Should().Be(1048);
        columnCannotBeNull.Message.Should().Contain("name");
    }

    /// <summary>
    /// EN: Verifies foreign key related helpers return the expected MySQL-style error codes.
    /// PT: Verifica se os helpers relacionados a chave estrangeira retornam os codigos esperados no estilo MySQL.
    /// </summary>
    [Fact]
    public void ForeignKeyFactoryMethods_ShouldReturnExpectedCodes()
    {
        var foreignKeyFails = Assert.IsType<MySqlMockException>(MySqlExceptionFactory.ForeignKeyFails("user_id", "users"));
        var referencedRow = Assert.IsType<MySqlMockException>(MySqlExceptionFactory.ReferencedRow("users"));

        foreignKeyFails.ErrorCode.Should().Be(1452);
        foreignKeyFails.Message.Should().Contain("user_id");
        foreignKeyFails.Message.Should().Contain("users");
        referencedRow.ErrorCode.Should().Be(1451);
        referencedRow.Message.Should().Contain("users");
    }
}
