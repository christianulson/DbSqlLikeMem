namespace DbSqlLikeMem.Db2.Dapper.Test;

/// <summary>
/// EN: Defines the class Db2JoinTests.
/// PT: Define a classe Db2JoinTests.
/// </summary>
public sealed class Db2JoinTests(
    ITestOutputHelper helper
) : DapperJoinTestsBase<Db2DbMock, Db2ConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override Db2ConnectionMock CreateConnection(Db2DbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies left joins keep all rows from the left table.
    /// PT: Verifica se left joins mantem todas as linhas da tabela da esquerda.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Join")]
    public void LeftJoin_ShouldKeepAllLeftRows_Test()
        => LeftJoin_ShouldKeepAllLeftRows();

    /// <summary>
    /// EN: Verifies right joins keep all rows from the right table.
    /// PT: Verifica se right joins mantem todas as linhas da tabela da direita.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Join")]
    public void RightJoin_ShouldKeepAllRightRows_Test()
        => RightJoin_ShouldKeepAllRightRows();

    /// <summary>
    /// EN: Verifies join conditions with multiple AND predicates are evaluated correctly.
    /// PT: Verifica se condicoes de join com multiplos predicados AND sao avaliadas corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "Db2Join")]
    public void Join_ON_WithMultipleConditions_AND_ShouldWork_Test()
        => Join_ON_WithMultipleConditions_AND_ShouldWork();
}
