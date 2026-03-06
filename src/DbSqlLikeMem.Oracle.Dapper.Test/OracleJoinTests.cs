namespace DbSqlLikeMem.Oracle.Test;

/// <summary>
/// EN: Defines the class OracleJoinTests.
/// PT: Define a classe OracleJoinTests.
/// </summary>
public sealed class OracleJoinTests(
    ITestOutputHelper helper
) : DapperJoinTestsBase<OracleDbMock, OracleConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies left joins keep all rows from the left table.
    /// PT: Verifica se left joins mantem todas as linhas da tabela da esquerda.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleJoin")]
    public void LeftJoin_ShouldKeepAllLeftRows_Test()
        => LeftJoin_ShouldKeepAllLeftRows();

    /// <summary>
    /// EN: Verifies right joins keep all rows from the right table.
    /// PT: Verifica se right joins mantem todas as linhas da tabela da direita.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleJoin")]
    public void RightJoin_ShouldKeepAllRightRows_Test()
        => RightJoin_ShouldKeepAllRightRows();

    /// <summary>
    /// EN: Verifies join predicates with multiple AND conditions work correctly.
    /// PT: Verifica se predicados de join com multiplas condicoes AND funcionam corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "OracleJoin")]
    public void Join_ON_WithMultipleConditions_AND_ShouldWork_Test()
        => Join_ON_WithMultipleConditions_AND_ShouldWork();
}
