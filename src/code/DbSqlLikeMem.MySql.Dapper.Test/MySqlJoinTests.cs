namespace DbSqlLikeMem.MySql.Dapper.Test;

/// <summary>
/// EN: Covers MySQL join scenarios against the Dapper provider.
/// PT-br: Cobre cenarios de join MySQL contra o provedor Dapper.
/// </summary>
public sealed class MySqlJoinTests(
    ITestOutputHelper helper
) : DapperJoinTestsBase<MySqlDbMock, MySqlConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override MySqlConnectionMock CreateConnection(MySqlDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies left joins keep all rows from the left table.
    /// PT-br: Verifica se left joins mantem todas as linhas da tabela da esquerda.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlJoin")]
    public void LeftJoin_ShouldKeepAllLeftRows_Test()
        => LeftJoin_ShouldKeepAllLeftRows();

    /// <summary>
    /// EN: Verifies right joins keep all rows from the right table.
    /// PT-br: Verifica se right joins mantem todas as linhas da tabela da direita.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlJoin")]
    public void RightJoin_ShouldKeepAllRightRows_Test()
        => RightJoin_ShouldKeepAllRightRows();

    /// <summary>
    /// EN: Verifies join predicates with multiple AND conditions work correctly.
    /// PT-br: Verifica se predicados de join com multiplas condicoes AND funcionam corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlJoin")]
    public void Join_ON_WithMultipleConditions_AND_ShouldWork_Test()
        => Join_ON_WithMultipleConditions_AND_ShouldWork();
}
