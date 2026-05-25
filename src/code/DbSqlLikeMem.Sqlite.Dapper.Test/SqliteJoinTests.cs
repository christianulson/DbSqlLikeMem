namespace DbSqlLikeMem.Sqlite.Dapper.Test;

/// <summary>
/// EN: Covers SQLite join scenarios against the Dapper provider.
/// PT-br: Cobre cenarios de join SQLite contra o provedor Dapper.
/// </summary>
public sealed class SqliteJoinTests(
    ITestOutputHelper helper
) : DapperJoinTestsBase<SqliteDbMock, SqliteConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies left joins keep all rows from the left table.
    /// PT-br: Verifica se left joins mantem todas as linhas da tabela da esquerda.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteJoin")]
    public void LeftJoin_ShouldKeepAllLeftRows_Test()
        => LeftJoin_ShouldKeepAllLeftRows();

    /// <summary>
    /// EN: Verifies right joins keep all rows from the right table.
    /// PT-br: Verifica se right joins mantem todas as linhas da tabela da direita.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteJoin")]
    public void RightJoin_ShouldKeepAllRightRows_Test()
        => RightJoin_ShouldKeepAllRightRows();

    /// <summary>
    /// EN: Verifies join conditions with multiple AND predicates are evaluated correctly.
    /// PT-br: Verifica se condicoes de join com multiplos predicados AND sao avaliadas corretamente.
    /// </summary>
    [Fact]
    [Trait("Category", "SqliteJoin")]
    public void Join_ON_WithMultipleConditions_AND_ShouldWork_Test()
        => Join_ON_WithMultipleConditions_AND_ShouldWork();
}
