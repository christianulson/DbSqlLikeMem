namespace DbSqlLikeMem.Sqlite.Dapper.Test;
/// <summary>
/// EN: Covers SQLite fluent mapping scenarios against the Dapper provider.
/// PT: Cobre cenarios de mapeamento fluent SQLite contra o provedor Dapper.
/// </summary>
public sealed class FluentTest(
        ITestOutputHelper helper
    ) : DapperFluentTestsBase<SqliteDbMock, SqliteConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override SqliteConnectionMock CreateConnection(SqliteDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies the fluent insert, update, and delete workflow works end to end.
    /// PT: Verifica se o fluxo fluente de insercao, atualizacao e exclusao funciona de ponta a ponta.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void InsertUpdateDeleteFluentScenario_Test()
        => InsertUpdateDeleteFluentScenario();

    /// <summary>
    /// EN: Verifies the fluent API composes and executes the expected operations.
    /// PT: Verifica se a API fluente compoe e executa as operacoes esperadas.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void TestFluent_Test()
        => TestFluent();
}
