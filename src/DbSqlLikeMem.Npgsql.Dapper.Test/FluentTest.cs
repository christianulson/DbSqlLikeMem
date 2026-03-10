namespace DbSqlLikeMem.Npgsql.Test;
/// <summary>
/// EN: Defines the class FluentTest.
/// PT: Define a classe FluentTest.
/// </summary>
public sealed class FluentTest(
        ITestOutputHelper helper
    ) : DapperFluentTestsBase<NpgsqlDbMock, NpgsqlConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override NpgsqlConnectionMock CreateConnection(NpgsqlDbMock db)
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
