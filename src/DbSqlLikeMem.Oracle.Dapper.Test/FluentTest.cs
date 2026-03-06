namespace DbSqlLikeMem.Oracle.Test;
/// <summary>
/// EN: Defines the class FluentTest.
/// PT: Define a classe FluentTest.
/// </summary>
public sealed class FluentTest(
        ITestOutputHelper helper
    ) : DapperFluentTestsBase<OracleDbMock, OracleConnectionMock>(helper)
{
    /// <inheritdoc />
    protected override OracleConnectionMock CreateConnection(OracleDbMock db)
        => new(db);

    /// <summary>
    /// EN: Verifies the fluent scenario supports insert, update, and delete operations.
    /// PT: Verifica se o cenario fluente suporta operacoes de insert, update e delete.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void InsertUpdateDeleteFluentScenario_Test()
        => InsertUpdateDeleteFluentScenario();

    /// <summary>
    /// EN: Verifies the fluent table-definition API configures schema and seed data correctly.
    /// PT: Verifica se a API fluente de definicao de tabelas configura corretamente o schema e os dados iniciais.
    /// </summary>
    [Fact]
    [Trait("Category", "FluentTest")]
    public void TestFluent_Test()
        => TestFluent();
}
