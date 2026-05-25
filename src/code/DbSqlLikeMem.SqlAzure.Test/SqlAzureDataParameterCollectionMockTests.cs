namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Contains tests for SQL Azure parameter collection behavior.
/// PT-br: Contém testes para o comportamento da coleção de parâmetros do SQL Azure.
/// </summary>
public sealed class SqlAzureDataParameterCollectionMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures adding duplicate parameter names is rejected case-insensitively.
    /// PT-br: Garante que adicionar nomes de parâmetros duplicados seja rejeitado sem diferenciar maiúsculas/minúsculas.
    /// </summary>
    [Fact]
    public void Add_DuplicateName_ShouldThrow()
    {
        var pars = new SqlAzureDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        FluentActions.Invoking(() => pars.AddWithValue("@id", 2)).Should().Throw<ArgumentException>();
    }

    /// <summary>
    /// EN: Ensures normalized lookups support quoted/backticked parameter names.
    /// PT-br: Garante que buscas normalizadas suportem nomes de parâmetro entre aspas/backticks.
    /// </summary>
    [Fact]
    public void Contains_ShouldMatchNormalizedQuotedNames()
    {
        var pars = new SqlAzureDataParameterCollectionMock();
        pars.AddWithValue("@`id`", 7);

        pars.Contains("@id").Should().BeTrue();
        pars.Contains("?id").Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures RemoveAt by name keeps dictionary index mapping consistent.
    /// PT-br: Garante que RemoveAt por nome mantenha consistente o mapeamento de índices no dicionário.
    /// </summary>
    [Fact]
    public void RemoveAt_ShouldReindexDictionary()
    {
        var pars = new SqlAzureDataParameterCollectionMock();
        pars.AddWithValue("@a", 1);
        pars.AddWithValue("@b", 2);
        pars.AddWithValue("@c", 3);

        pars.RemoveAt("@b");

        pars.Contains("@a").Should().BeTrue();
        pars.Contains("@b").Should().BeFalse();
        pars.Contains("@c").Should().BeTrue();
        pars["@c"].Value.Should().Be(3);
    }
}
