namespace DbSqlLikeMem.SqlAzure.Test;

/// <summary>
/// EN: Contains tests for SQL Azure parameter collection behavior.
/// PT: Contém testes para o comportamento da coleção de parâmetros do SQL Azure.
/// </summary>
public sealed class SqlAzureDataParameterCollectionMockTests
{
    /// <summary>
    /// EN: Ensures adding duplicate parameter names is rejected case-insensitively.
    /// PT: Garante que adicionar nomes de parâmetros duplicados seja rejeitado sem diferenciar maiúsculas/minúsculas.
    /// </summary>
    [Fact]
    public void Add_DuplicateName_ShouldThrow()
    {
        var pars = new SqlAzureDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        Assert.Throws<ArgumentException>(() => pars.AddWithValue("@id", 2));
    }

    /// <summary>
    /// EN: Ensures normalized lookups support quoted/backticked parameter names.
    /// PT: Garante que buscas normalizadas suportem nomes de parâmetro entre aspas/backticks.
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
    /// PT: Garante que RemoveAt por nome mantenha consistente o mapeamento de índices no dicionário.
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
