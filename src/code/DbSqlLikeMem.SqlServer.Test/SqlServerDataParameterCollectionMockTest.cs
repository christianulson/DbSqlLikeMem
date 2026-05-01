namespace DbSqlLikeMem.SqlServer.Test;
/// <summary>
/// EN: Verifies SQL Server parameter collections normalize names, preserve ordering, and enforce guard clauses.
/// PT-br: Verifica se colecoes de parametros do SQL Server normalizam nomes, preservam a ordem e aplicam validacoes.
/// </summary>
public sealed class SqlServerDataParameterCollectionMockTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parameter names normalize correctly for positional, quoted, and prefixed SQL Server forms.
    /// PT-br: Verifica se nomes de parametros sao normalizados corretamente para formas posicionais, entre aspas e com prefixo do SQL Server.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerDataParameterCollectionMockTest")]
    public void ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames()
    {
        Assert.Equal("id", SqlServerDataParameterCollectionMock.NormalizeParameterName("@id"));
        Assert.Equal("id", SqlServerDataParameterCollectionMock.NormalizeParameterName("?id"));
        Assert.Equal("id", SqlServerDataParameterCollectionMock.NormalizeParameterName("@`id`"));
        Assert.Equal("id", SqlServerDataParameterCollectionMock.NormalizeParameterName("@\"id\""));
        Assert.Equal("id", SqlServerDataParameterCollectionMock.NormalizeParameterName("@'id'"));
    }

    /// <summary>
    /// EN: Verifies duplicate parameter names are rejected case-insensitively.
    /// PT-br: Verifica se nomes duplicados de parametros sao rejeitados sem considerar maiusculas e minusculas.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerDataParameterCollectionMockTest")]
    public void ParameterCollection_Add_DuplicateName_ShouldThrow()
    {
        var pars = new SqlServerDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        Assert.Throws<ArgumentException>(() => pars.AddWithValue("@id", 2)); // case-insensitive
    }

    /// <summary>
    /// EN: Verifies removing a parameter by name keeps the collection index map in sync.
    /// PT-br: Verifica se remover um parametro pelo nome mantem o mapa de indices da colecao sincronizado.
    /// </summary>
    [Fact]
    [Trait("Category", "SqlServerDataParameterCollectionMockTest")]
    public void ParameterCollection_RemoveAt_ShouldReindexDictionary()
    {
        var pars = new SqlServerDataParameterCollectionMock();
        pars.AddWithValue("@a", 1);
        pars.AddWithValue("@b", 2);
        pars.AddWithValue("@c", 3);

        pars.RemoveAt("@b");

        Assert.True(pars.Contains("@a"));
        Assert.False(pars.Contains("@b"));
        Assert.True(pars.Contains("@c"));

        // c deve agora estar no índice 1
        Assert.Equal(3, pars["@c"].Value);
    }

}
