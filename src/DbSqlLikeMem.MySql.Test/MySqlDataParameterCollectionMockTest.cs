namespace DbSqlLikeMem.MySql.Test;
/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class MySqlDataParameterCollectionMockTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Tests ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames behavior.
    /// PT: Testa o comportamento de ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames()
    {
        Assert.Equal("id", MySqlDataParameterCollectionMock.NormalizeParameterName("@id"));
        Assert.Equal("id", MySqlDataParameterCollectionMock.NormalizeParameterName("?id"));
        Assert.Equal("id", MySqlDataParameterCollectionMock.NormalizeParameterName("@`id`"));
        Assert.Equal("id", MySqlDataParameterCollectionMock.NormalizeParameterName("@\"id\""));
        Assert.Equal("id", MySqlDataParameterCollectionMock.NormalizeParameterName("@'id'"));
    }

    /// <summary>
    /// EN: Tests ParameterCollection_Add_DuplicateName_ShouldThrow behavior.
    /// PT: Testa o comportamento de ParameterCollection_Add_DuplicateName_ShouldThrow.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ParameterCollection_Add_DuplicateName_ShouldThrow()
    {
        var pars = new MySqlDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        Assert.Throws<ArgumentException>(() => pars.AddWithValue("@id", 2)); // case-insensitive
    }

    /// <summary>
    /// EN: Tests ParameterCollection_RemoveAt_ShouldReindexDictionary behavior.
    /// PT: Testa o comportamento de ParameterCollection_RemoveAt_ShouldReindexDictionary.
    /// </summary>
    [Fact]
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public void ParameterCollection_RemoveAt_ShouldReindexDictionary()
    {
        var pars = new MySqlDataParameterCollectionMock();
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
