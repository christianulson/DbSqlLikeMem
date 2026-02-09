namespace DbSqlLikeMem.Oracle.Test;
public sealed class OracleDataParameterCollectionMockTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    [Fact]
    public void ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames()
    {
        Assert.Equal("id", OracleDataParameterCollectionMock.NormalizeParameterName("@id"));
        Assert.Equal("id", OracleDataParameterCollectionMock.NormalizeParameterName("?id"));
        Assert.Equal("id", OracleDataParameterCollectionMock.NormalizeParameterName("@`id`"));
        Assert.Equal("id", OracleDataParameterCollectionMock.NormalizeParameterName("@\"id\""));
        Assert.Equal("id", OracleDataParameterCollectionMock.NormalizeParameterName("@'id'"));
    }

    [Fact]
    public void ParameterCollection_Add_DuplicateName_ShouldThrow()
    {
        var pars = new OracleDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        Assert.Throws<ArgumentException>(() => pars.AddWithValue("@id", 2)); // case-insensitive
    }

    [Fact]
    public void ParameterCollection_RemoveAt_ShouldReindexDictionary()
    {
        var pars = new OracleDataParameterCollectionMock();
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
