namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers composite key equality and value ordering for index lookups.
/// PT: Cobre a igualdade de chaves compostas e a ordem dos valores usadas em buscas de indice.
/// </summary>
public sealed class IndexKeyTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies equivalent string and null-like values produce equal keys and equal hash codes.
    /// PT: Verifica que valores equivalentes de string e nulos geram chaves iguais e hashes iguais.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Equals_ShouldTreatEquivalentValuesAsEqual()
    {
        var left = new IndexKey("ABC", 42, DBNull.Value);
        var right = new IndexKey("abc", 42, null);

        Assert.Equal(left, right);
        Assert.Equal(left.GetHashCode(), right.GetHashCode());
    }

    /// <summary>
    /// EN: Verifies the exposed value list preserves the original key order.
    /// PT: Verifica que a lista de valores exposta preserva a ordem original da chave.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Values_ShouldExposeValuesInOrder()
    {
        var key = new IndexKey(["one", "two", "three", "four"]);

        Assert.Equal(new object?[] { "one", "two", "three", "four" }, key.Values);
        Assert.Equal("one | two | three | four", key.ToString());
    }
}
