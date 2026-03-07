namespace DbSqlLikeMem.MySql.Test;
/// <summary>
/// EN: Defines the class MySqlDataParameterCollectionMockTest.
/// PT: Define a classe MySqlDataParameterCollectionMockTest.
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
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
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
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
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
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
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

    /// <summary>
    /// EN: Verifies add overloads, contains, copy, and clear keep the collection and dictionary synchronized.
    /// PT: Verifica se as sobrecargas de add, contains, copy e clear mantem a colecao e o dicionario sincronizados.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_AddOverloads_ShouldKeepCollectionConsistent()
    {
        var pars = new MySqlDataParameterCollectionMock();

        var byDbType = pars.Add("@id", DbType.Int32);
        var byMySqlType = pars.Add("@name", MySqlDbType.VarChar);
        var bySize = pars.Add("@email", MySqlDbType.VarChar, 200);
        var byValue = pars.AddWithValue("@active", true);

        Assert.Equal(4, pars.Count);
        Assert.Same(byDbType, pars[0]);
        Assert.Same(byMySqlType, pars["@name"]);
        Assert.Same(bySize, pars["@email"]);
        Assert.Same(byValue, pars["@active"]);
        Assert.True(pars.Contains("@email"));
        Assert.Contains(byValue, pars);
        Assert.False(pars.Contains(new object()));
        Assert.Equal(2, pars.IndexOf("@email"));

        var copied = new MySqlParameter[4];
        pars.CopyTo(copied, 0);
        Assert.Same(byDbType, copied[0]);
        Assert.Same(byValue, copied[3]);

        var copiedAsArray = new object[4];
        pars.CopyTo(copiedAsArray, 0);
        Assert.Same(byMySqlType, copiedAsArray[1]);

        pars.Clear();

        Assert.Empty(pars);
        Assert.False(pars.Contains("@id"));
    }

    /// <summary>
    /// EN: Verifies replacing and removing parameters updates name lookups and reindexing correctly.
    /// PT: Verifica se substituir e remover parametros atualiza corretamente as buscas por nome e a reindexacao.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_SettersAndRemovals_ShouldUpdateIndexes()
    {
        var pars = new MySqlDataParameterCollectionMock();
        var first = new MySqlParameter("@id", 1);
        var second = new MySqlParameter("@name", "Ana");
        var third = new MySqlParameter("@email", "ana@example.com");

        pars.Add(first);
        pars.Add(second);
        ((DbParameterCollection)pars).Insert(1, third);

        Assert.Equal(1, pars.IndexOf("@email"));
        Assert.Same(third, pars[1]);

        var replacement = new MySqlParameter("@displayName", "Ana Maria");
        pars[1] = replacement;

        Assert.False(pars.Contains("@email"));
        Assert.True(pars.Contains("@displayName"));
        Assert.Same(replacement, pars["@displayName"]);

        pars["@displayName"] = new MySqlParameter("@nickname", "Aninha");

        Assert.False(pars.Contains("@displayName"));
        Assert.True(pars.Contains("@nickname"));
        Assert.Equal(1, pars.IndexOf("@nickname"));

        Assert.True(pars.Remove(pars["@nickname"]));
        Assert.False(pars.Remove(new MySqlParameter("@missing", 0)));

        pars.Remove(first);

        Assert.Single(pars);
        Assert.Equal(-1, pars.IndexOf("@id"));
        Assert.Equal(-1, pars.IndexOf(new object()));
    }

    /// <summary>
    /// EN: Verifies guard clauses throw for nulls, duplicates, and missing parameter names.
    /// PT: Verifica se as validacoes lancam excecao para nulos, duplicados e nomes de parametros ausentes.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_InvalidOperations_ShouldThrow()
    {
        var pars = new MySqlDataParameterCollectionMock();
        pars.AddWithValue("@id", 1);
        pars.AddWithValue("@name", "Ana");

        Assert.Throws<ArgumentNullException>(() => pars.Add((object)null!));
        Assert.Throws<ArgumentNullException>(() => pars.AddRange(null!));
        Assert.Throws<ArgumentNullException>(() => pars.Insert(0, (object?)null));
        Assert.Throws<ArgumentNullException>(() => pars.Remove((object?)null));
        Assert.Throws<ArgumentNullException>(() => pars.Remove((MySqlParameter)null!));
        Assert.Throws<ArgumentException>(() => _ = pars["@missing"]);
        Assert.Throws<ArgumentException>(() => pars[1] = new MySqlParameter("@id", 2));
    }
}
