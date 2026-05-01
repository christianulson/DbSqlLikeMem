namespace DbSqlLikeMem.MySql.Test;
/// <summary>
/// EN: Verifies MySQL parameter collections normalize names, preserve ordering, and enforce guard clauses.
/// PT-br: Verifica se colecoes de parametros do MySQL normalizam nomes, preservam a ordem e aplicam validacoes.
/// </summary>
public sealed class MySqlDataParameterCollectionMockTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies parameter names normalize correctly for positional, quoted, and prefixed MySQL forms.
    /// PT-br: Verifica se nomes de parametros sao normalizados corretamente para formas posicionais, entre aspas e com prefixo do MySQL.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_Normalize_ShouldWork_ForAtQuestionAndQuotedNames()
    {
        MySqlDataParameterCollectionMock.NormalizeParameterName("@id").Should().Be("id");
        MySqlDataParameterCollectionMock.NormalizeParameterName("?id").Should().Be("id");
        MySqlDataParameterCollectionMock.NormalizeParameterName("@`id`").Should().Be("id");
        MySqlDataParameterCollectionMock.NormalizeParameterName("@\"id\"").Should().Be("id");
        MySqlDataParameterCollectionMock.NormalizeParameterName("@'id'").Should().Be("id");
    }

    /// <summary>
    /// EN: Verifies duplicate parameter names are rejected case-insensitively.
    /// PT-br: Verifica se nomes duplicados de parametros sao rejeitados sem considerar maiusculas e minusculas.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_Add_DuplicateName_ShouldThrow()
    {
        var pars = new MySqlDataParameterCollectionMock();
        pars.AddWithValue("@Id", 1);

        FluentActions.Invoking(() => pars.AddWithValue("@id", 2)).Should().Throw<ArgumentException>(); // case-insensitive
    }

    /// <summary>
    /// EN: Verifies removing a parameter by name keeps the collection index map in sync.
    /// PT-br: Verifica se remover um parametro pelo nome mantem o mapa de indices da colecao sincronizado.
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

        pars.Contains("@a").Should().BeTrue();
        pars.Contains("@b").Should().BeFalse();
        pars.Contains("@c").Should().BeTrue();

        // c deve agora estar no índice 1
        pars["@c"].Value.Should().Be(3);
    }

    /// <summary>
    /// EN: Verifies add overloads, contains, copy, and clear keep the collection and dictionary synchronized.
    /// PT-br: Verifica se as sobrecargas de add, contains, copy e clear mantem a colecao e o dicionario sincronizados.
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

        pars.Count.Should().Be(4);
        pars[0].Should().BeSameAs(byDbType);
        pars["@name"].Should().BeSameAs(byMySqlType);
        pars["@email"].Should().BeSameAs(bySize);
        pars["@active"].Should().BeSameAs(byValue);
        pars.Contains("@email").Should().BeTrue();
        pars.Should().Contain(byValue);
        pars.Contains(new object()).Should().BeFalse();
        pars.IndexOf("@email").Should().Be(2);

        var copied = new MySqlParameter[4];
        pars.CopyTo(copied, 0);
        copied[0].Should().BeSameAs(byDbType);
        copied[3].Should().BeSameAs(byValue);

        var copiedAsArray = new object[4];
        pars.CopyTo(copiedAsArray, 0);
        copiedAsArray[1].Should().BeSameAs(byMySqlType);

        pars.Clear();

        pars.Should().BeEmpty();
        pars.Contains("@id").Should().BeFalse();
    }

    /// <summary>
    /// EN: Verifies replacing and removing parameters updates name lookups and reindexing correctly.
    /// PT-br: Verifica se substituir e remover parametros atualiza corretamente as buscas por nome e a reindexacao.
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

        pars.IndexOf("@email").Should().Be(1);
        pars[1].Should().BeSameAs(third);

        var replacement = new MySqlParameter("@displayName", "Ana Maria");
        pars[1] = replacement;

        pars.Contains("@email").Should().BeFalse();
        pars.Contains("@displayName").Should().BeTrue();
        pars["@displayName"].Should().BeSameAs(replacement);

        pars["@displayName"] = new MySqlParameter("@nickname", "Aninha");

        pars.Contains("@displayName").Should().BeFalse();
        pars.Contains("@nickname").Should().BeTrue();
        pars.IndexOf("@nickname").Should().Be(1);

        pars.Remove(pars["@nickname"]).Should().BeTrue();
        pars.Remove(new MySqlParameter("@missing", 0)).Should().BeFalse();

        pars.Remove(first);

        pars.Should().ContainSingle();
        pars.IndexOf("@id").Should().Be(-1);
        pars.IndexOf(new object()).Should().Be(-1);
    }

    /// <summary>
    /// EN: Verifies guard clauses throw for nulls, duplicates, and missing parameter names.
    /// PT-br: Verifica se as validacoes lancam excecao para nulos, duplicados e nomes de parametros ausentes.
    /// </summary>
    [Fact]
    [Trait("Category", "MySqlDataParameterCollectionMockTest")]
    public void ParameterCollection_InvalidOperations_ShouldThrow()
    {
        var pars = new MySqlDataParameterCollectionMock();
        pars.AddWithValue("@id", 1);
        pars.AddWithValue("@name", "Ana");

        FluentActions.Invoking(() => pars.Add((object)null!)).Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => pars.AddRange(null!)).Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => pars.Insert(0, (object?)null)).Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => pars.Remove((object?)null)).Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => pars.Remove((MySqlParameter)null!)).Should().Throw<ArgumentNullException>();
        FluentActions.Invoking(() => _ = pars["@missing"]).Should().Throw<ArgumentException>();
        FluentActions.Invoking(() => pars[1] = new MySqlParameter("@id", 2)).Should().Throw<ArgumentException>();
    }
}
