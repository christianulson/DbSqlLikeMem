namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Verifies Firebird command parameter collections accept provider-specific parameters and keep their indexes consistent.
/// PT: Verifica se colecoes de parametros de comando Firebird aceitam parametros especificos do provedor e mantem seus indices consistentes.
/// </summary>
public sealed class FirebirdDataParameterCollectionMockTest(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies a Firebird command parameter collection stores and resolves added parameters.
    /// PT: Verifica se a colecao de parametros de comando Firebird armazena e resolve parametros adicionados.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDataParameterCollectionMockTest")]
    public void ParameterCollection_Add_ShouldStoreFirebirdParameters()
    {
        using var command = new FirebirdCommandMock();

        var parameter = new FbParameter
        {
            ParameterName = "@id",
            Value = 1
        };

        var index = command.Parameters.Add(parameter);

        Assert.Equal(0, index);
        Assert.Single(command.Parameters);
        Assert.True(command.Parameters.Contains("@id"));
        Assert.Same(parameter, command.Parameters["@id"]);
        Assert.Equal(1, ((FbParameter)command.Parameters[0]!).Value);
    }

    /// <summary>
    /// EN: Verifies removing a parameter by index keeps the command collection empty again.
    /// PT: Verifica se remover um parametro por indice faz a colecao de comando voltar a ficar vazia.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDataParameterCollectionMockTest")]
    public void ParameterCollection_RemoveAt_ShouldClearCollection()
    {
        using var command = new FirebirdCommandMock();

        command.Parameters.Add(new FbParameter
        {
            ParameterName = "@a",
            Value = 1
        });
        command.Parameters.Add(new FbParameter
        {
            ParameterName = "@b",
            Value = 2
        });

        command.Parameters.RemoveAt(0);

        Assert.Single(command.Parameters);
        Assert.False(command.Parameters.Contains("@a"));
        Assert.True(command.Parameters.Contains("@b"));
        Assert.Equal(2, ((FbParameter)command.Parameters[0]!).Value);
    }

    /// <summary>
    /// EN: Verifies duplicate Firebird parameter names are rejected by the collection.
    /// PT: Verifica se nomes duplicados de parametros Firebird sao rejeitados pela colecao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDataParameterCollectionMockTest")]
    public void ParameterCollection_Add_DuplicateName_ShouldThrow()
    {
        using var command = new FirebirdCommandMock();

        command.Parameters.Add(new FbParameter
        {
            ParameterName = "@Id",
            Value = 1
        });

        Assert.Throws<ArgumentException>(() => command.Parameters.Add(new FbParameter
        {
            ParameterName = "@id",
            Value = 2
        }));
    }
}
