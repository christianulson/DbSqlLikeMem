namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for Firebird data reader mock behavior.
/// PT: Contem testes para o comportamento do leitor de dados simulador Firebird.
/// </summary>
public sealed class FirebirdDataReaderMockTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Verifies the Firebird reader can read values, resolve ordinals, and copy field values.
    /// PT: Verifica se o leitor Firebird consegue ler valores, resolver ordinais e copiar valores dos campos.
    /// </summary>
    [Fact]
    public void ReadAndProjection_ShouldExposeFieldValues()
    {
        var table = new TableResultMock
        {
            Columns =
            [
                new TableResultColMock("u", "Id", "Id", 0, DbType.Int32, false),
                new TableResultColMock("u", "Name", "Name", 1, DbType.String, false)
            ]
        };
        table.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Ana" });

        using var reader = new FirebirdDataReaderMock([table]);

        reader.Read().Should().BeTrue();
        reader.GetName(0).Should().Be("ID");
        reader.GetName(1).Should().Be("NAME");
        reader.GetOrdinal("Id").Should().Be(0);
        reader.GetInt32(0).Should().Be(1);
        reader.GetString(1).Should().Be("Ana");

        var values = new object[2];
        reader.GetValues(values).Should().Be(2);
        values.Should().Equal(1, "Ana");
    }
}
