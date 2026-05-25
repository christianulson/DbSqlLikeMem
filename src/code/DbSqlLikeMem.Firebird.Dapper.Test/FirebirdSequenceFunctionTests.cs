namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird sequence function aliases through the Dapper-facing mock surface.
/// PT-br: Cobre aliases de funcoes de sequence Firebird pela surface simulada voltada para Dapper.
/// </summary>
public sealed class FirebirdSequenceFunctionTests
{
    /// <summary>
    /// EN: Verifies GEN_ID and NEXT VALUE FOR stay consistent on the Dapper-facing Firebird surface.
    /// PT-br: Verifica se GEN_ID e NEXT VALUE FOR permanecem consistentes na surface Firebird voltada para Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void GenIdSequenceFunction_ShouldFollowFirebirdSemantics_Test()
    {
        var db = new FirebirdDbMock();
        db.AddSequence("seq_gen", startValue: 10, incrementBy: 10);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var genId = new FirebirdCommandMock(connection))
        {
            genId.CommandText = "SELECT GEN_ID(seq_gen, 1) FROM RDB$DATABASE";
            Assert.Equal(1, Convert.ToInt64(genId.ExecuteScalar(), CultureInfo.InvariantCulture));
        }

        using (var genId = new FirebirdCommandMock(connection))
        {
            genId.CommandText = "SELECT GEN_ID(seq_gen, 0) FROM RDB$DATABASE";
            Assert.Equal(1, Convert.ToInt64(genId.ExecuteScalar(), CultureInfo.InvariantCulture));
        }

        using (var next = new FirebirdCommandMock(connection))
        {
            next.CommandText = "SELECT NEXT VALUE FOR seq_gen FROM RDB$DATABASE";
            Assert.Equal(11, Convert.ToInt64(next.ExecuteScalar(), CultureInfo.InvariantCulture));
        }
    }
}
