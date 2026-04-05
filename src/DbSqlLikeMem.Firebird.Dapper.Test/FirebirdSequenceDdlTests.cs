namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird ALTER SEQUENCE execution scenarios over the Dapper-facing mock surface.
/// PT: Cobre cenarios de execucao de ALTER SEQUENCE sobre a surface simulada voltada para Dapper.
/// </summary>
public sealed class FirebirdSequenceDdlTests
{
    /// <summary>
    /// EN: Verifies ALTER SEQUENCE RESTART WITH changes the next generated value through the command surface.
    /// PT: Verifica se ALTER SEQUENCE RESTART WITH altera o proximo valor gerado pela surface de comando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void AlterSequenceRestartWith_ShouldResetNextGeneratedValue_Test()
    {
        var db = new FirebirdDbMock();
        db.AddSequence("seq_users", startValue: 1, incrementBy: 1);
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_users, 'Alice')";
            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        using (var alter = new FirebirdCommandMock(connection))
        {
            alter.CommandText = "ALTER SEQUENCE seq_users RESTART WITH 10";
            var affected = alter.ExecuteNonQuery();
            Assert.Equal(0, affected);
        }

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_users, 'Bob')";
            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        Assert.Collection(users,
            _ => { },
            _ => { });
        Assert.Equal(1, users[0][0]);
        Assert.Equal(10, users[1][0]);
    }

    /// <summary>
    /// EN: Verifies SET GENERATOR changes the sequence value through the command surface.
    /// PT: Verifica se SET GENERATOR altera o valor da sequence pela surface de comando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void SetGenerator_ShouldResetNextGeneratedValue_Test()
    {
        var db = new FirebirdDbMock();
        db.AddSequence("seq_set", startValue: 1, incrementBy: 1);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        using (var set = new FirebirdCommandMock(connection))
        {
            set.CommandText = "SET GENERATOR seq_set TO 4";
            Assert.Equal(0, set.ExecuteNonQuery());
        }

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "SELECT GEN_ID(seq_set, 1) FROM RDB$DATABASE";
            Assert.Equal(5, Convert.ToInt64(insert.ExecuteScalar(), CultureInfo.InvariantCulture));
        }
    }
}
