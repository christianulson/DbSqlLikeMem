namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird ALTER SEQUENCE execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de ALTER SEQUENCE no motor simulado Firebird.
/// </summary>
public sealed class FirebirdAlterSequenceTests : XUnitTestBase
{
    private readonly FirebirdDbMock db;
    private readonly FirebirdConnectionMock connection;
    private readonly ITableMock users;

    /// <summary>
    /// EN: Creates the Firebird database objects used by the alter sequence tests.
    /// PT: Cria os objetos de banco Firebird usados pelos testes de alter sequence.
    /// </summary>
    public FirebirdAlterSequenceTests(ITestOutputHelper helper) : base(helper)
    {
        db = new FirebirdDbMock();
        db.AddSequence("seq_users", startValue: 1, incrementBy: 1);
        users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        connection = new FirebirdConnectionMock(db);
        connection.Open();
    }

    /// <summary>
    /// EN: Verifies ALTER SEQUENCE RESTART WITH resets the next generated value.
    /// PT: Verifica se ALTER SEQUENCE RESTART WITH reinicia o proximo valor gerado.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AlterSequenceRestartWith_ShouldResetNextGeneratedValue()
    {
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
    /// EN: Verifies ALTER SEQUENCE RESTART WITH is restored by transaction rollback.
    /// PT: Verifica se ALTER SEQUENCE RESTART WITH e restaurado pelo rollback da transacao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void AlterSequenceRestartWith_ShouldRollbackWithTransaction()
    {
        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_users, 'Alice')";
            insert.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        {
            using (var alter = new FirebirdCommandMock(connection))
            {
                alter.CommandText = "ALTER SEQUENCE seq_users RESTART WITH 10";
                alter.ExecuteNonQuery();
            }

            using (var insert = new FirebirdCommandMock(connection))
            {
                insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_users, 'Bob')";
                insert.ExecuteNonQuery();
            }

            transaction.Rollback();
        }

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_users, 'Carol')";
            insert.ExecuteNonQuery();
        }

        Assert.Collection(users,
            _ => { },
            _ => { });
        Assert.Equal(1, users[0][0]);
        Assert.Equal(2, users[1][0]);
    }

    /// <summary>
    /// EN: Verifies generator aliases behave like sequence DDL in the Firebird mock.
    /// PT: Verifica se os aliases de generator se comportam como DDL de sequence no mock Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void GeneratorAliases_ShouldBehaveLikeSequenceDdl()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE GENERATOR seq_alias";
            Assert.Equal(0, create.ExecuteNonQuery());
        }

        using (var alter = new FirebirdCommandMock(connection))
        {
            alter.CommandText = "ALTER GENERATOR seq_alias RESTART WITH 7";
            Assert.Equal(0, alter.ExecuteNonQuery());
        }

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (NEXT VALUE FOR seq_alias, 'Alias')";
            Assert.Equal(1, insert.ExecuteNonQuery());
        }

        using (var drop = new FirebirdCommandMock(connection))
        {
            drop.CommandText = "DROP GENERATOR seq_alias";
            Assert.Equal(0, drop.ExecuteNonQuery());
        }

        Assert.False(db.TryGetSequence("seq_alias", out _));
    }

    /// <summary>
    /// EN: Verifies SET GENERATOR follows Firebird sequence restart semantics.
    /// PT: Verifica se SET GENERATOR segue a semantica de reinicio de sequence do Firebird.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void SetGenerator_ShouldBehaveLikeSequenceRestart()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE GENERATOR seq_set";
            Assert.Equal(0, create.ExecuteNonQuery());
        }

        using (var set = new FirebirdCommandMock(connection))
        {
            set.CommandText = "SET GENERATOR seq_set TO 4";
            Assert.Equal(0, set.ExecuteNonQuery());
        }

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (GEN_ID(seq_set, 1), 'Set')";
            Assert.Equal(1, insert.ExecuteNonQuery());
        }

        Assert.True(db.TryGetSequence("seq_set", out var sequence));
        Assert.NotNull(sequence);
        Assert.Equal(5, sequence!.CurrentValue);
    }

    /// <summary>
    /// EN: Disposes the Firebird connection used by the alter sequence tests.
    /// PT: Descarta a conexao Firebird usada pelos testes de alter sequence.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        connection.Dispose();
        base.Dispose(disposing);
    }
}
