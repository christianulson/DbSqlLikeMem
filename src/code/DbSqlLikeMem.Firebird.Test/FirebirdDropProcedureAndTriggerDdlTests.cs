namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird DROP PROCEDURE and DROP TRIGGER execution scenarios in the mock engine.
/// PT-br: Cobre cenarios de execucao de DROP PROCEDURE e DROP TRIGGER no motor simulado Firebird.
/// </summary>
public sealed class FirebirdDropProcedureAndTriggerDdlTests : XUnitTestBase
{
    private readonly FirebirdDbMock db;
    private readonly FirebirdConnectionMock connection;
    private readonly ITableMock users;

    /// <summary>
    /// EN: Creates the Firebird database objects used by the drop tests.
    /// PT-br: Cria os objetos de banco Firebird usados pelos testes de drop.
    /// </summary>
    public FirebirdDropProcedureAndTriggerDdlTests(ITestOutputHelper helper) : base(helper)
    {
        db = new FirebirdDbMock();
        users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        connection = new FirebirdConnectionMock(db);
        connection.Open();
    }

    /// <summary>
    /// EN: Verifies DROP PROCEDURE removes the stored definition.
    /// PT-br: Verifica se DROP PROCEDURE remove a definicao armazenada.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void DropProcedure_ShouldRemoveDefinition()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE PROCEDURE sp_echo(IN tenantId INT) BEGIN END";
            create.ExecuteNonQuery();
        }

        Assert.True(db.TryGetProcedure("sp_echo", out var procedure));
        Assert.NotNull(procedure);

        using (var drop = new FirebirdCommandMock(connection))
        {
            drop.CommandText = "DROP PROCEDURE IF EXISTS sp_echo";
            drop.ExecuteNonQuery();
        }

        Assert.False(db.TryGetProcedure("sp_echo", out _));
    }

    /// <summary>
    /// EN: Verifies DROP TRIGGER removes the stored trigger and keeps table DML working.
    /// PT-br: Verifica se DROP TRIGGER remove o trigger armazenado e mantém o DML da tabela funcionando.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void DropTrigger_ShouldRemoveDefinitionAndAllowInsert()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE TRIGGER trg_users_ai AFTER INSERT ON Users BEGIN END";
            create.ExecuteNonQuery();
        }

        Assert.True(users.HasTriggers(TableTriggerEvent.AfterInsert));

        using (var drop = new FirebirdCommandMock(connection))
        {
            drop.CommandText = "DROP TRIGGER IF EXISTS trg_users_ai";
            drop.ExecuteNonQuery();
        }

        Assert.False(users.HasTriggers(TableTriggerEvent.AfterInsert));

        using (var insert = new FirebirdCommandMock(connection))
        {
            insert.CommandText = "INSERT INTO Users (Id, Name) VALUES (3, 'Carol')";
            var affected = insert.ExecuteNonQuery();
            Assert.Equal(1, affected);
        }

        Assert.Single(users);
    }

    /// <summary>
    /// EN: Verifies DROP PROCEDURE is restored by transaction rollback.
    /// PT-br: Verifica se DROP PROCEDURE e restaurado pelo rollback da transacao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void DropProcedure_ShouldRollbackWithTransaction()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE PROCEDURE sp_tx(IN tenantId INT) BEGIN END";
            create.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        {
            using (var drop = new FirebirdCommandMock(connection))
            {
                drop.CommandText = "DROP PROCEDURE IF EXISTS sp_tx";
                drop.ExecuteNonQuery();
            }

            Assert.False(db.TryGetProcedure("sp_tx", out _));
            transaction.Rollback();
        }

        Assert.True(db.TryGetProcedure("sp_tx", out var procedure));
        Assert.NotNull(procedure);
    }

    /// <summary>
    /// EN: Verifies DROP TRIGGER is restored by transaction rollback.
    /// PT-br: Verifica se DROP TRIGGER e restaurado pelo rollback da transacao.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void DropTrigger_ShouldRollbackWithTransaction()
    {
        using (var create = new FirebirdCommandMock(connection))
        {
            create.CommandText = "CREATE TRIGGER trg_tx AFTER INSERT ON Users BEGIN END";
            create.ExecuteNonQuery();
        }

        using (var transaction = connection.BeginTransaction())
        {
            using (var drop = new FirebirdCommandMock(connection))
            {
                drop.CommandText = "DROP TRIGGER IF EXISTS trg_tx";
                drop.ExecuteNonQuery();
            }

            Assert.False(users.HasTriggers(TableTriggerEvent.AfterInsert));
            transaction.Rollback();
        }

        Assert.True(users.HasTriggers(TableTriggerEvent.AfterInsert));
    }

    /// <summary>
    /// EN: Disposes the Firebird connection used by the drop tests.
    /// PT-br: Descarta a conexao Firebird usada pelos testes de drop.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT-br: True para descartar recursos gerenciados.</param>
    protected override void Dispose(bool disposing)
    {
        connection.Dispose();
        base.Dispose(disposing);
    }
}
