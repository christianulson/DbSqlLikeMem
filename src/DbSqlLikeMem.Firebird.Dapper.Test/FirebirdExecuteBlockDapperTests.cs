namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird EXECUTE BLOCK scenarios through the Dapper-facing provider surface.
/// PT: Cobre cenarios de EXECUTE BLOCK pela surface do provedor exposta ao Dapper.
/// </summary>
public sealed class FirebirdExecuteBlockDapperTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
    private sealed class FirebirdConnectionSpyMock(
        FirebirdDbMock? db = null,
        string? defaultDatabase = null
        ) : FirebirdConnectionMock(db, defaultDatabase)
    {
        public static string? LastSetConnectionString { get; set; }
        public static string? LastSetDataSource { get; set; }

#pragma warning disable CS8764, CS8765
        public override string ConnectionString
        {
            get => base.ConnectionString;
            set
            {
                LastSetConnectionString = value;
                base.ConnectionString = value;
                LastSetDataSource = base.DataSource;
            }
        }
#pragma warning restore CS8764, CS8765
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK inserts rows through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK insere linhas pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldInsertRows_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (1, 'Alice');
    INSERT INTO Users (Id, Name) VALUES (2, 'Bob');
END
""");

        Assert.Equal(2, affected);
        Assert.Collection(users,
            _ => { },
            _ => { });
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can create a stored procedure through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK pode criar uma stored procedure pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldCreateProcedure_Test()
    {
        var db = new FirebirdDbMock();

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    CREATE OR ALTER PROCEDURE sp_block(IN tenantId INT) BEGIN END;
END
""");

        Assert.Equal(0, affected);
        Assert.True(db.TryGetProcedure("sp_block", out var procedure));
        Assert.NotNull(procedure);
        Assert.Single(procedure!.RequiredIn);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK ignores SUSPEND statements through the Dapper surface while still executing supported SQL.
    /// PT: Verifica se EXECUTE BLOCK ignora instrucoes SUSPEND pela surface Dapper enquanto ainda executa SQL suportado.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldIgnoreSuspendStatements_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    SUSPEND;
    INSERT INTO Users (Id, Name) VALUES (1, 'Alice');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK stops after EXIT through the Dapper surface while keeping earlier SQL changes.
    /// PT: Verifica se EXECUTE BLOCK para apos EXIT pela surface Dapper mantendo as alteracoes SQL anteriores.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldStopAtExit_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (1, 'Alice');
    EXIT;
    INSERT INTO Users (Id, Name) VALUES (2, 'Bob');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes a simple EXECUTE STATEMENT payload through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK executa uma carga simples de EXECUTE STATEMENT pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteExecuteStatement_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Alice'')';
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK assigns the first row returned by EXECUTE STATEMENT through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK atribui a primeira linha retornada por EXECUTE STATEMENT pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteExecuteStatementInto_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK
RETURNS (outValue INT)
AS
BEGIN
    EXECUTE STATEMENT 'SELECT 42 FROM RDB$DATABASE' INTO :outValue;
    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Into');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(42) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes parameterized EXECUTE STATEMENT payloads with Firebird clauses through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK executa cargas parametrizadas de EXECUTE STATEMENT com clausulas Firebird pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteParameterizedExecuteStatement_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT ('INSERT INTO Users (Id, Name) VALUES (:userId, :userName)') (userId := 10, userName := 'Param') WITH CALLER PRIVILEGES;
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(10) == true);
    }

    /// <summary>
    /// EN: Verifies autonomous EXECUTE STATEMENT changes survive a rollback on the outer Dapper transaction.
    /// PT: Verifica se alteracoes autonomas de EXECUTE STATEMENT sobrevivem ao rollback da transacao externa Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldCommitAutonomousExecuteStatementChanges_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var outerTransaction = connection.BeginTransaction();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (13, ''Autonomous'')'
        WITH AUTONOMOUS TRANSACTION;
END
""");

        outerTransaction.Rollback();

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(13) == true);
    }

    /// <summary>
    /// EN: Verifies common-transaction EXECUTE STATEMENT changes stay attached to the outer Dapper transaction.
    /// PT: Verifica se alteracoes de EXECUTE STATEMENT com transacao comum permanecem ligadas à transacao externa Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldFollowCommonExecuteStatementTransaction_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();
        using var outerTransaction = connection.BeginTransaction();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (14, ''Common'')'
        ON EXTERNAL DATA SOURCE 'ignored'
        WITH COMMON TRANSACTION;
END
""");

        outerTransaction.Rollback();

        Assert.Equal(1, affected);
        Assert.Empty(users);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN ANY DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN ANY DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenAnyDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO MissingTable (Id, Name) VALUES (1, ''Fail'')';
    WHEN ANY DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (15, 'Handled');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(15) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN SQLCODE DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN SQLCODE DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlCodeDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLCODE DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (16, 'SqlCode');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(16) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using the Firebird SQLCODE -803 form through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando a forma Firebird SQLCODE -803 pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlCodeMinus803Do_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLCODE -803 DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (27, 'SqlCodeMinus803');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(27) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can resolve a comma-separated WHEN SQLCODE selector list through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue resolver uma lista de seletores WHEN SQLCODE separada por virgula pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlCodeSelectorListDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLCODE -999, -803 DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (32, 'SqlCodeSelectorList');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(32) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN GDSCODE DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN GDSCODE DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (17, 'GdsCode');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(17) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a specific WHEN SQLCODE &lt;code&gt; DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler especifico WHEN SQLCODE &lt;codigo&gt; DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlCodeSpecificDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLCODE 1062 DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (19, 'SqlCodeSpecific');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(19) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a specific WHEN GDSCODE &lt;name&gt; DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler especifico WHEN GDSCODE &lt;nome&gt; DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeSpecificDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE no_dup DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (20, 'GdsCodeSpecific');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(20) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE primary_key DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (25, 'PrimaryKey');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(25) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN SQLSTATE '23000' DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN SQLSTATE '23000' DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlState23000Do_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLSTATE '23000' DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (34, 'SqlState23000');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(34) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN SQLSTATE '23000' DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN SQLSTATE '23000' DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenSqlState23000ForeignKeyDo_Test()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("Parents");
        parents.AddColumn("Id", DbType.Int32, false);
        parents.AddPrimaryKeyIndexes("Id");
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("Children");
        children.AddColumn("ParentId", DbType.Int32, false);
        children.CreateForeignKey(
            "FK_CHILDREN_PARENTS",
            "Parents",
            [("ParentId", "Id")]);

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';
    WHEN SQLSTATE '23000' DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (35, 'SqlState23000ForeignKey');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(35) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_violation DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_violation DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyViolationDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE primary_key_violation DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (30, 'PrimaryKeyViolation');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(30) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_violation DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_violation DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyViolationAliasDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE primary_key_violation DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (32, 'PrimaryKeyViolationAlias');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(32) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_exists DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_exists DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyExistsDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN GDSCODE primary_key_exists DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (27, 'PrimaryKeyExists');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(27) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyDo_Test()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("Parents");
        parents.AddColumn("Id", DbType.Int32, false);
        parents.AddPrimaryKeyIndexes("Id");
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("Children");
        children.AddColumn("ParentId", DbType.Int32, false);
        children.CreateForeignKey(
            "FK_CHILDREN_PARENTS",
            "Parents",
            [("ParentId", "Id")]);

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';
    WHEN GDSCODE foreign_key DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (23, 'ForeignKey');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(23) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key_violation DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key_violation DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyViolationDo_Test()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("Parents");
        parents.AddColumn("Id", DbType.Int32, false);
        parents.AddPrimaryKeyIndexes("Id");
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("Children");
        children.AddColumn("ParentId", DbType.Int32, false);
        children.CreateForeignKey(
            "FK_CHILDREN_PARENTS",
            "Parents",
            [("ParentId", "Id")]);

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';
    WHEN GDSCODE foreign_key_violation DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (31, 'ForeignKeyViolation');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(31) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key_violation DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key_violation DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyViolationAliasDo_Test()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("Parents");
        parents.AddColumn("Id", DbType.Int32, false);
        parents.AddPrimaryKeyIndexes("Id");
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("Children");
        children.AddColumn("ParentId", DbType.Int32, false);
        children.CreateForeignKey(
            "FK_CHILDREN_PARENTS",
            "Parents",
            [("ParentId", "Id")]);

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';
    WHEN GDSCODE foreign_key_violation DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (33, 'ForeignKeyViolationAlias');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(33) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE not_null_violation DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE not_null_violation DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeNotNullDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';
    WHEN GDSCODE not_null_violation DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (24, 'NotNull');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(24) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a referenced-row error using a specific WHEN GDSCODE referenced_row DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de linha referenciada usando um handler especifico WHEN GDSCODE referenced_row DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeReferencedRowDo_Test()
    {
        var db = new FirebirdDbMock();
        var parents = db.AddTable("Parents");
        parents.AddColumn("Id", DbType.Int32, false);
        parents.AddPrimaryKeyIndexes("Id");
        parents.Add(new Dictionary<int, object?> { [0] = 1 });

        var children = db.AddTable("Children");
        children.AddColumn("ParentId", DbType.Int32, false);
        children.CreateForeignKey(
            "FK_CHILDREN_PARENTS",
            "Parents",
            [("ParentId", "Id")]);
        children.Add(new Dictionary<int, object?> { [0] = 1 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'DELETE FROM Parents WHERE Id = 1';
    WHEN GDSCODE referenced_row DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (26, 'ReferencedRow');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(26) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE primary_key_notnull DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE primary_key_notnull DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyNotNullDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';
    WHEN GDSCODE primary_key_notnull DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (28, 'PrimaryKeyNotNull');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(28) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE not_valid DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE not_valid DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeNotValidDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';
    WHEN GDSCODE not_valid DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (29, 'NotValid');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(29) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can resolve a comma-separated WHEN GDSCODE selector list through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue resolver uma lista de seletores WHEN GDSCODE separada por virgula pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenGdsCodeSelectorListDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';
    WHEN GDSCODE primary_key, not_valid DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (30, 'SelectorList');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(30) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK keeps the first matching WHEN handler when multiple handlers are present through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK preserva o primeiro handler WHEN correspondente quando ha varios handlers pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldUseFirstWhenHandler_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO MissingTable (Id, Name) VALUES (1, ''Fail'')';
    WHEN SQLCODE 1062 DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (21, 'FirstHandler');
    END
    WHEN ANY DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (22, 'SecondHandler');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(21) == true);
        Assert.DoesNotContain(users, row => row[0]?.Equals(22) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN EXCEPTION &lt;name&gt; DO handler through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN EXCEPTION &lt;nome&gt; DO pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldHandleWhenExceptionDo_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN EXCEPTION E_FAIL DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (18, 'Exception');
    END
END
""");

        Assert.Equal(1, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.Contains(users, row => row[0]?.Equals(18) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK does not match WHEN EXCEPTION handlers with a different logical name through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK nao corresponde a handlers WHEN EXCEPTION com nome logico diferente pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldNotHandleWhenExceptionWithDifferentName_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);
        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        Assert.Throws<SqlMockException>(() => connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';
    WHEN EXCEPTION E_OTHER DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (31, 'WrongException');
    END
END
"""));
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);
        Assert.DoesNotContain(users, row => row[0]?.Equals(31) == true);
    }

    /// <summary>
    /// EN: Verifies ON EXTERNAL DATA SOURCE is preserved on the cloned external connection through the Dapper surface.
    /// PT: Verifica se ON EXTERNAL DATA SOURCE e preservado na conexao externa clonada pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldPreserveExternalDataSourceOnClonedConnection_Test()
    {
        FirebirdConnectionSpyMock.LastSetConnectionString = null;

        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        var spyConnection = new FirebirdConnectionSpyMock(db);
        spyConnection.Open();

        var affected = spyConnection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (40, ''ExternalDs'')'
        ON EXTERNAL DATA SOURCE 'fb://external-db'
        AS USER 'SYSDBA'
        PASSWORD 'masterkey'
        ROLE 'RDB$ADMIN';
END
""");

        Assert.Equal(1, affected);
        Assert.Contains("DATA SOURCE=fb://external-db", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("USER=SYSDBA", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("ROLE=RDB$ADMIN", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("PASSWORD=masterkey", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Equal("fb://external-db", FirebirdConnectionSpyMock.LastSetDataSource);
        Assert.Contains(users, row => row[0]?.Equals(40) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts Firebird external EXECUTE STATEMENT clauses through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK aceita clausulas externas do EXECUTE STATEMENT do Firebird pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldAcceptExternalExecuteStatementClauses_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (11, ''External'')'
        ROLE 'RDB$ADMIN'
        PASSWORD 'masterkey'
        AS USER 'SYSDBA'
        ON EXTERNAL DATA SOURCE 'ignored'
        WITH CALLER PRIVILEGES;
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(11) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE STATEMENT external user and role clauses affect Firebird context values through the Dapper surface.
    /// PT: Verifica se as clausulas externas de usuario e role do EXECUTE STATEMENT afetam os valores de contexto Firebird pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldUseExternalExecuteStatementIdentity_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK
RETURNS (ctx VARCHAR(80))
AS
BEGIN
    EXECUTE STATEMENT 'SELECT CURRENT_USER || ''|'' || CURRENT_ROLE FROM RDB$DATABASE'
        AS USER 'ALTUSER'
        ROLE 'RDB$ADMIN'
        ON EXTERNAL DATA SOURCE 'ignored'
        INTO :ctx;
    INSERT INTO Users (Id, Name) VALUES (12, :ctx);
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(12) == true);
        Assert.Contains(users, row => row[1]?.Equals("ALTUSER|RDB$ADMIN") == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE STATEMENT accepts Firebird option clauses in different orders through the Dapper surface.
    /// PT: Verifica se EXECUTE STATEMENT aceita clausulas de opcao do Firebird em ordens diferentes pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldAcceptExecuteStatementOptionClausesInAnyOrder_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (12, ''Ordered'')'
        PASSWORD 'masterkey'
        ON EXTERNAL DATA SOURCE 'ignored'
        ROLE 'RDB$ADMIN'
        AS USER 'SYSDBA'
        WITH COMMON TRANSACTION;
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(12) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK resolves declared input parameters through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK resolve parametros de entrada declarados pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldResolveDeclaredInputParameters_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 1)
AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (:tenantId, 'Grace');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can assign RETURNS variables and reuse them later through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK pode atribuir variaveis de RETURNS e reutiliza-las depois pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldAssignReturnVariables_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 1)
RETURNS (outValue INT)
AS
BEGIN
    outValue = tenantId + 1;
    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Hank');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes nested BEGIN ... END compound statements through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK executa blocos compostos BEGIN ... END aninhados pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteNestedCompoundBlocks_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK AS
BEGIN
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (7, 'Ivy');
        INSERT INTO Users (Id, Name) VALUES (8, 'Jon');
    END;
    INSERT INTO Users (Id, Name) VALUES (9, 'Kate');
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(7) == true);
        Assert.Contains(users, row => row[0]?.Equals(8) == true);
        Assert.Contains(users, row => row[0]?.Equals(9) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates IF ... THEN ... ELSE blocks through the Dapper surface and uses the selected branch output.
    /// PT: Verifica se EXECUTE BLOCK avalia blocos IF ... THEN ... ELSE pela surface Dapper e usa a saida do ramo selecionado.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteIfThenElseCompoundBlocks_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (outValue INT)
AS
BEGIN
    IF (tenantId = 1) THEN
    BEGIN
        outValue = tenantId + 1;
    END
    ELSE
    BEGIN
        outValue = 3;
    END

    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Lia');
END
""");

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates WHILE loops through the Dapper surface and keeps loop variables scoped across iterations.
    /// PT: Verifica se EXECUTE BLOCK avalia loops WHILE pela surface Dapper e mantem variaveis do loop no escopo entre iteracoes.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteWhileLoop_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    counter = 0;
    WHILE (counter < 3) DO
    BEGIN
        counter = counter + 1;
        INSERT INTO Users (Id, Name) VALUES (:counter, 'Loop');
    END
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK stops a WHILE loop when LEAVE is reached through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK interrompe um loop WHILE quando LEAVE e alcancado pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldLeaveWhileLoop_Test()
    {
        var db = new FirebirdDbMock();
        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    counter = 0;
    WHILE (counter < 5) DO
    BEGIN
        counter = counter + 1;
        IF (counter = 3) THEN
            LEAVE;

        INSERT INTO Users (Id, Name) VALUES (:counter, 'Leave');
    END
END
""");

        Assert.Equal(2, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.DoesNotContain(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR SELECT loops through the Dapper surface and assigns selected values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR SELECT pela surface Dapper e atribui os valores selecionados a variaveis no escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteForSelectLoop_Test()
    {
        var db = new FirebirdDbMock();
        var numbers = db.AddTable("Numbers");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR SELECT Id FROM Numbers ORDER BY Id INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'For');
    END
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR EXECUTE STATEMENT loops through the Dapper surface and assigns selected values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR EXECUTE STATEMENT pela surface Dapper e atribui os valores selecionados a variaveis no escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoop_Test()
    {
        var db = new FirebirdDbMock();
        var numbers = db.AddTable("NumbersExec");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersExec ORDER BY Id' INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExec');
    END
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts AS CURSOR on FOR EXECUTE STATEMENT loops through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK aceita AS CURSOR em loops FOR EXECUTE STATEMENT pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoopWithCursor_Test()
    {
        var db = new FirebirdDbMock();
        var numbers = db.AddTable("NumbersExecCursor");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersExecCursor ORDER BY Id' INTO counter AS CURSOR cur DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExecCursor');
    END
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR EXECUTE STATEMENT loops with named parameters through the Dapper surface and assigns selected values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR EXECUTE STATEMENT com parametros nomeados pela surface Dapper e atribui os valores selecionados a variaveis no escopo.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoopWithParameters_Test()
    {
        var db = new FirebirdDbMock();
        var numbers = db.AddTable("NumbersParam");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT ('SELECT Id FROM NumbersParam WHERE Id >= :minId ORDER BY Id') (minId := 2) INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExecParam');
    END
END
""");

        Assert.Equal(2, affected);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts FOR EXECUTE STATEMENT with caller privileges clauses through the Dapper surface.
    /// PT: Verifica se EXECUTE BLOCK aceita FOR EXECUTE STATEMENT com clausulas de caller privileges pela surface Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldExecuteForExecuteStatementWithCallerPrivileges_Test()
    {
        var db = new FirebirdDbMock();
        var numbers = db.AddTable("NumbersCaller");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });
        numbers.Add(new Dictionary<int, object?> { [0] = 3 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        using var connection = new FirebirdConnectionMock(db);
        connection.Open();

        var affected = connection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersCaller ORDER BY Id' WITH CALLER PRIVILEGES INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'Caller');
    END
END
""");

        Assert.Equal(3, affected);
        Assert.Equal(3, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
        Assert.Contains(users, row => row[0]?.Equals(3) == true);
    }

    /// <summary>
    /// EN: Verifies FOR EXECUTE STATEMENT preserves the external connection string details through the cloned Dapper connection.
    /// PT: Verifica se FOR EXECUTE STATEMENT preserva os detalhes da connection string externa pela conexao clonada Dapper.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdDapper")]
    public void ExecuteBlock_ShouldPreserveExternalDataSourceOnForExecuteStatementLoop_Test()
    {
        FirebirdConnectionSpyMock.LastSetConnectionString = null;

        var db = new FirebirdDbMock();
        var numbers = db.AddTable("NumbersExternal");
        numbers.AddColumn("Id", DbType.Int32, false);
        numbers.Add(new Dictionary<int, object?> { [0] = 1 });
        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        var users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        var spyConnection = new FirebirdConnectionSpyMock(db);
        spyConnection.Open();

        var affected = spyConnection.Execute("""
EXECUTE BLOCK (tenantId INT = 0)
RETURNS (counter INT)
AS
BEGIN
    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersExternal ORDER BY Id'
        ON EXTERNAL DATA SOURCE 'fb://external-loop'
        AS USER 'SYSDBA'
        PASSWORD 'masterkey'
        ROLE 'RDB$ADMIN'
        INTO counter DO
    BEGIN
        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExecExternal');
    END
END
""");

        Assert.Equal(2, affected);
        Assert.Contains("DATA SOURCE=fb://external-loop", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("USER=SYSDBA", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("ROLE=RDB$ADMIN", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Contains("PASSWORD=masterkey", FirebirdConnectionSpyMock.LastSetConnectionString);
        Assert.Equal("fb://external-loop", FirebirdConnectionSpyMock.LastSetDataSource);
        Assert.Equal(2, users.Count);
        Assert.Contains(users, row => row[0]?.Equals(1) == true);
        Assert.Contains(users, row => row[0]?.Equals(2) == true);
    }
}
