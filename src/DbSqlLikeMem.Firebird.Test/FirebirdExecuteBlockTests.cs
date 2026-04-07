namespace DbSqlLikeMem.Firebird.Test;
/// <summary>
/// EN: Covers Firebird EXECUTE BLOCK execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de EXECUTE BLOCK no motor simulado Firebird.
/// </summary>
public sealed class FirebirdExecuteBlockTests : XUnitTestBase
{
    private sealed class FirebirdConnectionSpyMock : FirebirdConnectionMock
    {
        public static string? LastSetConnectionString { get; set; }
        public static string? LastSetDataSource { get; set; }

        public FirebirdConnectionSpyMock(FirebirdDbMock? db = null, string? defaultDatabase = null)
            : base(db, defaultDatabase)
        {
        }

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

    private readonly FirebirdDbMock db;
    private readonly FirebirdConnectionMock connection;

    private readonly ITableMock users;
    /// <summary>
    /// EN: Creates the Firebird database objects used by the execute block tests.
    /// PT: Cria os objetos de banco Firebird usados pelos testes de execute block.
    /// </summary>
    public FirebirdExecuteBlockTests(ITestOutputHelper helper) : base(helper)
    {
        db = new FirebirdDbMock();
        users = db.AddTable("Users");
        users.AddColumn("Id", DbType.Int32, false);
        users.AddColumn("Name", DbType.String, false);

        connection = new FirebirdConnectionMock(db);
        connection.Open();

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes supported statements in the block body.
    /// PT: Verifica se EXECUTE BLOCK executa as instrucoes suportadas no corpo do bloco.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ExecuteBlock_ShouldExecuteSupportedStatements()
    {
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = """
EXECUTE BLOCK AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (1, 'Alice');
    INSERT INTO Users (Id, Name) VALUES (2, 'Bob');
END
"""
        };

        var affected = command.ExecuteNonQuery();

        Assert.Equal(2, affected);
        Assert.Collection(users,
            _ => { },
            _ => { });

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can execute CREATE OR ALTER PROCEDURE statements inside the body.
    /// PT: Verifica se EXECUTE BLOCK pode executar instrucoes CREATE OR ALTER PROCEDURE dentro do corpo.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ExecuteBlock_ShouldAllowProcedureCreation()
    {
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = """
EXECUTE BLOCK AS
BEGIN
    CREATE OR ALTER PROCEDURE sp_block(IN tenantId INT) BEGIN END;
END
"""
        };

        var affected = command.ExecuteNonQuery();

        Assert.Equal(0, affected);
        Assert.True(db.TryGetProcedure("sp_block", out var procedure));
        Assert.NotNull(procedure);
        Assert.Single(procedure!.RequiredIn);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts parameter and RETURNS clauses while executing the block body.
    /// PT: Verifica se EXECUTE BLOCK aceita clausulas de parametro e RETURNS enquanto executa o corpo do bloco.
    /// </summary>
    [Fact]
    [Trait("Category", "FirebirdMock")]
    public void ExecuteBlock_ShouldAcceptParametersAndReturnsClauses()
    {
        using var command = new FirebirdCommandMock(connection)
        {
            CommandText = """
EXECUTE BLOCK (tenantId INT = 1)
RETURNS (outValue INT)
AS
BEGIN
    INSERT INTO Users (Id, Name) VALUES (3, 'Carol');
END
"""
        };

        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);

        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(3) == true && row[1]?.Equals("Carol") == true);
    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK ignores SUSPEND statements while executing supported SQL statements in the body.
    /// PT: Verifica se EXECUTE BLOCK ignora instrucoes SUSPEND enquanto executa instrucoes SQL suportadas no corpo.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldIgnoreSuspendStatements()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    SUSPEND;

    INSERT INTO Users (Id, Name) VALUES (4, 'Dave');

END

"""

        };



        var affected = command.ExecuteNonQuery();

        Assert.Equal(1, affected);
        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(4) == true && row[1]?.Equals("Dave") == true);
    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK stops executing statements after EXIT while keeping earlier work.
    /// PT: Verifica se EXECUTE BLOCK para de executar instrucoes apos EXIT mantendo o trabalho anterior.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldStopAtExit()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    INSERT INTO Users (Id, Name) VALUES (5, 'Eve');

    EXIT;

    INSERT INTO Users (Id, Name) VALUES (6, 'Frank');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);
        Assert.Contains(users, row => row[0]?.Equals(5) == true && row[1]?.Equals("Eve") == true);


        Assert.DoesNotContain(users, row => row[0]?.Equals(6) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes simple EXECUTE STATEMENT payloads through the block body.
    /// PT: Verifica se EXECUTE BLOCK executa cargas simples de EXECUTE STATEMENT pelo corpo do bloco.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteExecuteStatement()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (6, ''Frank'')';

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(6) == true && row[1]?.Equals("Frank") == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK assigns the first row returned by EXECUTE STATEMENT into scoped variables.
    /// PT: Verifica se EXECUTE BLOCK atribui a primeira linha retornada por EXECUTE STATEMENT em variaveis no escopo.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteExecuteStatementInto()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK

RETURNS (outValue INT)

AS

BEGIN

    EXECUTE STATEMENT 'SELECT 42 FROM RDB$DATABASE' INTO :outValue;

    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Into');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(42) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes parameterized EXECUTE STATEMENT payloads with Firebird clauses in the supported subset.
    /// PT: Verifica se EXECUTE BLOCK executa cargas parametrizadas de EXECUTE STATEMENT com clausulas Firebird no subset suportado.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteParameterizedExecuteStatement()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT ('INSERT INTO Users (Id, Name) VALUES (:userId, :userName)') (userId := 10, userName := 'Param') WITH AUTONOMOUS TRANSACTION;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(10) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK keeps autonomous EXECUTE STATEMENT changes after the outer transaction rolls back.
    /// PT: Verifica se EXECUTE BLOCK mantem alteracoes autonomas de EXECUTE STATEMENT depois do rollback da transacao externa.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldCommitAutonomousExecuteStatementChanges()

    {

        using var outerTransaction = connection.BeginTransaction();



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (13, ''Autonomous'')'

        WITH AUTONOMOUS TRANSACTION;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        outerTransaction.Rollback();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(13) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK keeps common-transaction EXECUTE STATEMENT changes tied to the outer transaction.
    /// PT: Verifica se EXECUTE BLOCK mantem alteracoes de EXECUTE STATEMENT com transacao comum presas à transacao externa.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldFollowCommonExecuteStatementTransaction()

    {

        using var outerTransaction = connection.BeginTransaction();



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (14, ''Common'')'

        ON EXTERNAL DATA SOURCE 'ignored'

        WITH COMMON TRANSACTION;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        outerTransaction.Rollback();



        Assert.Equal(1, affected);

        Assert.Empty(users);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN ANY DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN ANY DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenAnyDo()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO MissingTable (Id, Name) VALUES (1, ''Fail'')';

    WHEN ANY DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (15, 'Handled');

    END;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(15) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN SQLCODE DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN SQLCODE DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlCodeDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLCODE DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (16, 'SqlCode');

    END;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(16) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using the Firebird SQLCODE -803 form.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando a forma Firebird SQLCODE -803.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlCodeMinus803Do()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLCODE -803 DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (27, 'SqlCodeMinus803');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(27) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can resolve a comma-separated WHEN SQLCODE selector list.
    /// PT: Verifica se EXECUTE BLOCK consegue resolver uma lista de seletores WHEN SQLCODE separada por virgula.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlCodeSelectorListDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLCODE -999, -803 DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (32, 'SqlCodeSelectorList');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(32) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN GDSCODE DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN GDSCODE DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (17, 'GdsCode');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(17) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a specific WHEN SQLCODE &lt;code&gt; DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler especifico WHEN SQLCODE &lt;codigo&gt; DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlCodeSpecificDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLCODE 1062 DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (19, 'SqlCodeSpecific');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(19) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a specific WHEN GDSCODE &lt;name&gt; DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler especifico WHEN GDSCODE &lt;nome&gt; DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeSpecificDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE no_dup DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (20, 'GdsCodeSpecific');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(20) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE primary_key DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (25, 'PrimaryKey');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(25) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN SQLSTATE '23000' DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN SQLSTATE '23000' DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlState23000Do()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLSTATE '23000' DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (34, 'SqlState23000');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(34) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN SQLSTATE '23000' DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN SQLSTATE '23000' DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenSqlState23000ForeignKeyDo()

    {

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



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';

    WHEN SQLSTATE '23000' DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (35, 'SqlState23000ForeignKey');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(35) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_violation DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_violation DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyViolationDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE primary_key_violation DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (30, 'PrimaryKeyViolation');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(30) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_violation DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_violation DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyViolationAliasDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE primary_key_violation DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (32, 'PrimaryKeyViolationAlias');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(32) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a duplicate-key error using a specific WHEN GDSCODE primary_key_exists DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave duplicada usando um handler especifico WHEN GDSCODE primary_key_exists DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyExistsDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN GDSCODE primary_key_exists DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (27, 'PrimaryKeyExists');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(27) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyDo()

    {

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



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';

    WHEN GDSCODE foreign_key DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (23, 'ForeignKey');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(23) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key_violation DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key_violation DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyViolationDo()

    {

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



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';

    WHEN GDSCODE foreign_key_violation DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (31, 'ForeignKeyViolation');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(31) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a foreign-key error using a specific WHEN GDSCODE foreign_key_violation DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de chave estrangeira usando um handler especifico WHEN GDSCODE foreign_key_violation DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeForeignKeyViolationAliasDo()

    {

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



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Children (ParentId) VALUES (999)';

    WHEN GDSCODE foreign_key_violation DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (33, 'ForeignKeyViolationAlias');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(33) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE not_null_violation DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE not_null_violation DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeNotNullDo()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';

    WHEN GDSCODE not_null_violation DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (24, 'NotNull');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(24) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a referenced-row error using a specific WHEN GDSCODE referenced_row DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de linha referenciada usando um handler especifico WHEN GDSCODE referenced_row DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeReferencedRowDo()

    {

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



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'DELETE FROM Parents WHERE Id = 1';

    WHEN GDSCODE referenced_row DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (26, 'ReferencedRow');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(26) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE column_cannot_be_null DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE column_cannot_be_null DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodePrimaryKeyNotNullDo()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';

    WHEN GDSCODE column_cannot_be_null DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (28, 'PrimaryKeyNotNull');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(28) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from a not-null error using a specific WHEN GDSCODE column_cannot_be_null DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro de not null usando um handler especifico WHEN GDSCODE column_cannot_be_null DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeNotValidDo()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';

    WHEN GDSCODE column_cannot_be_null DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (29, 'NotValid');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(29) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can resolve a comma-separated WHEN GDSCODE selector list.
    /// PT: Verifica se EXECUTE BLOCK consegue resolver uma lista de seletores WHEN GDSCODE separada por virgula.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenGdsCodeSelectorListDo()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, NULL)';

    WHEN GDSCODE not_null_violation, column_cannot_be_null DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (30, 'SelectorList');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(30) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK keeps the first matching WHEN handler when multiple handlers are present.
    /// PT: Verifica se EXECUTE BLOCK preserva o primeiro handler WHEN correspondente quando ha varios handlers.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldUseFirstWhenHandler()

    {

        users.AddPrimaryKeyIndexes("Id");
        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN SQLCODE 1062 DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (21, 'FirstHandler');

    END;

    WHEN ANY DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (22, 'SecondHandler');

    END;

END;

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(21) == true && row[1]?.Equals("FirstHandler") == true);

        Assert.DoesNotContain(users, row => row[0]?.Equals(22) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can recover from an error using a WHEN EXCEPTION &lt;name&gt; DO handler.
    /// PT: Verifica se EXECUTE BLOCK consegue se recuperar de um erro usando um handler WHEN EXCEPTION &lt;nome&gt; DO.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldHandleWhenExceptionDo()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN EXCEPTION E_FAIL DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (18, 'Exception');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.Contains(users, row => row[0]?.Equals(18) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK does not match WHEN EXCEPTION handlers with a different logical name.
    /// PT: Verifica se EXECUTE BLOCK nao corresponde a handlers WHEN EXCEPTION com nome logico diferente.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldNotHandleWhenExceptionWithDifferentName()

    {

        users.AddPrimaryKeyIndexes("Id");

        users.Add(new Dictionary<int, object?> { [0] = 1, [1] = "Seed" });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (1, ''Fail'')';

    WHEN EXCEPTION E_OTHER DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (31, 'WrongException');

    END

END

"""

        };



        Assert.Throws<FirebirdMockException>(() => command.ExecuteNonQuery());

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(1) == true && row[1]?.Equals("Seed") == true);

        Assert.DoesNotContain(users, row => row[0]?.Equals(31) == true);

    }
    /// <summary>
    /// EN: Verifies ON EXTERNAL DATA SOURCE is preserved on the cloned external connection.
    /// PT: Verifica se ON EXTERNAL DATA SOURCE e preservado na conexao externa clonada.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldPreserveExternalDataSourceOnClonedConnection()

    {

        FirebirdConnectionSpyMock.LastSetConnectionString = null;

        var spyConnection = new FirebirdConnectionSpyMock(db);

        spyConnection.Open();



        using var command = new FirebirdCommandMock(spyConnection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (40, ''ExternalDs'')'

        ON EXTERNAL DATA SOURCE 'fb://external-db'

        AS USER 'SYSDBA'

        PASSWORD 'masterkey'

        ROLE 'RDB$ADMIN';

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Contains("DATA SOURCE=fb://external-db", FirebirdConnectionSpyMock.LastSetConnectionString);

        Assert.Contains("USER=SYSDBA", FirebirdConnectionSpyMock.LastSetConnectionString);

        Assert.Contains("ROLE=RDB$ADMIN", FirebirdConnectionSpyMock.LastSetConnectionString);

        Assert.Contains("PASSWORD=masterkey", FirebirdConnectionSpyMock.LastSetConnectionString);

        Assert.Equal("fb://external-db", FirebirdConnectionSpyMock.LastSetDataSource);

        Assert.Contains(users, row => row[0]?.Equals(40) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts Firebird external EXECUTE STATEMENT clauses in the supported subset.
    /// PT: Verifica se EXECUTE BLOCK aceita clausulas externas do EXECUTE STATEMENT do Firebird no subset suportado.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldAcceptExternalExecuteStatementClauses()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (11, ''External'')'

        ROLE 'RDB$ADMIN'

        PASSWORD 'masterkey'

        AS USER 'SYSDBA'

        ON EXTERNAL DATA SOURCE 'ignored'

        WITH CALLER PRIVILEGES;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(11) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE STATEMENT external user and role clauses affect Firebird context values in the mock.
    /// PT: Verifica se as clausulas externas de usuario e role do EXECUTE STATEMENT afetam os valores de contexto Firebird no mock.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldUseExternalExecuteStatementIdentity()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

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

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(12) == true);

        Assert.Contains(users, row => row[1]?.Equals("ALTUSER|RDB$ADMIN") == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE STATEMENT accepts Firebird option clauses in different orders inside EXECUTE BLOCK.
    /// PT: Verifica se EXECUTE STATEMENT aceita clausulas de opcao do Firebird em ordens diferentes dentro de EXECUTE BLOCK.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldAcceptExecuteStatementOptionClausesInAnyOrder()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    EXECUTE STATEMENT 'INSERT INTO Users (Id, Name) VALUES (12, ''Ordered'')'

        PASSWORD 'masterkey'

        ON EXTERNAL DATA SOURCE 'ignored'

        ROLE 'RDB$ADMIN'

        AS USER 'SYSDBA'

        WITH COMMON TRANSACTION;

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(12) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK resolves declared input parameters inside the block body.
    /// PT: Verifica se EXECUTE BLOCK resolve parametros de entrada declarados dentro do corpo do bloco.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldResolveDeclaredInputParameters()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 1)

AS

BEGIN

    INSERT INTO Users (Id, Name) VALUES (:tenantId, 'Grace');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK can assign RETURNS variables and reuse them later in the block body.
    /// PT: Verifica se EXECUTE BLOCK pode atribuir variaveis de RETURNS e reutiliza-las depois no corpo do bloco.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldAssignReturnVariables()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 1)

RETURNS (outValue INT)

AS

BEGIN

    outValue = tenantId + 1;

    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Hank');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates IF ... THEN ... ELSE blocks and uses the selected branch output.
    /// PT: Verifica se EXECUTE BLOCK avalia blocos IF ... THEN ... ELSE e usa a saida do ramo selecionado.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteIfThenElseCompoundBlocks()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

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

    END;



    INSERT INTO Users (Id, Name) VALUES (:outValue, 'Lia');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(1, affected);

        Assert.Single(users);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates WHILE loops and keeps loop variables scoped across iterations.
    /// PT: Verifica se EXECUTE BLOCK avalia loops WHILE e mantem variaveis do loop no escopo entre iteracoes.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteWhileLoop()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

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

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK stops a WHILE loop when BREAK is reached.
    /// PT: Verifica se EXECUTE BLOCK interrompe um loop WHILE quando BREAK e alcancado.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldBreakWhileLoop()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    counter = 0;

    WHILE (counter < 5) DO

    BEGIN

        counter = counter + 1;

        IF (counter = 3) THEN

            BREAK;



        INSERT INTO Users (Id, Name) VALUES (:counter, 'Break');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(2, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.DoesNotContain(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR SELECT loops and assigns the selected row values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR SELECT e atribui os valores da linha selecionada a variaveis no escopo.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForSelectLoop()

    {

        var numbers = db.AddTable("Numbers");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        numbers.Add(new Dictionary<int, object?> { [0] = 3 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR SELECT Id FROM Numbers ORDER BY Id INTO counter DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'For');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts AS CURSOR on FOR SELECT loops while preserving the selected rows.
    /// PT: Verifica se EXECUTE BLOCK aceita AS CURSOR em loops FOR SELECT enquanto preserva as linhas selecionadas.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForSelectLoopWithCursor()

    {

        var numbers = db.AddTable("NumbersCursor");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR SELECT Id FROM NumbersCursor ORDER BY Id INTO counter AS CURSOR cur DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'Cursor');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(2, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR EXECUTE STATEMENT loops and assigns selected values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR EXECUTE STATEMENT e atribui os valores selecionados a variaveis no escopo.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoop()

    {

        var numbers = db.AddTable("NumbersExec");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        numbers.Add(new Dictionary<int, object?> { [0] = 3 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersExec ORDER BY Id' INTO counter DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExec');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts AS CURSOR on FOR EXECUTE STATEMENT loops while preserving the selected rows.
    /// PT: Verifica se EXECUTE BLOCK aceita AS CURSOR em loops FOR EXECUTE STATEMENT enquanto preserva as linhas selecionadas.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoopWithCursor()

    {

        var numbers = db.AddTable("NumbersExecCursor");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        numbers.Add(new Dictionary<int, object?> { [0] = 3 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersExecCursor ORDER BY Id' INTO counter AS CURSOR cur DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExecCursor');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK evaluates FOR EXECUTE STATEMENT loops with named parameters and assigns selected values to scoped variables.
    /// PT: Verifica se EXECUTE BLOCK avalia loops FOR EXECUTE STATEMENT com parametros nomeados e atribui os valores selecionados a variaveis no escopo.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForExecuteStatementLoopWithParameters()

    {

        var numbers = db.AddTable("NumbersParam");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        numbers.Add(new Dictionary<int, object?> { [0] = 3 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR EXECUTE STATEMENT ('SELECT Id FROM NumbersParam WHERE Id >= :minId ORDER BY Id') (minId := 2) INTO counter DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'ForExecParam');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(2, affected);

        Assert.Equal(2, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK accepts FOR EXECUTE STATEMENT with autonomous transaction clauses in the supported subset.
    /// PT: Verifica se EXECUTE BLOCK aceita FOR EXECUTE STATEMENT com clausulas de transacao autonoma no subset suportado.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteForExecuteStatementWithAutonomousTransaction()

    {

        var numbers = db.AddTable("NumbersTxn");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });

        numbers.Add(new Dictionary<int, object?> { [0] = 3 });



        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK (tenantId INT = 0)

RETURNS (counter INT)

AS

BEGIN

    FOR EXECUTE STATEMENT 'SELECT Id FROM NumbersTxn ORDER BY Id' WITH AUTONOMOUS TRANSACTION INTO counter DO

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (:counter, 'Txn');

    END

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(1) == true);

        Assert.Contains(users, row => row[0]?.Equals(2) == true);

        Assert.Contains(users, row => row[0]?.Equals(3) == true);

    }
    /// <summary>
    /// EN: Verifies FOR EXECUTE STATEMENT preserves the external connection string details in the cloned connection.
    /// PT: Verifica se FOR EXECUTE STATEMENT preserva os detalhes da connection string externa na conexao clonada.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldPreserveExternalDataSourceOnForExecuteStatementLoop()

    {

        FirebirdConnectionSpyMock.LastSetConnectionString = null;



        var numbers = db.AddTable("NumbersExternal");

        numbers.AddColumn("Id", DbType.Int32, false);

        numbers.Add(new Dictionary<int, object?> { [0] = 1 });

        numbers.Add(new Dictionary<int, object?> { [0] = 2 });



        var spyConnection = new FirebirdConnectionSpyMock(db);

        spyConnection.Open();



        using var command = new FirebirdCommandMock(spyConnection)

        {

            CommandText = """

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

"""

        };



        var affected = command.ExecuteNonQuery();



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
    /// <summary>
    /// EN: Verifies EXECUTE BLOCK executes nested BEGIN ... END compound statements in the block body.
    /// PT: Verifica se EXECUTE BLOCK executa blocos compostos BEGIN ... END aninhados no corpo do bloco.
    /// </summary>

    [Fact]

    [Trait("Category", "FirebirdMock")]

    public void ExecuteBlock_ShouldExecuteNestedCompoundBlocks()

    {

        using var command = new FirebirdCommandMock(connection)

        {

            CommandText = """

EXECUTE BLOCK AS

BEGIN

    BEGIN

        INSERT INTO Users (Id, Name) VALUES (7, 'Ivy');

        INSERT INTO Users (Id, Name) VALUES (8, 'Jon');

    END;

    INSERT INTO Users (Id, Name) VALUES (9, 'Kate');

END

"""

        };



        var affected = command.ExecuteNonQuery();



        Assert.Equal(3, affected);

        Assert.Equal(3, users.Count);

        Assert.Contains(users, row => row[0]?.Equals(7) == true);

        Assert.Contains(users, row => row[0]?.Equals(8) == true);

        Assert.Contains(users, row => row[0]?.Equals(9) == true);

    }
    /// <summary>
    /// EN: Disposes the Firebird connection used by the execute block tests.
    /// PT: Descarta a conexao Firebird usada pelos testes de execute block.
    /// </summary>
    /// <param name="disposing">EN: True to dispose managed resources. PT: True para descartar recursos gerenciados.</param>

    protected override void Dispose(bool disposing)

    {

        connection.Dispose();

        base.Dispose(disposing);

    }

}
