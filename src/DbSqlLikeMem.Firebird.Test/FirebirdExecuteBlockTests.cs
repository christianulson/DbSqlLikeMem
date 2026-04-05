namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Covers Firebird EXECUTE BLOCK execution scenarios in the mock engine.
/// PT: Cobre cenarios de execucao de EXECUTE BLOCK no motor simulado Firebird.
/// </summary>
public sealed class FirebirdExecuteBlockTests : XUnitTestBase
{
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
        Assert.Collection(users,
            _ => { },
            _ => { },
            _ => { });
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
        Assert.Collection(users,
            _ => { },
            _ => { },
            _ => { });
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
        Assert.Collection(users,
            _ => { },
            _ => { },
            _ => { });
        Assert.Contains(users, row => row[0]?.Equals(5) == true);
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
        Assert.Collection(users,
            _ => { },
            _ => { },
            _ => { });
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
    END

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
