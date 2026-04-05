namespace DbSqlLikeMem.Firebird.Dapper.Test;

/// <summary>
/// EN: Covers Firebird EXECUTE BLOCK scenarios through the Dapper-facing provider surface.
/// PT: Cobre cenarios de EXECUTE BLOCK pela surface do provedor exposta ao Dapper.
/// </summary>
public sealed class FirebirdExecuteBlockDapperTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{
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
}
