namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes parser-style benchmark helpers over representative SQL snippets.
/// PT-br: Executa helpers de benchmark no estilo parser sobre trechos SQL representativos.
/// </summary>
public static class ParseServiceTest
{
    /// <summary>
    /// EN: Counts the tokens produced by a simple SELECT statement.
    /// PT-br: Conta os tokens produzidos por uma instrução SELECT simples.
    /// </summary>
    public static int RunParseSimpleSelect()
        => SimpleTokenize("SELECT Name FROM Users WHERE Id = 1");

    /// <summary>
    /// EN: Counts the tokens produced by a complex join statement.
    /// PT-br: Conta os tokens produzidos por uma instrução de junção complexa.
    /// </summary>
    public static int RunParseComplexJoin()
        => SimpleTokenize("SELECT u.Name, COUNT(o.Id) FROM Users u LEFT JOIN Orders o ON o.UsersId = u.Id WHERE u.Name LIKE 'A%' GROUP BY u.Name ORDER BY u.Name");

    /// <summary>
    /// EN: Counts the tokens produced by an INSERT RETURNING statement.
    /// PT-br: Conta os tokens produzidos por uma instrução INSERT RETURNING.
    /// </summary>
    public static int RunParseInsertReturning()
        => SimpleTokenize("INSERT INTO Users(Id, Name) VALUES(1, 'Alice') RETURNING Id");

    /// <summary>
    /// EN: Counts the tokens produced by an ON CONFLICT DO UPDATE statement.
    /// PT-br: Conta os tokens produzidos por uma instrução ON CONFLICT DO UPDATE.
    /// </summary>
    public static int RunParseOnConflictDoUpdate()
        => SimpleTokenize("INSERT INTO Users(Id, Name) VALUES(1, 'Alice') ON CONFLICT(Id) DO UPDATE SET Name = EXCLUDED.Name");

    /// <summary>
    /// EN: Counts the tokens produced by a JSON extraction statement.
    /// PT-br: Conta os tokens produzidos por uma instrução de extração JSON.
    /// </summary>
    public static int RunParseJsonExtract()
        => SimpleTokenize("SELECT JSON_VALUE('{\"user\":{\"name\":\"Alice\"}}', '$.user.name')");

    /// <summary>
    /// EN: Counts the tokens produced by a string-aggregate WITHIN GROUP statement.
    /// PT-br: Conta os tokens produzidos por uma instrução de agregacao de strings WITHIN GROUP.
    /// </summary>
    public static int RunParseStringAggregateWithinGroup()
        => SimpleTokenize("SELECT LISTAGG(Name, ',') WITHIN GROUP (ORDER BY Name) FROM Users");

    /// <summary>
    /// EN: Counts the tokens produced by an auto-dialect paging statement.
    /// PT-br: Conta os tokens produzidos por uma instrução de paginação auto-dialect.
    /// </summary>
    public static int RunParseAutoDialectTopLimitFetch()
        => SimpleTokenize("SELECT * FROM Users ORDER BY Id OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");

    /// <summary>
    /// EN: Counts the tokens produced by a multi-statement batch.
    /// PT-br: Conta os tokens produzidos por um lote de multiplas instrucoes.
    /// </summary>
    public static int RunParseMultiStatementBatch()
        => SimpleTokenize("INSERT INTO Users(Id, Name) VALUES(1, 'Alice'); UPDATE Users SET Name = 'Bob' WHERE Id = 1; SELECT Name FROM Users WHERE Id = 1;");

    private static int SimpleTokenize(string sql)
    {
        var tokens = sql
            .Replace("(", " ")
            .Replace(")", " ")
            .Replace(",", " ")
            .Replace(";", " ")
            .Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        return tokens.Length;
    }
}
