namespace DbSqlLikeMem.Db2.Test.Parser;


/// <summary>
/// EN: Expected casing results for SQL parser corpus tests.
/// PT: Resultados esperados de capitalização nos testes de corpus do parser SQL.
/// </summary>
public enum SqlCaseExpectation
{
    /// <summary>
    /// Gets or sets a value indicating whether the parsing operation completed successfully.
    /// </summary>
    ParseOk,

    /// <summary>
    /// Represents an exception that is thrown when an invalid operation occurs.
    /// </summary>
    ThrowInvalid,

    /// <summary>
    /// Throws a NotSupportedException to indicate that the requested operation is not supported.
    /// </summary>
    ThrowNotSupported
}

/// <summary>
/// Auto-generated summary.
/// </summary>
public sealed class SqlQueryParserCorpusTests(
    ITestOutputHelper helper
) : XUnitTestBase(helper)
{

    private static object[] Case(string sql, string why, SqlCaseExpectation expectation, int minVersion = 0)
        => [sql, why, expectation, minVersion];

    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IEnumerable<object[]> Statements()
    {
        // Válidas (ParseOk)
        foreach (var row in SelectStatements())
        {
            var sql = (string)row[0];
            var why = row.Length > 1 ? (string)row[1] : "valid statement";
            var minVersion = 0;

            var trimmed = sql.TrimStart();
            if (trimmed.StartsWith("MERGE", StringComparison.OrdinalIgnoreCase))
                minVersion = Db2Dialect.MergeMinVersion;
            else if (trimmed.Contains("WITH", StringComparison.OrdinalIgnoreCase))
                minVersion = Db2Dialect.WithCteMinVersion;

            yield return Case(sql, why, SqlCaseExpectation.ParseOk, minVersion);
        }

        // Inválidas (ThrowInvalid)
        foreach (var row in InvalidSelectStatements())
        {
            var sql = (string)row[0];
            var why = row.Length > 1 ? (string)row[1] : "invalid statement";
            var minVersion = 0;

            var trimmed = sql.TrimStart();
            if (trimmed.Contains("WITH", StringComparison.OrdinalIgnoreCase))
                minVersion = Db2Dialect.WithCteMinVersion;

            yield return Case(sql, why, SqlCaseExpectation.ThrowInvalid, minVersion);
        }

        // Não-Select / incompletas (ThrowInvalid)
        foreach (var row in NonSelectStatements())
        {
            var sql = (string)row[0];
            var why = row.Length > 1 ? (string)row[1] : "non-select or incomplete statement";
            var minVersion = 0;

            var trimmed = sql.TrimStart();
            if (trimmed.Contains("WITH", StringComparison.OrdinalIgnoreCase))
                minVersion = Db2Dialect.WithCteMinVersion;

            yield return Case(sql, why, SqlCaseExpectation.ThrowInvalid, minVersion);
        }
    }

    // -----------------------------------------------------------------
    // ✅ QUERIES VÁLIDAS (devem parsear)
    // Cada item: (sql, o que está validando)
    // -----------------------------------------------------------------
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IEnumerable<object[]> SelectStatements()
    {
        // Básico / case-insensitive
        yield return new object[] { "SELECT * FROM Users", "basic SELECT/FROM (uppercase)" };
        yield return new object[] { "select * from users", "basic select/from (lowercase)" };
        yield return new object[] { "select 1", "select literal integer" };
        yield return new object[] { "select 1 + 2", "arithmetic expression" };
        yield return new object[] { "select 'abc'", "string literal" };
        yield return new object[] { "select null", "NULL literal" };
        yield return new object[] { "select true, false", "boolean literals" };

        // Identificadores / quoting
        yield return new object[] { "SELECT * FROM Users1", "identifier with digits" };
        yield return new object[] { "SELECT * FROM Users2", "identifier with digits" };
        yield return new object[] { "SELECT * FROM \"Users1\"", "double-quoted identifier" };
        yield return new object[] { "SELECT * FROM \"Users2\"", "double-quoted identifier" };
        yield return new object[] { "select u.id from db1.users u", "qualified name db.table + alias" };
        yield return new object[] { "select * from shcema1.users", "schema.table reference (typo ok for parser)" };

        // WHERE: parâmetros, AND/OR precedence, case-insensitive AND
        yield return new object[] { "SELECT * FROM Users WHERE Id = @Id", "WHERE with parameter" };
        yield return new object[] { "SELECT * FROM t WHERE first = @f AND second = @s", "WHERE with AND and params" };
        yield return new object[] { "SELECT id FROM users WHERE id = 1 OR id = 2 AND name = 'Bob'", "AND/OR precedence (AND binds tighter)" };
        yield return new object[] { "SELECT id FROM users WHERE id = 1 aNd name = 'John'", "case-insensitive AND keyword" };
        yield return new object[] { "SELECT id FROM users WHERE (id = 1 OR id = 2) AND email IS NULL", "parentheses precedence + IS NULL" };
        yield return new object[] { "SELECT id FROM users WHERE email IS NOT NULL", "IS NOT NULL" };
        yield return new object[] { "SELECT id FROM users WHERE id != 2", "not equals !=" };
        yield return new object[] { "SELECT id FROM users WHERE id = '2'", "string vs numeric literal in WHERE (parser only)" };
        yield return new object[] { "SELECT id FROM users WHERE id >= 2 AND id <= 3", "range comparisons" };

        // IN / LIKE / functions in WHERE
        yield return new object[] { "SELECT * FROM t WHERE id IN (1,3)", "IN list" };
        yield return new object[] { "SELECT id FROM users WHERE id IN (1,3)", "IN list (id)" };
        yield return new object[] { "SELECT * FROM t WHERE name LIKE 'a%'", "LIKE pattern prefix" };
        yield return new object[] { "SELECT id FROM users WHERE name LIKE '%oh%'", "LIKE pattern contains" };
        yield return new object[] { "SELECT id FROM users WHERE FIND_IN_SET('b', tags)", "function call in WHERE" };

        // SELECT list aliasing (including DB2 'name `alias`' style)
        yield return new object[] { "SELECT name AS \"User Name\" FROM users", "alias with AS using double quotes" };
        yield return new object[] { "SELECT u.id AS uid, o.id AS oid FROM users u", "multiple select item aliases" };
        yield return new object[] { "SELECT id, id + 1 AS nextId FROM users ORDER BY id", "expr alias + ORDER BY column" };

        // DISTINCT
        yield return new object[] { "SELECT DISTINCT id FROM t ORDER BY id DESC LIMIT 2 OFFSET 1", "SELECT DISTINCT + ORDER BY + LIMIT/OFFSET" };
        yield return new object[] { "SELECT DISTINCT name FROM users ORDER BY name", "DISTINCT single column + ORDER BY" };
        yield return new object[] { "select distinct tenant_id, status from users", "DISTINCT multiple columns" };
        yield return new object[] { "select count(distinct user_id) from orders", "COUNT(DISTINCT expr) aggregate (parser feature)" };

        // ORDER BY
        yield return new object[] { "SELECT * FROM emails ORDER BY CreatedDate DESC", "ORDER BY DESC" };
        yield return new object[] { "SELECT * FROM t ORDER BY iddesc ASC LIMIT 2 OFFSET 1", "ORDER BY ASC + LIMIT/OFFSET" };
        yield return new object[] { "SELECT id, id + 1 AS x FROM users ORDER BY x DESC", "ORDER BY select-item alias" };
        yield return new object[] { "SELECT id, name FROM users ORDER BY 2 ASC, 1 DESC", "ORDER BY ordinal positions" };

        // CASE/COALESCE/CONCAT/IF/IFNULL/IIF
        yield return new object[] { "SELECT id, CASE WHEN email IS NULL THEN 0 ELSE 1 END AS hasEmail FROM users ORDER BY id", "CASE WHEN expression" };
        yield return new object[] { "SELECT id, COALESCE(email, 'none') AS em FROM users ORDER BY id", "COALESCE function" };
        yield return new object[] { "SELECT id, CONCAT(name, '#', id) AS tag FROM users ORDER BY id", "CONCAT function with mixed args" };
        yield return new object[] { "SELECT id, IF(email IS NULL, 'no', 'yes') AS flag FROM users ORDER BY id", "IF(cond, a, b)" };
        yield return new object[] { "SELECT id, IFNULL(email, 'none') AS em FROM users ORDER BY id", "IFNULL(a,b)" };
        yield return new object[] { "SELECT id, IIF(email IS NULL, 0, 1) AS hasEmail FROM users ORDER BY id", "IIF(cond,a,b)" };

        // JOINs
        yield return new object[] { @"SELECT U.*, UT.TenantId 
                FROM ""User"" U
                JOIN ""UserTenant"" UT ON U.Id = UT.UserId
                WHERE U.Id = @Id", "INNER JOIN with ON + WHERE param + quoted table" };
        yield return new object[] { @"SELECT u.id, o.id AS orderId
                  FROM users u
                  LEFT JOIN orders o ON u.id = o.userId
                  ORDER BY u.id", "LEFT JOIN + ORDER BY" };
        yield return new object[] { @"SELECT u.id, o.id AS orderId
                  FROM users u
                  INNER JOIN orders o ON u.id = o.userId AND o.status = 'paid'", "INNER JOIN with compound ON (AND)" };
        yield return new object[] { @"SELECT u.id, o.id AS orderId
                  FROM users u
                  RIGHT JOIN orders o ON u.id = o.userId", "RIGHT JOIN" };

        // GROUP BY / HAVING aggregates
        yield return new object[] { @"SELECT grp
    , COUNT(id) AS C
    , SUM(amt) AS S
    , AVG(amt) AS A
    , MIN(amt) AS MI
    , MAX(amt) AS MA 
 FROM tx 
GROUP BY grp", "GROUP BY with multiple aggregates" };
        yield return new object[] { "SELECT grp, COUNT(val) AS C FROM t GROUP BY grp HAVING C > 1", "HAVING using alias (DB2 allows)" };
        yield return new object[] { @"SELECT userId, COUNT(id) AS total, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  ORDER BY userId", "GROUP BY + aggregates + ORDER BY" };
        yield return new object[] { @"SELECT userId, SUM(amount) AS sumAmount
                  FROM orders
                  GROUP BY userId
                  HAVING sumAmount >= 10", "HAVING using aggregate alias" };
        yield return new object[] { @"SELECT u.tenant_id, COUNT(*) AS total
FROM users u
WHERE u.deleted IS NULL
GROUP BY u.tenant_id
HAVING COUNT(*) >= 10;", "COUNT(*) + WHERE + GROUP BY + HAVING" };

        // Subquery FROM / scalar subquery / IN subquery / EXISTS correlated / NOT EXISTS
        yield return new object[] { @"SELECT t.Id
FROM (SELECT Id FROM (SELECT Id FROM users) x) t
ORDER BY t.Id", "nested derived tables + ORDER BY qualified column" };
        yield return new object[] { @"SELECT u.Id
FROM users u
WHERE EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id", "EXISTS correlated subquery" };
        yield return new object[] { @"SELECT u.Id
FROM users u
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.UserId = u.Id)
ORDER BY u.Id", "NOT EXISTS correlated subquery" };
        yield return new object[] { @"SELECT u.Id, o.Amount
FROM users u
JOIN (SELECT UserId, Amount FROM orders WHERE Amount > 50) o ON o.UserId = u.Id
ORDER BY u.Id, o.Amount", "JOIN with derived subquery + ORDER BY multiple keys" };

        // IN com tuplas e parâmetros especiais (parser feature)
        yield return new object[] { "select * from t where (a) in (@rows)", "IN with row-parameter placeholder" };
        yield return new object[] { "select * from t where (a,b) in (@rows)", "IN with tuple row-parameter placeholder" };
        yield return new object[] { "select * from t where a in (@ids)", "IN with parameter list placeholder" };
        yield return new object[] { "select * from t where a in ((SELECT 1 WHERE 0))", "IN with subquery (double parens)" };
        yield return new object[] { "select * from t where (a) in ((SELECT 1 WHERE 0))", "row IN with subquery (double parens)" };

        // JSON operators (parser support)
        yield return new object[] { "select json_extract(data, '$.name') from users", "JSON_EXTRACT function call" };

        // Regex / null-safe equality
        yield return new object[] { "select * from users where a <=> b", "null-safe equality <=>" };
        yield return new object[] { "select * from users where name regexp '^[A-Z]+'", "REGEXP operator" };
        yield return new object[] { "select * from users where name not regexp '[0-9]'", "NOT REGEXP operator" };

        // Comentários (split/tokenizer)
        yield return new object[] { "select * from users -- comentario", "line comment" };
        yield return new object[] { "select * from users /* bloco */", "block comment" };

        // CTE / WITH / WITH RECURSIVE
        yield return new object[] { @"
WITH active_users AS (
  SELECT u.id, u.tenant_id
  FROM users u
  WHERE u.deleted IS NULL
)
SELECT au.tenant_id, COUNT(*) AS total
FROM active_users au
GROUP BY au.tenant_id;
", "WITH CTE + GROUP BY" };

        yield return new object[] { @"WITH RECURSIVE seq(n) AS (
  SELECT 1
  UNION ALL
  SELECT n + 1 FROM seq WHERE n < 10
)
SELECT n FROM seq;
", "WITH RECURSIVE" };

        // Derived table containing WITH (DB2 8 supports WITH inside derived tables)
        yield return new object[] { @"SELECT *
FROM (
  WITH x AS (
    SELECT id, tenant_id
    FROM users
    WHERE deleted IS NULL
  )
  SELECT tenant_id, COUNT(*) AS total
  FROM x
  GROUP BY tenant_id
) dt
WHERE dt.total >= 10;
", "WITH inside derived table + outer WHERE" };

        // DISTINCT + expressão
        yield return new object[] {
    "SELECT DISTINCT (id + 1) AS x FROM users ORDER BY x",
    "DISTINCT over expression with alias"
};

        // COUNT DISTINCT com subquery correlacionada
        yield return new object[] {
    @"SELECT u.id, COUNT(DISTINCT o.status)
      FROM users u
      LEFT JOIN orders o ON o.user_id = u.id
      GROUP BY u.id",
    "COUNT(DISTINCT ...) with JOIN + GROUP BY"
};

        // EXISTS em SELECT-list (boolean scalar)
        yield return new object[] {
    "SELECT EXISTS (SELECT 1 FROM users) AS hasUsers",
    "EXISTS used as scalar expression in SELECT-list"
};

        // ORDER BY com expressão sem alias
        yield return new object[] {
    "SELECT id FROM users ORDER BY (id * 2) DESC",
    "ORDER BY expression without alias"
};

        // HAVING com expressão (sem alias)
        yield return new object[] {
    @"SELECT user_id, SUM(total)
      FROM orders
      GROUP BY user_id
      HAVING SUM(total) BETWEEN 100 AND 500",
    "HAVING with BETWEEN expression"
};

        // IN com subquery correlacionada
        yield return new object[] {
    @"SELECT u.id
      FROM users u
      WHERE u.id IN (SELECT o.user_id FROM orders o WHERE o.amount > 10)",
    "IN with correlated subquery"
};

        // NOT IN com lista literal
        yield return new object[] {
    "SELECT id FROM users WHERE id NOT IN (1,2,3)",
    "NOT IN with literal list"
};

        // CASE sem ELSE
        yield return new object[] {
    "SELECT id, CASE WHEN active = 1 THEN 'Y' END AS flag FROM users",
    "CASE expression without ELSE"
};

        // CAST com tipo numérico
        yield return new object[] {
    "SELECT CAST(id AS SIGNED) FROM users",
    "CAST to numeric type"
};

        // LIMIT somente com count
        yield return new object[] {
    "SELECT * FROM users LIMIT 5",
    "LIMIT with count only"
};

        // ORDER BY com ordinal zero
        yield return new object[] {
            "SELECT id FROM users ORDER BY 0",
            "ORDER BY ordinal"
        };

        // HAVING sem GROUP BY nem agregação
        yield return new object[] {
            "SELECT id FROM users HAVING id > 1",
            "HAVING without GROUP BY or aggregate"
        };

        yield return new object[] {
            "INSERT INTO users_archive SELECT * FROM users",
            "INSERT INTO ... SELECT simple"
        };

        yield return new object[] {
            "INSERT INTO users_archive (id, name) SELECT id, name FROM users",
            "INSERT INTO ... SELECT with explicit column list"
        };

        yield return new object[] {
            @"INSERT INTO users_archive (id, name)
              SELECT u.id, u.name
              FROM users u
              WHERE u.active = 1",
            "INSERT INTO ... SELECT with alias and WHERE"
        };

        yield return new object[] {
            @"INSERT INTO audit_log (user_id, total)
              SELECT user_id, COUNT(*)
              FROM orders
              GROUP BY user_id",
            "INSERT INTO ... SELECT with GROUP BY"
        };

        yield return new object[] {
            @"INSERT INTO audit_log (user_id, total)
              SELECT user_id, COUNT(*)
              FROM orders
              GROUP BY user_id
              HAVING COUNT(*) > 1",
            "INSERT INTO ... SELECT with GROUP BY and HAVING"
        };

        yield return new object[] {
            @"INSERT INTO t (a)
              SELECT 1",
            "INSERT INTO ... SELECT constant without FROM"
        };

        yield return new object[] {
            @"INSERT INTO users (id, name)
              SELECT id, name FROM users_tmp",
            "INSERT INTO ... SELECT from users_tmp"
        };

        yield return new object[] {
            @"INSERT INTO users (id, name)
              SELECT id, name FROM users_tmp WHERE name = 'fixed'",
            "INSERT INTO ... SELECT with literal filter"
        };

        yield return new object[] {
            @"INSERT INTO users (id, name)
              SELECT id, @name FROM users_tmp",
            "INSERT INTO ... SELECT using parameter expression"
        };

        yield return new object[] {
            @"INSERT INTO users (id, name)
              SELECT id, name FROM users_tmp
              WHERE active = 1",
            "INSERT INTO ... SELECT with WHERE"
        };

        yield return new object[] {
            @"INSERT INTO stats (grp, total)
              SELECT grp, SUM(val)
              FROM t
              GROUP BY grp",
            "INSERT INTO ... SELECT aggregate"
        };

        yield return new object[] {
            @"INSERT INTO t (a)
              SELECT CASE WHEN x = 'ON DUPLICATE' THEN 1 ELSE 0 END FROM src",
            "INSERT INTO ... SELECT containing string 'ON DUPLICATE'"
        };

        yield return new object[] {
            @"INSERT INTO t (a)
              SELECT JSON_EXTRACT(data, '$.on_duplicate') FROM src",
            "INSERT INTO ... SELECT with JSON path containing on_duplicate"
        };

        yield return new object[] { "DELETE FROM Users WHERE Id = 1", "DELETE by literal id" };
        yield return new object[] { "DELETE FROM Users WHERE Id = @Id", "DELETE by parameter" };
        yield return new object[] { "DELETE FROM p WHERE id = 42", "DELETE using short table alias" };
        yield return new object[] { "DELETE FROM parent WHERE id = 1", "DELETE from parent table" };
        yield return new object[] { "DELETE FROM user WHERE id = @id", "DELETE from reserved-name table using parameter" };
        yield return new object[] { "DELETE FROM users WHERE id = @id", "DELETE from users using parameter" };
        yield return new object[] { "DELETE FROM users WHERE id = 1", "DELETE with FROM keyword (DB2 syntax)" };

        yield return new object[] { "INSERT INTO Users (Id, Name, Email) VALUES (1, 'John Doe', 'john@example.com')", "INSERT with literals" };
        yield return new object[] { "INSERT INTO Users (Id, Name, Email) VALUES (@Id, @Name, @Email)", "INSERT with parameters" };
        yield return new object[] {
            @"INSERT INTO Users (Id, Name, Email, CreatedDate, UpdatedData, TestGuid, TestGuidNull)
              VALUES (@Id, @Name, @Email, @CreatedDate, @UpdatedData, @TestGuid, @TestGuidNull)",
            "INSERT with many parameters and nullable columns"
        };
        yield return new object[] { "INSERT INTO data (id, info) VALUES (@id, @info)", "INSERT into generic table with parameters" };
        yield return new object[] { "INSERT INTO orders (id,userId,amount) VALUES (13,0,1)", "INSERT numeric values without spaces" };
        yield return new object[] { "INSERT INTO t () VALUES ()", "INSERT default row (no columns, no values)" };
        yield return new object[] { "INSERT INTO t () VALUES ()", "INSERT default row (no columns, no values)" };
        yield return new object[] { "INSERT INTO t VALUES ()", "INSERT default row (no columns, no values)" };
        yield return new object[] { "INSERT INTO t (id) VALUES (1)", "INSERT with single column" };
        yield return new object[] {
            "INSERT INTO t (id, val) VALUES (1, 'A'), (2, 'B'), (3, 'C')",
            "INSERT multi-row VALUES"
        };
        yield return new object[] { "INSERT INTO user (name, email, created) VALUES (@name, @email, @created)", "INSERT into reserved-name table" };
        yield return new object[] { "INSERT INTO users (id, name) VALUES (1, 'Alice')", "INSERT single row literal" };
        yield return new object[] { "INSERT INTO users (id, name) VALUES (1, 'John Doe')", "INSERT with spaced string literal" };
        yield return new object[] { "INSERT INTO users (id, name, createdDate) VALUES (@id, @name, @dt)", "INSERT with date parameter" };
        yield return new object[] { "INSERT INTO users (id,name) VALUES (@id,@name)", "INSERT without spaces" };
        yield return new object[] { "INSERT INTO users (id,name,email) VALUES (4,'John','j2@x.com')", "INSERT compact literal syntax" };
        yield return new object[] { "INSERT INTO users (name) VALUES (@name)", "INSERT with identity/auto-increment column" };

        yield return new object[] { "UPDATE Users SET Name = 'Jane Doe' WHERE Id = 1", "UPDATE single column by id" };
        yield return new object[] {
    @"UPDATE Users
      SET Name = @Name, Email = @Email, UpdatedData = @UpdatedData, TestGuidNull = @TestGuidNull
      WHERE Id = @Id",
    "UPDATE multiple columns with parameters"
};
        yield return new object[] { "UPDATE gen SET gen = 123, base = 20 WHERE id = 1", "UPDATE multiple numeric columns" };
        yield return new object[] {
    "UPDATE t SET val = 'Z' WHERE grp = 'X' AND id = 1",
    "UPDATE with composite WHERE clause"
};
        yield return new object[] { "UPDATE user SET name = @name WHERE id = @id", "UPDATE reserved-name table using parameters" };
        yield return new object[] {
    @"UPDATE users
      SET name = @name,
          UpdatedData = @dtUpdate
      WHERE id = @id",
    "UPDATE with multiline SET syntax"
};
        yield return new object[] { "UPDATE users SET email = 'a@a.com', name = 'John' WHERE id = 2", "UPDATE two columns with literals" };
        yield return new object[] {
    "UPDATE users SET email = 'ok@ok.com' WHERE id = 1 aNd name = 'John'",
    "UPDATE with mixed-case AND keyword"
};
        yield return new object[] { "UPDATE users SET name = 'Bob' WHERE id = 1", "UPDATE literal string value" };
        yield return new object[] { "UPDATE users SET name = 'Jane Doe' WHERE id = 1", "UPDATE spaced string literal" };
        yield return new object[] { "UPDATE users SET name = 'X' WHERE id = 999", "UPDATE non-existing row (no-op)" };
        yield return new object[] {
    "UPDATE users SET name = 'X', email = 'x@x.com' WHERE id = 1",
    "UPDATE multiple columns by id"
};
        yield return new object[] { "UPDATE users SET name = 'Z' WHERE id = 1", "UPDATE single column simple case" };
        yield return new object[] { "UPDATE users SET name = 'Z' WHERE id = @id", "UPDATE using parameter in WHERE" };

        yield return new object[] { "delete FrOm users wHeRe id = 1", "DELETE with mixed-case keywords" };
        yield return new object[] { "uPdAtE users sEt name = 'Z' wHeRe id = 1", "UPDATE with mixed-case keywords" };

        // MERGE (não é DB2 SELECT)
        yield return new object[] {
            "MERGE INTO users u USING tmp t ON u.id = t.id WHEN MATCHED THEN UPDATE SET u.name = t.name", "In valid Merge"
        };
    }

    // -----------------------------------------------------------------
    // ❌ QUERIES INVÁLIDAS (parecem SELECT/WITH mas devem falhar)
    // Cada item: (sql, motivo)
    // -----------------------------------------------------------------
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IEnumerable<object[]> InvalidSelectStatements()
    {
        yield return new object[] { "SELECT EXISTS", "invalid: EXISTS requires subquery, e.g. EXISTS(SELECT 1)" };

        yield return new object[] { @"
select id
     , (SELEC UserId FROM user_roles R WHERE U.id = R.userId ORDER BY UserId LIMIT 1 ) 
  from shcema1.users U", "invalid: typo SELEC" };

        yield return new object[] { @"
select id
     , (SELEC UserId2 FROM user_roles R WHERE U.id = R.userId ORDER BY 1 LIMIT 1 ) 
  from shcema1.users U", "invalid: typo SELEC" };
        // DISTINCT duplicado
        yield return new object[] {
    "SELECT DISTINCT DISTINCT id FROM users",
    "invalid: duplicated DISTINCT keyword"
};

        // COUNT DISTINCT com múltiplos DISTINCT
        yield return new object[] {
    "SELECT COUNT(DISTINCT DISTINCT id) FROM users",
    "invalid: duplicated DISTINCT inside function"
};

        // EXISTS sem subquery válida
        yield return new object[] {
    "SELECT EXISTS ()",
    "invalid: EXISTS requires non-empty subquery"
};

        // BETWEEN incompleto
        yield return new object[] {
    "SELECT * FROM users WHERE id BETWEEN 1",
    "invalid: BETWEEN requires AND clause"
};

        // IN sem lista
        yield return new object[] {
    "SELECT * FROM users WHERE id IN ()",
    "invalid: IN requires at least one element or subquery"
};

        // GROUP BY com expressão inválida
        yield return new object[] {
    "SELECT id FROM users GROUP BY",
    "invalid: GROUP BY without expressions"
};

        yield return new object[] {
    "SELECT SELECT * FROM users",
    "invalid: duplicated SELECT"
};
        yield return new object[] {
    "SELECT * SELECT FROM users",
    "invalid: SELECT inside SELECT"
};
        yield return new object[] {
    "SELECT * FROM FROM users",
    "invalid: duplicated FROM"
};

        yield return new object[] {
    "SELECT * FROM users FROM t",
    "invalid: FROM inside FROM"
};
    }

    // -----------------------------------------------------------------
    // ❌ NÃO-SELECT (continua como você já tinha)
    // -----------------------------------------------------------------
    /// <summary>
    /// Auto-generated summary.
    /// </summary>
    public static IEnumerable<object[]> NonSelectStatements()
    {
        yield return new object[] { "INSERT INTO `User`" };
        yield return new object[] { "UPDATE `User`" };
        yield return new object[] { "delete" };
        yield return new object[] { "WITH u AS (SELECT id, name FROM users WHERE id <= 2)" };
        // TRUNCATE
        yield return new object[] {
    "TRUNCATE TABLE users"
};

        // CREATE TABLE
        yield return new object[] {
    "CREATE TABLE users (id INT)"
};

        // DROP TABLE
        yield return new object[] {
    "DROP TABLE users"
};

        // ALTER TABLE
        yield return new object[] {
    "ALTER TABLE users ADD COLUMN age INT"
};


        // CALL procedure
        yield return new object[] {
    "CALL my_proc(1,2,3)"
};
    }

    /// <summary>
    /// EN: Tests Parse_ShouldHandle_MultiStatementStrings_BySplitting behavior.
    /// PT: Testa o comportamento de Parse_ShouldHandle_MultiStatementStrings_BySplitting.
    /// </summary>
    [Theory]
    [MemberDataDb2Version]
    public void Parse_ShouldHandle_MultiStatementStrings_BySplitting(int version)
    {
        var d = new Db2Dialect(version);
        const string multi = "SELECT 1; SELECT 2 FROM t WHERE id = 1; INSERT INTO t(id) VALUES(1);";
        var stmts = SqlQueryParser.SplitStatementsTopLevel(multi, d);

        Assert.Equal(3, stmts.Count);

        Assert.NotNull(SqlQueryParser.Parse(stmts[0], d));
        Assert.NotNull(SqlQueryParser.Parse(stmts[1], d));
        var q3 = SqlQueryParser.Parse(stmts[2], d);
        Assert.NotNull(q3);

        // exemplo (ajuste pro seu modelo):
        Assert.True(q3 is SqlInsertQuery);
    }

    /// <summary>
    /// EN: Tests Parse_Corpus behavior.
    /// PT: Testa o comportamento de Parse_Corpus.
    /// </summary>
    [Theory]
    [MemberDataByDb2Version(nameof(Statements))]
    public void Parse_Corpus(string sql, string why, SqlCaseExpectation expectation, int minVersion, int version)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(sql, nameof(sql));

        Console.WriteLine($"Version: {version}, MinVersion: {minVersion}");
        Console.WriteLine($"Why: {why}");
        Console.WriteLine("Query: @\"" + sql + "\"");
        ConsoleWriter.Flush();

        var dialect = new Db2Dialect(version);

        // regra: se precisa de minVersion e versão atual é menor, então é NotSupported (não é inválido)
        if (minVersion > 0
            && version < minVersion
            //&& expectation == SqlCaseExpectation.ParseOk
            )
            expectation = SqlCaseExpectation.ThrowNotSupported;

#pragma warning disable CA1031 // Do not catch general exception types
        try
        {
            var parsed = SqlQueryParser.ParseMulti(sql, dialect).ToList();

            Assert.True(expectation == SqlCaseExpectation.ParseOk,
                $"Esperava {expectation} mas parseou.");

            Assert.NotEmpty(parsed);
            foreach (var q in parsed)
                Assert.NotNull(q);
        }
        catch (NotSupportedException e)
        {
            Console.WriteLine($"NotSupportedException: {e}");
            Assert.True(expectation == SqlCaseExpectation.ThrowNotSupported,
                $"Esperava {expectation} mas veio NotSupported.");
        }
        catch (Exception e)
        {
            Console.WriteLine($"Exception: {e}");
            Assert.True(expectation == SqlCaseExpectation.ThrowInvalid,
                $"Esperava {expectation} mas veio Exception.");
        }
#pragma warning restore CA1031 // Do not catch general exception types
    }
}
