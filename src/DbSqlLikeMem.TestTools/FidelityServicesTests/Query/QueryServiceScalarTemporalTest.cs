namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest<T>
{
    /// <summary>
    /// EN: Executes a scalar date query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de data e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunDateScalar()
    {
        var value = ExecuteScalar(Dialect.DateScalar());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the JSON scalar benchmark when the provider supports it.
    /// PT: Executa o benchmark escalar de JSON quando o provedor suporta isso.
    /// </summary>
    public object? RunJsonScalarRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON scalar benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonScalarRead("{\"name\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the nested JSON path benchmark when the provider supports it.
    /// PT: Executa o benchmark de caminho JSON aninhado quando o provedor suporta isso.
    /// </summary>
    public object? RunJsonPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonPathRead("{\"user\":{\"name\":\"Alice\"}}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the JSON insert and cast benchmark when the provider supports JSON reads.
    /// PT: Executa o benchmark de insert e cast de JSON quando o provedor suporta leituras JSON.
    /// </summary>
    public object? RunJsonInsertCast()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the JSON insert/cast benchmark.");
        }

        var value = ExecuteScalar(Dialect.JsonScalarRead("{\"value\":42,\"text\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a current timestamp scalar query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de timestamp atual e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunTemporalCurrentTimestamp()
    {
        var value = ExecuteScalar(Dialect.TemporalCurrentTimestamp());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a temporal date-add query and keeps the provider result alive.
    /// PT: Executa uma consulta temporal de soma de data e mantém o resultado do provedor vivo.
    /// </summary>
    public object? RunTemporalDateAdd()
    {
        var value = ExecuteScalar(Dialect.TemporalDateAdd());
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the provider string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings do provedor sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregate(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregate(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the ordered string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao ordenada de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateOrdered(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateOrdered(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the distinct string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao distinta de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateDistinct(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateDistinct(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the custom-separator string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao com separador customizado sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateCustomSeparator(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateCustomSeparator(users, ";")), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the large-group string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings em grupo grande sobre nomes de usuarios de exemplo.
    /// </summary>
    public string? RunStringAggregateLargeGroup(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var value = Convert.ToString(ExecuteScalar(Dialect.StringAggregateLargeGroup(users)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a string-aggregation summary query with total, distinct, and repeated-name counts over sample user names.
    /// PT: Executa uma consulta resumo de agregacao de strings com contagens total, distinta e de nomes repetidos sobre nomes de usuarios de exemplo.
    /// </summary>
    public (string? Ordered, int TotalCount, int DistinctCount, int BobCount) RunStringAggregateSummaryMatrix(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        var ordered = Convert.ToString(ExecuteScalar(Dialect.StringAggregateOrdered(users)), CultureInfo.InvariantCulture);
        var totalCount = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(*) FROM {users}"), CultureInfo.InvariantCulture);
        var distinctCount = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(DISTINCT Name) FROM {users}"), CultureInfo.InvariantCulture);
        var bobCount = Convert.ToInt32(ExecuteScalar($"SELECT COUNT(*) FROM {users} WHERE Name = 'Bob'"), CultureInfo.InvariantCulture);

        GC.KeepAlive(ordered);
        GC.KeepAlive(totalCount);
        GC.KeepAlive(distinctCount);
        GC.KeepAlive(bobCount);
        return (ordered, totalCount, distinctCount, bobCount);
    }

    /// <summary>
    /// EN: Executes a grouped string report with CASE and COALESCE over sample user names.
    /// PT: Executa um relatorio agrupado de strings com CASE e COALESCE sobre nomes de usuarios de exemplo.
    /// </summary>
    public int RunStringAggregateGroupCaseMatrix(params object[] pars)
    {
        if (!Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var users = (string)pars[0];
        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END AS NameGroup,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName
FROM {users}
GROUP BY CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END
ORDER BY NameGroup
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateStringAggregateGroupCaseRow(reader, "B", 2, 1, "Bob", "Bob");

        Assert.True(reader.Read());
        ValidateStringAggregateGroupCaseRow(reader, "Other", 3, 3, "Alice", "Delta");

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 2;
    }

    /// <summary>
    /// EN: Executes a grouped name-initial report with distinct counts and HAVING filtering over the configured users table.
    /// PT: Executa um relatorio agrupado por inicial do nome com contagens distintas e filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public int RunGroupByNameInitialMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var initialExpr = $"UPPER({Dialect.StringPrefixExpression("Name", 1)})";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    SUM(CASE WHEN Name = 'Alice' THEN 1 ELSE 0 END) AS AliceCount,
    SUM(CASE WHEN Name = 'Bob' THEN 1 ELSE 0 END) AS BobCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName,
    CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END AS HasAtLeastTwo
FROM {users}
GROUP BY {initialExpr}
HAVING COUNT(*) >= 2
ORDER BY {initialExpr}
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateGroupByNameInitialRow(reader, "A", 3, 2, 2, 0, "Adam", "Alice", 1);

        Assert.True(reader.Read());
        ValidateGroupByNameInitialRow(reader, "B", 3, 2, 0, 2, "Bob", "Brian", 1);

        Assert.True(reader.Read());
        ValidateGroupByNameInitialRow(reader, "C", 2, 2, 0, 0, "Carla", "Chris", 1);

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes a grouped name report with HAVING filtering over the configured users table.
    /// PT: Executa um relatorio agrupado por nome com filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public int RunGroupByNameHavingMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    COUNT(*) AS TotalCount
FROM {users}
GROUP BY Name
HAVING COUNT(*) >= 2
ORDER BY Name
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Bob", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 2;
    }

    /// <summary>
    /// EN: Executes a GROUP BY ordinal query over the configured users table and validates grouped counts.
    /// PT: Executa uma consulta GROUP BY ordinal na tabela de usuarios configurada e valida as contagens agrupadas.
    /// </summary>
    public int RunGroupByOrdinalMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var initialExpr = $"UPPER({Dialect.StringPrefixExpression("Name", 1)})";

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount
FROM {users}
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY 1
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("A", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("B", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("C", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes an ORDER BY ordinal query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY ordinal na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public int RunOrderByOrdinalMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    Id
FROM {users}
ORDER BY 2 DESC
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Charlie", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(3, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Bravo", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(2, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Alpha", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(1, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes a DISTINCT query ordered by ordinal and validates the projected names.
    /// PT: Executa uma consulta DISTINCT ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public int RunDistinctOrderByOrdinalMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    Name
FROM {users}
ORDER BY 1
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Bob", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Charlie", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Delta", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 4;
    }

    /// <summary>
    /// EN: Executes a DISTINCT query with a text filter ordered by ordinal and validates the projected names.
    /// PT: Executa uma consulta DISTINCT com filtro de texto ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public int RunDistinctLikeOrderByOrdinalMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    UPPER(Name)
FROM {users}
WHERE UPPER(Name) LIKE '%A%'
ORDER BY 1
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("ALICE", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("CHARLIE", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("DELTA", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes an IN-list predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado IN com lista na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunInListPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Name IN ('Alice', 'Charlie')
"""), CultureInfo.InvariantCulture);

        Assert.Equal(2, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a BETWEEN predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado BETWEEN na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunBetweenPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Id BETWEEN 2 AND 4
"""), CultureInfo.InvariantCulture);

        Assert.Equal(3, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a LIKE predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado LIKE na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunLikePredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Name LIKE 'A%'
"""), CultureInfo.InvariantCulture);

        Assert.Equal(1, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY query over the configured users table and returns the matching row count.
    /// PT: Executa uma consulta combinada com BETWEEN, LIKE e ORDER BY na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunBetweenLikeOrderByMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {users}
WHERE Id BETWEEN 1 AND 4
  AND Name LIKE 'A%'
ORDER BY Name
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Aaron", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 2;
    }

    /// <summary>
    /// EN: Executes a NOT LIKE predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado NOT LIKE na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunNotLikePredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Name NOT LIKE 'A%'
"""), CultureInfo.InvariantCulture);

        Assert.Equal(4, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a not-equal predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado diferente de na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunNotEqualPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Name <> 'Bob'
"""), CultureInfo.InvariantCulture);

        Assert.Equal(4, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes an equality predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado de igualdade na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunEqualPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Name = 'Bob'
"""), CultureInfo.InvariantCulture);

        Assert.Equal(1, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a parameterized name lookup over the configured users table and returns the matched name.
    /// PT: Executa uma consulta parametrizada por nome na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public string? RunParameterSelectByNameMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var name = (string)pars[1];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {users}
WHERE Name = {Dialect.Parameter("name")}
""";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "name";
        parameter.DbType = DbType.String;
        parameter.Value = name;
        command.Parameters.Add(parameter);

        var result = Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(name, result);
        GC.KeepAlive(result);
        GC.KeepAlive(users);
        GC.KeepAlive(name);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameterized id lookup over the configured users table and returns the matched name.
    /// PT: Executa uma consulta parametrizada por id na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public string? RunParameterSelectByIdMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var id = (int)pars[1];
        var expectedName = (string)pars[2];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {users}
WHERE Id = {Dialect.Parameter("id")}
""";

        var parameter = command.CreateParameter();
        parameter.ParameterName = "id";
        parameter.DbType = DbType.Int32;
        parameter.Value = id;
        command.Parameters.Add(parameter);

        var result = Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        Assert.Equal(expectedName, result);
        GC.KeepAlive(result);
        GC.KeepAlive(users);
        GC.KeepAlive(id);
        GC.KeepAlive(expectedName);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameter roundtrip over typed user columns and validates string, numeric, boolean, date, and null parameters.
    /// PT: Executa um roundtrip de parametros sobre colunas tipadas de usuarios e valida parametros de texto, numericos, booleanos, data e nulos.
    /// </summary>
    public int RunParameterRoundTripMatrix(params object[] pars)
    {
        var users = (string)pars[0];
        var uId = (string)pars[1];
        var tableName = $"{users}_{uId}";
        var id = (int)pars[2];
        var name = (string)pars[3];
        var email = (string?)pars[4];
        var isActive = (bool)pars[5];
        var age = (short)pars[6];
        var balance = (decimal)pars[7];
        var createdAt = (DateTime)pars[8];
        var updatedAt = pars[9] is DBNull ? (DateTime?)null : (DateTime)pars[9];
        var profileJson = (string?)pars[10];

        using var insertCommand = Connection.CreateCommand();
        insertCommand.CommandText = $"""
INSERT INTO {tableName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES (
    {Dialect.Parameter("id")},
    {Dialect.Parameter("name")},
    {Dialect.Parameter("email")},
    {Dialect.Parameter("isActive")},
    {Dialect.Parameter("age")},
    {Dialect.Parameter("balance")},
    {Dialect.Parameter("createdAt")},
    {Dialect.Parameter("updatedAt")},
    {Dialect.Parameter("profileJson")}
)
""";

        AddParameter(insertCommand, "id", DbType.Int32, id);
        AddParameter(insertCommand, "name", DbType.String, name);
        AddParameter(insertCommand, "email", DbType.String, email is null ? DBNull.Value : email);
        AddParameter(insertCommand, "isActive", DbType.Boolean, isActive);
        AddParameter(insertCommand, "age", DbType.Int16, age);
        AddParameter(insertCommand, "balance", DbType.Decimal, balance);
        AddParameter(insertCommand, "createdAt", DbType.DateTime, createdAt);
        AddParameter(insertCommand, "updatedAt", DbType.DateTime, updatedAt is null ? DBNull.Value : updatedAt.Value);
        AddParameter(insertCommand, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);

        Assert.Equal(1, insertCommand.ExecuteNonQuery());

        using var selectCommand = Connection.CreateCommand();
        selectCommand.CommandText = $"""
SELECT
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
FROM {tableName}
WHERE Id = {Dialect.Parameter("id")}
""";

        AddParameter(selectCommand, "id", DbType.Int32, id);

        using var reader = selectCommand.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(name, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(email, reader.IsDBNull(1) ? null : Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(isActive, Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(age, Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(balance, Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(
            createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(5), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

        var updatedAtText = reader.IsDBNull(6)
            ? null
            : Convert.ToDateTime(reader.GetValue(6), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        Assert.Equal(updatedAt is null ? null : updatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture), updatedAtText);
        Assert.Equal(profileJson, reader.IsDBNull(7) ? null : Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());

        GC.KeepAlive(tableName);
        GC.KeepAlive(id);
        GC.KeepAlive(name);
        GC.KeepAlive(email);
        GC.KeepAlive(isActive);
        GC.KeepAlive(age);
        GC.KeepAlive(balance);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(updatedAt);
        GC.KeepAlive(profileJson);
        return 1;
    }

    /// <summary>
    /// EN: Executes a typed parameter projection and validates ANSI text, fixed-length text, numeric, temporal, GUID, and binary values returned by provider-specific parameter objects.
    /// PT: Executa uma projeção de parametros tipados e valida valores de texto ANSI, texto de comprimento fixo, numericos, temporais, GUID e binario retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public int RunParameterTypeMatrix(params object[] pars)
    {
        var text = (string)pars[0];
        var ansiText = (string)pars[1];
        var ansiFixedText = (string)pars[2];
        var fixedText = (string)pars[3];
        var int16Value = (short)pars[4];
        var int32Value = (int)pars[5];
        var int64Value = (long)pars[6];
        var boolValue = (bool)pars[7];
        var decimalValue = (decimal)pars[8];
        var doubleValue = (double)pars[9];
        var timeSpanValue = (TimeSpan)pars[10];
        var dateTimeOffsetValue = (DateTimeOffset)pars[11];
        var dateTimeValue = (DateTime)pars[12];
        var guidValue = (Guid)pars[13];
        var binaryValue = (byte[])pars[14];

        using var command = Connection.CreateCommand();
        command.CommandText = Dialect.SelectParameterProjection($"""
SELECT
    {Dialect.Parameter("text")} AS TextValue,
    {Dialect.Parameter("ansiText")} AS AnsiTextValue,
    {Dialect.Parameter("ansiFixedText")} AS AnsiFixedTextValue,
    {Dialect.Parameter("fixedText")} AS FixedTextValue,
    {Dialect.Parameter("int16Value")} AS Int16Value,
    {Dialect.Parameter("int32Value")} AS Int32Value,
    {Dialect.Parameter("int64Value")} AS Int64Value,
    {Dialect.Parameter("boolValue")} AS BoolValue,
    {Dialect.Parameter("decimalValue")} AS DecimalValue,
    {Dialect.Parameter("doubleValue")} AS DoubleValue,
    {Dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {Dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {Dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {Dialect.Parameter("guidValue")} AS GuidValue,
    {Dialect.Parameter("binaryValue")} AS BinaryValue
""");

        AddParameter(command, "text", DbType.String, text);
        AddParameter(command, "ansiText", DbType.AnsiString, ansiText);
        AddParameter(command, "ansiFixedText", DbType.AnsiStringFixedLength, ansiFixedText);
        AddParameter(command, "fixedText", DbType.StringFixedLength, fixedText);
        AddParameter(command, "int16Value", DbType.Int16, int16Value);
        AddParameter(command, "int32Value", DbType.Int32, int32Value);
        AddParameter(command, "int64Value", DbType.Int64, int64Value);
        AddParameter(command, "boolValue", DbType.Boolean, boolValue);
        AddParameter(command, "decimalValue", DbType.Decimal, decimalValue);
        AddParameter(command, "doubleValue", DbType.Double, doubleValue);
        AddParameter(command, "timeSpanValue", DbType.Time, timeSpanValue);
        AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, dateTimeOffsetValue);
        AddParameter(command, "dateTimeValue", DbType.DateTime, dateTimeValue);
        AddParameter(command, "guidValue", DbType.Guid, guidValue);
        AddParameter(command, "binaryValue", DbType.Binary, binaryValue);

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(text, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(ansiText, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(ansiFixedText, Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture)?.TrimEnd());
        Assert.Equal(fixedText, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture)?.TrimEnd());
        Assert.Equal(int16Value, Convert.ToInt16(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(int32Value, Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(int64Value, Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(boolValue, Convert.ToBoolean(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.Equal(decimalValue, Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture));
        Assert.Equal(doubleValue, Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture));
        Assert.Equal(
            timeSpanValue,
            NormalizeTimeSpanValue(reader.GetValue(10)));
        Assert.Equal(dateTimeOffsetValue, NormalizeDateTimeOffsetValue(reader.GetValue(11)));
        Assert.Equal(
            dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(12), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(guidValue, NormalizeGuidValue(reader.GetValue(13)));
        Assert.True(binaryValue.AsSpan().SequenceEqual(NormalizeBinaryValue(reader.GetValue(14))));

        Assert.False(reader.Read());

        GC.KeepAlive(text);
        GC.KeepAlive(ansiText);
        GC.KeepAlive(ansiFixedText);
        GC.KeepAlive(fixedText);
        GC.KeepAlive(int16Value);
        GC.KeepAlive(int32Value);
        GC.KeepAlive(int64Value);
        GC.KeepAlive(boolValue);
        GC.KeepAlive(decimalValue);
        GC.KeepAlive(doubleValue);
        GC.KeepAlive(timeSpanValue);
        GC.KeepAlive(dateTimeOffsetValue);
        GC.KeepAlive(dateTimeValue);
        GC.KeepAlive(guidValue);
        GC.KeepAlive(binaryValue);
        return 1;
    }

    /// <summary>
    /// EN: Executes a compact typed parameter projection for date and currency values returned by provider-specific parameter objects.
    /// PT: Executa uma projeção compacta de parametros tipados para valores de data e moeda retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public int RunParameterDateCurrencyMatrix(params object[] pars)
    {
        var dateValue = (DateTime)pars[0];
        var currencyValue = (decimal)pars[1];

        using var command = Connection.CreateCommand();
        command.CommandText = Dialect.SelectParameterProjection($"""
SELECT
    {Dialect.Parameter("dateValue")} AS DateValue,
    {Dialect.Parameter("currencyValue")} AS CurrencyValue
""");

        AddParameter(command, "dateValue", DbType.Date, dateValue);
        AddParameter(command, "currencyValue", DbType.Currency, currencyValue);

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());

        Assert.Equal(
            dateValue.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(0), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        Assert.Equal(currencyValue, Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(dateValue);
        GC.KeepAlive(currencyValue);
        return 1;
    }

    private static void AddParameter(DbCommand command, string name, DbType dbType, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = dbType;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }

    private static Guid NormalizeGuidValue(object? value)
    {
        return value switch
        {
            Guid guid => guid,
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            string text => Guid.Parse(text),
            null => throw new InvalidOperationException("GUID parameter returned a null value."),
            _ => Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("GUID parameter returned an unconvertible value."))
        };
    }

    private static DateTimeOffset NormalizeDateTimeOffsetValue(object? value)
    {
        return value switch
        {
            DateTimeOffset dateTimeOffset => dateTimeOffset,
            DateTime dateTime => new DateTimeOffset(dateTime, TimeSpan.Zero),
            string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTimeOffset parameter returned a null value."),
            _ => DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("DateTimeOffset parameter returned an unconvertible value."), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
        };
    }

    private static TimeSpan NormalizeTimeSpanValue(object? value)
    {
        return value switch
        {
            TimeSpan timeSpan => timeSpan,
            DateTime dateTime => dateTime.TimeOfDay,
            string text => TimeSpan.Parse(text, CultureInfo.InvariantCulture),
            null => throw new InvalidOperationException("TimeSpan parameter returned a null value."),
            _ => TimeSpan.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("TimeSpan parameter returned an unconvertible value."), CultureInfo.InvariantCulture)
        };
    }

    private static byte[] NormalizeBinaryValue(object? value)
    {
        return value switch
        {
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            Memory<byte> memory => memory.ToArray(),
            null => throw new InvalidOperationException("Binary parameter returned a null value."),
            _ => throw new InvalidOperationException($"Unsupported binary parameter type: {value.GetType().FullName}.")
        };
    }

    /// <summary>
    /// EN: Executes a greater-than predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado maior que na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunGreaterThanPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Id > 3
"""), CultureInfo.InvariantCulture);

        Assert.Equal(2, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a less-than predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado menor que na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunLessThanPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Id < 3
"""), CultureInfo.InvariantCulture);

        Assert.Equal(2, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a greater-than-or-equal predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado maior ou igual na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunGreaterThanOrEqualPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Id >= 3
"""), CultureInfo.InvariantCulture);

        Assert.Equal(3, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes a less-than-or-equal predicate over the configured users table and returns the matching row count.
    /// PT: Executa um predicado menor ou igual na tabela de usuarios configurada e retorna a contagem de linhas correspondentes.
    /// </summary>
    public int RunLessThanOrEqualPredicateMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        var value = Convert.ToInt32(ExecuteScalar($"""
SELECT COUNT(*)
FROM {users}
WHERE Id <= 3
"""), CultureInfo.InvariantCulture);

        Assert.Equal(3, value);
        GC.KeepAlive(users);
        return value;
    }

    /// <summary>
    /// EN: Executes an ORDER BY Name query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY Name na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public int RunOrderByNameMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {users}
ORDER BY Name
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Bob", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Charlie", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes an ORDER BY Name descending query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY Name descendente na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public int RunOrderByNameDescendingMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {users}
ORDER BY Name DESC
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        Assert.Equal("Charlie", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Bob", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.True(reader.Read());
        Assert.Equal("Alice", Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes a paged name query using ROW_NUMBER and validates the selected page rows.
    /// PT: Executa uma consulta paginada por nome usando ROW_NUMBER e valida as linhas da pagina selecionada.
    /// </summary>
    public int RunNamePaginationMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM (
    SELECT Name, ROW_NUMBER() OVER (ORDER BY Name) AS rn
    FROM {users}
) q
WHERE rn BETWEEN 2 AND 4
ORDER BY rn
""";

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateNamePaginationRow(reader, "Bravo");

        Assert.True(reader.Read());
        ValidateNamePaginationRow(reader, "Charlie");

        Assert.True(reader.Read());
        ValidateNamePaginationRow(reader, "Delta");

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 3;
    }

    /// <summary>
    /// EN: Executes a native paged name query and validates the selected page rows for the configured users table.
    /// PT: Executa uma consulta nativa paginada por nome e valida as linhas da pagina selecionada na tabela de usuarios configurada.
    /// </summary>
    public int RunPagedNameProjectionMatrix(params object[] pars)
    {
        var users = (string)pars[0];

        using var command = Connection.CreateCommand();
        command.CommandText = Dialect.PagedNameProjection(users, 1, 2);

        using var reader = command.ExecuteReader();

        Assert.True(reader.Read());
        ValidateNamePaginationRow(reader, "Bravo");

        Assert.True(reader.Read());
        ValidateNamePaginationRow(reader, "Charlie");

        Assert.False(reader.Read());
        GC.KeepAlive(users);
        return 2;
    }

    /// <summary>
    /// EN: Reads a current-time predicate query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com predicado de tempo atual na tabela de usuarios configurada.
    /// </summary>
    public object? RunTemporalNowWhere(params object[] pars)
    {
        var users = (string)pars[0];
        var value = ExecuteScalar(Dialect.TemporalNowWhere(users));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Reads a current-time ordering query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com ordenação por tempo atual na tabela de usuarios configurada.
    /// </summary>
    public object? RunTemporalNowOrderBy(params object[] pars)
    {
        var users = (string)pars[0];
        var value = ExecuteScalar(Dialect.TemporalNowOrderBy(users));
        GC.KeepAlive(value);
        return value;
    }

    private static void ValidateStringAggregateGroupCaseRow(
        DbDataReader reader,
        string expectedNameGroup,
        int expectedTotalCount,
        int expectedDistinctCount,
        string expectedFirstName,
        string expectedLastName)
    {
        Assert.Equal(expectedNameGroup, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalCount, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedDistinctCount, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFirstName, Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedLastName, Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture));
    }

    private static void ValidateGroupByNameInitialRow(
        DbDataReader reader,
        string expectedNameInitial,
        int expectedTotalCount,
        int expectedDistinctCount,
        int expectedAliceCount,
        int expectedBobCount,
        string expectedFirstName,
        string expectedLastName,
        int expectedHasAtLeastTwo)
    {
        Assert.Equal(expectedNameInitial, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(expectedTotalCount, Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(expectedDistinctCount, Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(expectedAliceCount, Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(expectedBobCount, Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(expectedFirstName, Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture));
        Assert.Equal(expectedLastName, Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture));
        Assert.Equal(expectedHasAtLeastTwo, Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture));
    }

    private static void ValidateNamePaginationRow(DbDataReader reader, string expectedName)
    {
        Assert.Equal(expectedName, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
    }
}
