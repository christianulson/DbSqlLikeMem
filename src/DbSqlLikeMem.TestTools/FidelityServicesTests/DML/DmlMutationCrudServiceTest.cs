namespace DbSqlLikeMem.TestTools.DML;

public partial class DmlMutationServiceTest<T>
{
    /// <summary>
    /// EN: Inserts typed user rows with provider parameters inside a transaction and validates that the rollback clears them.
    /// PT: Insere linhas tipadas de usuario com parametros do provedor dentro de uma transacao e valida que o rollback as remove.
    /// </summary>
    public int RunParameterTransactionRollback(params object[] pars)
    {
        var users = (string)pars[0];
        var firstName = (string)pars[1];
        var secondName = (string)pars[2];
        var firstEmail = (string)pars[3];
        var secondEmail = (string)pars[4];
        var firstCreatedAt = (DateTime)pars[5];
        var secondCreatedAt = (DateTime)pars[6];

        using var transaction = Connection.BeginTransaction();
        InsertTypedUser(users, 1, firstName, firstEmail, true, (short)31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}", transaction);
        InsertTypedUser(users, 2, secondName, secondEmail, false, (short)22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}", transaction);
        transaction.Rollback();

        using var countCommand = Connection.CreateCommand();
        countCommand.CommandText = $"""
SELECT COUNT(*)
FROM {users}
""";

        var count = Convert.ToInt32(countCommand.ExecuteScalar(), CultureInfo.InvariantCulture);
        if (count != 0)
        {
            throw new InvalidOperationException($"Unexpected transactional parameter rollback count for {Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(users);
        GC.KeepAlive(firstName);
        GC.KeepAlive(secondName);
        GC.KeepAlive(firstEmail);
        GC.KeepAlive(secondEmail);
        GC.KeepAlive(firstCreatedAt);
        GC.KeepAlive(secondCreatedAt);
        return count;
    }

    /// <summary>
    /// EN: Inserts typed user rows with provider parameters inside a transaction and validates the committed result.
    /// PT: Insere linhas tipadas de usuario com parametros do provedor dentro de uma transacao e valida o resultado confirmado.
    /// </summary>
    public int RunParameterTransactionCommit(params object[] pars)
    {
        var users = (string)pars[0];
        var firstName = (string)pars[1];
        var secondName = (string)pars[2];
        var firstEmail = (string)pars[3];
        var secondEmail = (string)pars[4];
        var firstCreatedAt = (DateTime)pars[5];
        var secondCreatedAt = (DateTime)pars[6];

        using var transaction = Connection.BeginTransaction();
        InsertTypedUser(users, 1, firstName, firstEmail, true, (short)31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}", transaction);
        InsertTypedUser(users, 2, secondName, secondEmail, false, (short)22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}", transaction);
        transaction.Commit();

        using var countCommand = Connection.CreateCommand();
        countCommand.CommandText = $"""
SELECT COUNT(*)
FROM {users}
""";

        var count = Convert.ToInt32(countCommand.ExecuteScalar(), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected transactional parameter insert count for {Dialect.DisplayName}: {count}.");
        }

        VerifyInsertedTypedUser(users, 1, firstName, firstEmail, true, (short)31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}");
        VerifyInsertedTypedUser(users, 2, secondName, secondEmail, false, (short)22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}");

        GC.KeepAlive(users);
        GC.KeepAlive(firstName);
        GC.KeepAlive(secondName);
        GC.KeepAlive(firstEmail);
        GC.KeepAlive(secondEmail);
        GC.KeepAlive(firstCreatedAt);
        GC.KeepAlive(secondCreatedAt);
        return count;
    }

    /// <summary>
    /// EN: Inserts a typed user row with nullable provider parameters and validates the persisted values.
    /// PT: Insere uma linha tipada de usuario com parametros anulaveis do provedor e valida os valores persistidos.
    /// </summary>
    public int RunParameterInsertNullRoundTrip(params object?[] pars)
    {
        var users = (string?)pars[0];
        var name = (string?)pars[1];
        var email = (string?)pars[2];
        var isActive = (bool?)pars[3];
        var age = (short?)pars[4];
        var balance = (decimal?)pars[5];
        var createdAt = (DateTime?)pars[6];
        var updatedAt = (DateTime?)pars[7];
        var profileJson = (string?)pars[8];
        var tableName = users;

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
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

        AddParameter(command, "id", DbType.Int32, 1);
        AddParameter(command, "name", DbType.String, name);
        AddParameter(command, "email", DbType.AnsiString, email is null ? DBNull.Value : email);
        AddParameter(command, "isActive", DbType.Boolean, isActive);
        AddParameter(command, "age", DbType.Int16, age);
        AddParameter(command, "balance", DbType.Currency, balance);
        AddParameter(command, "createdAt", DbType.DateTime, createdAt);
        AddParameter(command, "updatedAt", DbType.DateTime, updatedAt is null ? DBNull.Value : updatedAt.Value);
        AddParameter(command, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);

        var affected = command.ExecuteNonQuery();
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter insert rowcount for {Dialect.DisplayName}: {affected}.");
        }

        using var verifyCommand = Connection.CreateCommand();
        verifyCommand.CommandText = $"""
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
WHERE Id = 1
""";

        using var reader = verifyCommand.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(name, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(email, reader.IsDBNull(1) ? null : Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(isActive, Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(age, Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(balance, Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(
            createdAt?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(5), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(
            updatedAt is null ? null : updatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            reader.IsDBNull(6) ? null : Convert.ToDateTime(reader.GetValue(6), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(profileJson, reader.IsDBNull(7) ? null : Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.False(reader.Read());

        GC.KeepAlive(users);
        GC.KeepAlive(name);
        GC.KeepAlive(email);
        GC.KeepAlive(isActive);
        GC.KeepAlive(age);
        GC.KeepAlive(balance);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(updatedAt);
        GC.KeepAlive(profileJson);
        return affected;
    }

    /// <summary>
    /// EN: Inserts typed user rows with provider parameters and validates the persisted values and row count.
    /// PT: Insere linhas tipadas de usuario com parametros do provedor e valida os valores persistidos e a contagem de linhas.
    /// </summary>
    public int RunParameterInsertRoundTrip(params object[] pars)
    {
        var users = (string)pars[0];
        var firstName = (string)pars[1];
        var secondName = (string)pars[2];
        var firstEmail = (string)pars[3];
        var secondEmail = (string)pars[4];
        var firstIsActive = (bool)pars[5];
        var secondIsActive = (bool)pars[6];
        var firstAge = (short)pars[7];
        var secondAge = (short)pars[8];
        var firstBalance = (decimal)pars[9];
        var secondBalance = (decimal)pars[10];
        var firstCreatedAt = (DateTime)pars[11];
        var secondCreatedAt = (DateTime)pars[12];
        var firstUpdatedAt = (DateTime)pars[13];
        var secondUpdatedAt = (DateTime)pars[14];
        var firstProfileJson = (string)pars[15];
        var secondProfileJson = (string)pars[16];

        InsertTypedUser(users, 1, firstName, firstEmail, firstIsActive, firstAge, firstBalance, firstCreatedAt, firstUpdatedAt, firstProfileJson);
        InsertTypedUser(users, 2, secondName, secondEmail, secondIsActive, secondAge, secondBalance, secondCreatedAt, secondUpdatedAt, secondProfileJson);

        using var command = Connection.CreateCommand();
        command.CommandText = $"""
SELECT
    COUNT(*)
FROM {users}
""";

        var count = Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected parameter insert count for {Dialect.DisplayName}: {count}.");
        }

        VerifyInsertedTypedUser(users, 1, firstName, firstEmail, firstIsActive, firstAge, firstBalance, firstCreatedAt, firstUpdatedAt, firstProfileJson);
        VerifyInsertedTypedUser(users, 2, secondName, secondEmail, secondIsActive, secondAge, secondBalance, secondCreatedAt, secondUpdatedAt, secondProfileJson);

        GC.KeepAlive(users);
        GC.KeepAlive(firstName);
        GC.KeepAlive(secondName);
        GC.KeepAlive(firstEmail);
        GC.KeepAlive(secondEmail);
        GC.KeepAlive(firstIsActive);
        GC.KeepAlive(secondIsActive);
        GC.KeepAlive(firstAge);
        GC.KeepAlive(secondAge);
        GC.KeepAlive(firstBalance);
        GC.KeepAlive(secondBalance);
        GC.KeepAlive(firstCreatedAt);
        GC.KeepAlive(secondCreatedAt);
        GC.KeepAlive(firstUpdatedAt);
        GC.KeepAlive(secondUpdatedAt);
        GC.KeepAlive(firstProfileJson);
        GC.KeepAlive(secondProfileJson);
        return count;
    }

    /// <summary>
    /// EN: Updates typed user columns with provider parameters, deletes another row, and validates the persisted result.
    /// PT: Atualiza colunas tipadas de usuario com parametros do provedor, exclui outra linha e valida o resultado persistido.
    /// </summary>
    public int RunParameterUpdateDeleteRoundTrip(params object[] pars)
    {
        var users = (string)pars[0];
        var name = (string)pars[1];
        var email = (string)pars[2];
        var isActive = (bool)pars[3];
        var age = (short)pars[4];
        var balance = (decimal)pars[5];
        var updatedAt = (DateTime)pars[6];
        var profileJson = (string)pars[7];
        var deleteId = (int)pars[8];
        var tableName = users;

        using var updateCommand = Connection.CreateCommand();
        updateCommand.CommandText = $"""
UPDATE {tableName}
SET
    Name = {Dialect.Parameter("name")},
    Email = {Dialect.Parameter("email")},
    IsActive = {Dialect.Parameter("isActive")},
    Age = {Dialect.Parameter("age")},
    Balance = {Dialect.Parameter("balance")},
    UpdatedAt = {Dialect.Parameter("updatedAt")},
    ProfileJson = {Dialect.Parameter("profileJson")}
WHERE Id = 1
""";

        AddParameter(updateCommand, "name", DbType.String, name);
        AddParameter(updateCommand, "email", DbType.AnsiStringFixedLength, email);
        AddParameter(updateCommand, "isActive", DbType.Boolean, isActive);
        AddParameter(updateCommand, "age", DbType.Int16, age);
        AddParameter(updateCommand, "balance", DbType.Currency, balance);
        AddParameter(updateCommand, "updatedAt", DbType.DateTime, updatedAt);
        AddParameter(updateCommand, "profileJson", DbType.StringFixedLength, profileJson);

        var updated = updateCommand.ExecuteNonQuery();
        if (updated < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter update rowcount for {Dialect.DisplayName}: {updated}.");
        }

        using var deleteCommand = Connection.CreateCommand();
        deleteCommand.CommandText = $"""
DELETE FROM {tableName}
WHERE Id = {Dialect.Parameter("deleteId")}
""";
        AddParameter(deleteCommand, "deleteId", DbType.Int32, deleteId);

        var deleted = deleteCommand.ExecuteNonQuery();
        if (deleted < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter delete rowcount for {Dialect.DisplayName}: {deleted}.");
        }

        using var verifyCommand = Connection.CreateCommand();
        verifyCommand.CommandText = $"""
SELECT
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    UpdatedAt,
    ProfileJson
FROM {tableName}
WHERE Id = 1
""";

        using var reader = verifyCommand.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(name, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(email, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture)?.TrimEnd());
        Assert.Equal(isActive, Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(age, Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(balance, Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(
            updatedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(5), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(profileJson, Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture)?.TrimEnd());
        Assert.False(reader.Read());

        GC.KeepAlive(users);
        GC.KeepAlive(name);
        GC.KeepAlive(email);
        GC.KeepAlive(isActive);
        GC.KeepAlive(age);
        GC.KeepAlive(balance);
        GC.KeepAlive(updatedAt);
        GC.KeepAlive(profileJson);
        GC.KeepAlive(deleteId);
        return updated + deleted;
    }

    private void InsertTypedUser(
        string tableName,
        int id,
        string name,
        string email,
        bool isActive,
        short age,
        decimal balance,
        DateTime createdAt,
        DateTime updatedAt,
        string profileJson,
        DbTransaction? transaction = null)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $"""
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

        AddParameter(command, "id", DbType.Int32, id);
        AddParameter(command, "name", DbType.String, name);
        AddParameter(command, "email", DbType.AnsiString, email);
        AddParameter(command, "isActive", DbType.Boolean, isActive);
        AddParameter(command, "age", DbType.Int16, age);
        AddParameter(command, "balance", DbType.Currency, balance);
        AddParameter(command, "createdAt", DbType.DateTime, createdAt);
        AddParameter(command, "updatedAt", DbType.DateTime, updatedAt);
        AddParameter(command, "profileJson", DbType.String, profileJson);
        if (transaction is not null)
        {
            command.Transaction = transaction;
        }

        var affected = command.ExecuteNonQuery();
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter insert rowcount for {Dialect.DisplayName}: {affected}.");
        }
    }

    private void VerifyInsertedTypedUser(
        string tableName,
        int id,
        string name,
        string email,
        bool isActive,
        short age,
        decimal balance,
        DateTime createdAt,
        DateTime updatedAt,
        string profileJson)
    {
        using var command = Connection.CreateCommand();
        command.CommandText = $"""
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

        AddParameter(command, "id", DbType.Int32, id);

        using var reader = command.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(name, Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture));
        Assert.Equal(email, Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture));
        Assert.Equal(isActive, Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture));
        Assert.Equal(age, Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture));
        Assert.Equal(balance, Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture));
        Assert.Equal(
            createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(5), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(
            updatedAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture),
            Convert.ToDateTime(reader.GetValue(6), CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        Assert.Equal(profileJson, Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture));
        Assert.False(reader.Read());
    }

    /// <summary>
    /// EN: Reads a user name by primary key and validates the updated value.
    /// PT: Lê um nome de usuario pela chave primaria e valida o valor atualizado.
    /// </summary>
    public string RunUpdateByPk(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected update result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value!;
    }

    /// <summary>
    /// EN: Deletes one user row and validates the remaining row count.
    /// PT: Exclui uma linha de usuario e valida a contagem de linhas restante.
    /// </summary>
    public int RunDeleteByPk(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.DeleteUserById(users, 1));
        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected delete count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Executes the join query between users and orders and validates the count.
    /// PT: Executa a consulta de junção entre usuarios e pedidos e valida a contagem.
    /// </summary>
    public int RunSelectJoin(params object[] pars)
    {
        var users = (string)pars[0];
        var orders = (string)pars[1];
        var value = Convert.ToInt32(ExecuteScalar(Dialect.CountJoinForUser(users, orders, 1)), CultureInfo.InvariantCulture);
        if (value != 2)
        {
            throw new InvalidOperationException($"Unexpected join count for {Dialect.DisplayName}: {value}.");
        }

        return value;
    }

    /// <summary>
    /// EN: Updates a row and validates the affected-row count reported by the provider.
    /// PT: Atualiza uma linha e valida a contagem de linhas afetadas informada pelo provedor.
    /// </summary>
    public int RunRowCountAfterUpdate(params object[] pars)
    {
        var users = (string)pars[0];
        var affected = ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected update rowcount for {Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }

    /// <summary>
    /// EN: Updates one user row, deletes another row, and validates the final row count and remaining value.
    /// PT: Atualiza uma linha de usuario, exclui outra linha e valida a contagem final e o valor restante.
    /// </summary>
    public int RunUpdateDeleteRoundTrip(params object[] pars)
    {
        var users = (string)pars[0];
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"));
        ExecuteNonQuery(Dialect.DeleteUserById(users, 2));

        var remaining = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(remaining, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected update/delete result for {Dialect.DisplayName}: {remaining ?? "<null>"}.");
        }

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected update/delete count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    /// <summary>
    /// EN: Updates one row and deletes another inside a transaction, then validates the committed result.
    /// PT: Atualiza uma linha e exclui outra dentro de uma transacao, depois valida o resultado confirmado.
    /// </summary>
    public int RunTransactionalUpdateDeleteCommit(params object[] pars)
    {
        var users = (string)pars[0];

        using var transaction = Connection.BeginTransaction();
        ExecuteNonQuery(Dialect.UpdateUserNameById(users, 1, "Alice-v2"), transaction);
        ExecuteNonQuery(Dialect.DeleteUserById(users, 2), transaction);
        transaction.Commit();

        var value = Convert.ToString(ExecuteScalar(Dialect.SelectUserNameById(users, 1)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected transactional update/delete result for {Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        var count = Convert.ToInt32(ExecuteScalar(Dialect.CountRows(users)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected transactional update/delete count for {Dialect.DisplayName}: {count}.");
        }

        return count;
    }

    private static void AddParameter(DbCommand command, string name, DbType dbType, object? value)
    {
        var parameter = command.CreateParameter();
        parameter.ParameterName = name;
        parameter.DbType = dbType;
        parameter.Value = value ?? DBNull.Value;
        command.Parameters.Add(parameter);
    }
}
