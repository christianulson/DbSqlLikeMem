using DbSqlLikeMem.TestTools.FidelityServicesTests.DML;
using DbSqlLikeMem.TestTools.Json;
using System.Text.Json;

namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Inserts typed user rows with provider parameters inside a transaction and validates that the rollback clears them.
/// PT-br: Insere linhas tipadas de usuario com parametros do provedor
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationParameterTransactionRollbackServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : DmlMutationCrudServiceTestBase(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts typed user rows with provider parameters inside a transaction and validates that the rollback clears them.
    /// PT-br: Insere linhas tipadas de usuario com parametros do provedor dentro de uma transacao e valida que o rollback as remove.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstName = (string)args[0];
        var secondName = (string)args[1];
        var firstEmail = (string)args[2];
        var secondEmail = (string)args[3];
        var firstCreatedAt = (DateTime)args[4];
        var secondCreatedAt = (DateTime)args[5];

        await InsertTypedUserAsync(1, firstName, firstEmail, true, 31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}");

        using var transaction = Repo.BeginTransaction();
        await InsertTypedUserAsync(2, secondName, secondEmail, false, 22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}", transaction);

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName), transaction), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected transactional parameter uncommited count for {Repo.Dialect.DisplayName}: {count}.");
        }

        transaction.Rollback();

        var count2 = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count2 != 1)
        {
            throw new InvalidOperationException($"Unexpected transactional parameter rollback count for {Repo.Dialect.DisplayName}: {count2}.");
        }

        GC.KeepAlive(Context.TbUsersFullName);
        GC.KeepAlive(firstName);
        GC.KeepAlive(secondName);
        GC.KeepAlive(firstEmail);
        GC.KeepAlive(secondEmail);
        GC.KeepAlive(firstCreatedAt);
        GC.KeepAlive(secondCreatedAt);
        return new { count, count2 };
    }

}

/// <summary>
/// EN: Inserts typed user rows with provider parameters and validates the persisted values and row count.
/// PT-br: Insere linhas tipadas de usuario com parametros do provedor e valida os valores persistidos e a contagem de linhas.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationParameterTransactionCommitServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts typed user rows with provider parameters inside a transaction and validates the committed result.
    /// PT-br: Insere linhas tipadas de usuario com parametros do provedor dentro de uma transacao e valida o resultado confirmado.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstName = (string)args[0];
        var secondName = (string)args[1];
        var firstEmail = (string)args[2];
        var secondEmail = (string)args[3];
        var firstCreatedAt = (DateTime)args[4];
        var secondCreatedAt = (DateTime)args[5];

        using var transaction = Repo.BeginTransaction();
        await InsertTypedUserAsync(1, firstName, firstEmail, true, 31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}", transaction);
        await InsertTypedUserAsync(2, secondName, secondEmail, false, 22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}", transaction);
        transaction.Commit();

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected transactional parameter insert count for {Repo.Dialect.DisplayName}: {count}.");
        }

        await VerifyInsertedTypedUserAsync(1, firstName, firstEmail, true, (short)31, 123.45m, firstCreatedAt, firstCreatedAt, "{\"theme\":\"dark\"}");
        await VerifyInsertedTypedUserAsync(2, secondName, secondEmail, false, (short)22, 67.89m, secondCreatedAt, secondCreatedAt, "{\"theme\":\"light\"}");

        GC.KeepAlive(Context.TbUsersFullName);
        GC.KeepAlive(firstName);
        GC.KeepAlive(secondName);
        GC.KeepAlive(firstEmail);
        GC.KeepAlive(secondEmail);
        GC.KeepAlive(firstCreatedAt);
        GC.KeepAlive(secondCreatedAt);
        return count;
    }

}

/// <summary>
/// EN: Updates typed user columns with provider parameters, including Oracle empty-string normalization for email, deletes another row, and validates the persisted result.
/// PT-br: Atualiza colunas tipadas de usuario com parametros do provedor, incluindo a normalizacao de string vazia para email no Oracle, exclui outra linha e valida o resultado persistido.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationParameterInsertNullRoundTripServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a typed user row with nullable provider parameters and validates the persisted values.
    /// PT-br: Insere uma linha tipada de usuario com parametros anulaveis do provedor e valida os valores persistidos.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var name = (string?)args[0];
        var email = (string?)args[1];
        var isActive = (bool?)args[2];
        var age = (short?)args[3];
        var balance = (decimal?)args[4];
        var createdAt = (DateTime?)args[5];
        var updatedAt = (DateTime?)args[6];
        var profileJson = (string?)args[7];

        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
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
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")},
    {Repo.Dialect.Parameter("email")},
    {Repo.Dialect.Parameter("isActive")},
    {Repo.Dialect.Parameter("age")},
    {Repo.Dialect.Parameter("balance")},
    {Repo.Dialect.Parameter("createdAt")},
    {Repo.Dialect.Parameter("updatedAt")},
    {Repo.Dialect.JsonParameter("profileJson")}
)
""", addParameters: command =>
        {
            AddParameter(command, "id", DbType.Int32, 1);
            AddParameter(command, "name", DbType.String, name);
            AddParameter(command, "email", DbType.AnsiString, email is null ? DBNull.Value : email);
            AddParameter(command, "isActive", DbType.Boolean, isActive);
            AddParameter(command, "age", DbType.Int16, age);
            AddParameter(command, "balance", DbType.Currency, balance);
            AddParameter(command, "createdAt", DbType.DateTime, createdAt);
            AddParameter(command, "updatedAt", DbType.DateTime, updatedAt is null ? DBNull.Value : updatedAt.Value);
            AddParameter(command, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);
        });
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        var reader = await Repo.ExecuteReaderAsync($"""
        SELECT
            Name,
            Email,
            IsActive,
            Age,
            Balance,
            CreatedAt,
            UpdatedAt,
            ProfileJson
        FROM {Context.TbUsersFullName}
        WHERE Id = 1
        """);

        reader.Should().NotBeEmpty();
        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        row.Length.Should().Be(8);

        NormalizeNullableText(row[0]).Should().Be(name);
        NormalizeNullableText(row[1]).Should().Be(email);
        Convert.ToBoolean(row[2], CultureInfo.InvariantCulture).Should().Be(isActive!.Value);
        Convert.ToInt16(row[3], CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(row[4], CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeNullableDateTimeText(row[5]).Should().Be(createdAt?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        NormalizeNullableDateTimeText(row[6]).Should().Be(updatedAt is null ? null : updatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        JsonTextAssertions.ShouldMatchJsonText(
            NormalizeNullableText(row[7]),
        profileJson);

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

}

/// <summary>
/// EN: Inserts typed user rows with provider parameters and validates the persisted values and row count.
/// PT-br: Insere linhas tipadas de usuario com parametros do provedor e valida os valores persistidos e a contagem de linhas.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationParameterInsertRoundTripServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts typed user rows with provider parameters and validates the persisted values and row count.
    /// PT-br: Insere linhas tipadas de usuario com parametros do provedor e valida os valores persistidos e a contagem de linhas.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var firstName = (string)args[0];
        var secondName = (string)args[1];
        var firstEmail = (string)args[2];
        var secondEmail = (string)args[3];
        var firstIsActive = (bool)args[4];
        var secondIsActive = (bool)args[5];
        var firstAge = (short)args[6];
        var secondAge = (short)args[7];
        var firstBalance = (decimal)args[8];
        var secondBalance = (decimal)args[9];
        var firstCreatedAt = (DateTime)args[10];
        var secondCreatedAt = (DateTime)args[11];
        var firstUpdatedAt = (DateTime)args[12];
        var secondUpdatedAt = (DateTime)args[13];
        var firstProfileJson = (string)args[14];
        var secondProfileJson = (string)args[15];
        await InsertTypedUserAsync(1, firstName, firstEmail, firstIsActive, firstAge, firstBalance, firstCreatedAt, firstUpdatedAt, firstProfileJson);
        await InsertTypedUserAsync(2, secondName, secondEmail, secondIsActive, secondAge, secondBalance, secondCreatedAt, secondUpdatedAt, secondProfileJson);

        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 2)
        {
            throw new InvalidOperationException($"Unexpected parameter insert count for {Repo.Dialect.DisplayName}: {count}.");
        }

        await VerifyInsertedTypedUserAsync(1, firstName, firstEmail, firstIsActive, firstAge, firstBalance, firstCreatedAt, firstUpdatedAt, firstProfileJson);
        await VerifyInsertedTypedUserAsync(2, secondName, secondEmail, secondIsActive, secondAge, secondBalance, secondCreatedAt, secondUpdatedAt, secondProfileJson);

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

}

/// <summary>
/// EN: Updates typed user columns with provider parameters, including Oracle empty-string normalization for email, deletes another row, and validates the persisted result.
/// PT-br: Atualiza colunas tipadas de usuario com parametros do provedor, incluindo a normalizacao de string vazia para email no Oracle, exclui outra linha e valida o resultado persistido.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationParameterUpdateDeleteRoundTripServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Updates typed user columns with provider parameters, including Oracle empty-string normalization for email, deletes another row, and validates the persisted result.
    /// PT-br: Atualiza colunas tipadas de usuario com parametros do provedor, incluindo a normalizacao de string vazia para email no Oracle, exclui outra linha e valida o resultado persistido.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var name = (string)args[0];
        var email = (string)args[1];
        var isActive = (bool)args[2];
        var age = (short)args[3];
        var balance = (decimal)args[4];
        var updatedAt = (DateTime?)args[5];
        var profileJson = (string)args[6];
        var deleteId = (int)args[7];
        var emailSql = email.Length == 0 ? "''" : Repo.Dialect.Parameter("email");

        var updated = await Repo.ExecuteNonQueryAsync($"""
UPDATE {Context.TbUsersFullName}
SET
    Name = {Repo.Dialect.Parameter("name")},
    Email = {emailSql},
    IsActive = {Repo.Dialect.Parameter("isActive")},
    Age = {Repo.Dialect.Parameter("age")},
    Balance = {Repo.Dialect.Parameter("balance")},
    UpdatedAt = {Repo.Dialect.Parameter("updatedAt")},
    ProfileJson = {Repo.Dialect.JsonParameter("profileJson")}
WHERE Id = 1
""",
addParameters: command =>
{
    AddParameter(command, "name", DbType.String, name);
    if (email.Length > 0)
    {
        AddParameter(command, "email", DbType.AnsiStringFixedLength, email);
    }
    AddParameter(command, "isActive", DbType.Boolean, isActive);
    AddParameter(command, "age", DbType.Int16, age);
    AddParameter(command, "balance", DbType.Currency, balance);
    AddParameter(command, "updatedAt", DbType.DateTime, updatedAt);
    AddParameter(command, "profileJson", DbType.StringFixedLength, profileJson);
});
        if (updated < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter update rowcount for {Repo.Dialect.DisplayName}: {updated}.");
        }

        var deleted = await Repo.ExecuteNonQueryAsync($"""
DELETE FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("deleteId")}
""", addParameters: command =>
            AddParameter(command, "deleteId", DbType.Int32, deleteId));
        if (deleted < 1)
        {
            throw new InvalidOperationException($"Unexpected parameter delete rowcount for {Repo.Dialect.DisplayName}: {deleted}.");
        }

        var reader = await Repo.ExecuteReaderAsync($"""
        SELECT
            Name,
            Email,
            IsActive,
            Age,
            Balance,
            UpdatedAt,
            ProfileJson
        FROM {Context.TbUsersFullName}
        WHERE Id = 1
        """);
        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        row.Length.Should().Be(7);

        NormalizeNullableText(row[0]).Should().Be(name);
        NormalizeNullableText(row[1])?.TrimEnd()
            .Should().Be(NormalizeOracleNullableText(email));
        Convert.ToBoolean(row[2], CultureInfo.InvariantCulture).Should().Be(isActive);
        Convert.ToInt16(row[3], CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(row[4], CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeNullableDateTimeText(row[5])
            .Should().Be(updatedAt?.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        JsonTextAssertions.ShouldMatchJsonText(
            NormalizeNullableText(row[6])?.TrimEnd(),
            profileJson);

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

}

/// <summary>
/// EN: Reads a user name by primary key and validates the updated value.
/// PT-br: Lê um nome de usuario pela chave primaria e valida o valor atualizado.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationUpdateByPkServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Reads a user name by primary key and validates the updated value.
    /// PT-br: Lê um nome de usuario pela chave primaria e valida o valor atualizado.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the update flow. PT-br: Id principal opcional do usuario para o fluxo de update.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, userId, "Alice-v2"));
        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, userId)), CultureInfo.InvariantCulture);
        if (!string.Equals(value, "Alice-v2", StringComparison.Ordinal))
        {
            throw new InvalidOperationException($"Unexpected update result for {Repo.Dialect.DisplayName}: {value ?? "<null>"}.");
        }

        return value;
    }

}

/// <summary>
/// EN: Deletes one user row and validates the remaining row projection.
/// PT-br: Exclui uma linha de usuario e valida a projeção da linha restante.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationDeleteByPkServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Deletes one user row and validates the remaining row projection.
    /// PT-br: Exclui uma linha de usuario e valida a projeção da linha restante.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the delete flow. PT-br: Id principal opcional do usuario para o fluxo de delete.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, userId));
        var lst = await Repo.ExecuteReaderAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
""");
        if (lst.Count != 1
            || lst[0].Count != 1)
        {
            throw new InvalidOperationException($"Unexpected delete count for {Repo.Dialect.DisplayName}: {JsonSerializer.Serialize(lst)}.");
        }

        return lst;
    }

}

/// <summary>
/// EN: Executes the join query between users and orders and validates the count.
/// PT-br: Executa a consulta de junção entre usuarios e pedidos e valida a contagem.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationSelectJoinServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Executes the join query between users and orders and validates the count.
    /// PT-br: Executa a consulta de junção entre usuarios e pedidos e valida a contagem.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the join query. PT-br: Id principal opcional do usuario para a consulta de join.</param>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSelectJoinCountAsync(args);

    /// <summary>
    /// EN: Executes the join query between users and orders and validates the count.
    /// PT-br: Executa a consulta de junção entre usuarios e pedidos e valida a contagem.
    /// </summary>
    /// <param name="args">EN: Optional primary user id for the join query. PT-br: Id principal opcional do usuario para a consulta de join.</param>
    public async Task<int> RunSelectJoinCountAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;
        var value = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountJoinForUser(Context, userId)), CultureInfo.InvariantCulture);
        if (value != 2)
        {
            throw new InvalidOperationException($"Unexpected join count for {Repo.Dialect.DisplayName}: {value}.");
        }

        return value;
    }

}

/// <summary>
/// EN: Updates a row and validates the affected-row count reported by the provider.
/// PT-br: Atualiza uma linha e valida a contagem de linhas afetadas informada pelo provedor.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationRowCountAfterUpdateServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Updates a row and validates the affected-row count reported by the provider.
    /// PT-br: Atualiza uma linha e valida a contagem de linhas afetadas informada pelo provedor.
    /// </summary>
    /// <param name="args">EN: Optional user id for the update flow. PT-br: Id opcional do usuario para o fluxo de update.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var userId = args.Length > 0 ? (int)args[0] : 1;
        var affected = await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, userId, "Alice-v2"));
        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected update rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }

}

/// <summary>
/// EN: Updates one user row, deletes another row, and validates the final row count and remaining value.
/// PT-br: Atualiza uma linha de usuario, exclui outra linha e valida a contagem final e o valor restante.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationUpdateDeleteRoundTripServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Updates one user row, deletes another row, and validates the final row count and remaining value.
    /// PT-br: Atualiza uma linha de usuario, exclui outra linha e valida a contagem final e o valor restante.
    /// </summary>
    /// <param name="args">EN: Optional update and delete user ids. PT-br: Ids opcionais de usuario para update e delete.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var updateUserId = args.Length > 0 ? (int)args[0] : 1;
        var deleteUserId = args.Length > 1 ? (int)args[1] : 2;

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, updateUserId, "Alice-v2"));
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, deleteUserId));

        var remaining = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, updateUserId)), CultureInfo.InvariantCulture);
        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected update/delete count for {Repo.Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(remaining);

        return count;
    }
}

/// <summary>
/// EN: Updates one row and deletes another inside a transaction, then validates the committed result.
/// PT-br: Atualiza uma linha e exclui outra dentro de uma transacao, depois valida o resultado confirmado.
/// </summary>
/// <param name="repo"></param>
/// <param name="context"></param>
public class DmlMutationTransactionalUpdateDeleteCommitServiceTest(
    RepoService repo,
    FidelityTestContext context
) : DmlMutationCrudServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Updates one row and deletes another inside a transaction, then validates the committed result.
    /// PT-br: Atualiza uma linha e exclui outra dentro de uma transacao, depois valida o resultado confirmado.
    /// </summary>
    /// <param name="args">EN: Optional update and delete user ids. PT-br: Ids opcionais de usuario para update e delete.</param>
    public async Task<object?> RunTestAsync(params object[] args)
    {
        var updateUserId = args.Length > 0 ? (int)args[0] : 1;
        var deleteUserId = args.Length > 1 ? (int)args[1] : 2;

        using var transaction = Repo.BeginTransaction();
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.UpdateUserNameById(Context, updateUserId, "Alice-v2"), transaction);
        await Repo.ExecuteNonQueryAsync(Repo.Dialect.DeleteUserById(Context, deleteUserId), transaction);
        transaction.Commit();

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.SelectUserNameById(Context, updateUserId)), CultureInfo.InvariantCulture);
        var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
        if (count != 1)
        {
            throw new InvalidOperationException($"Unexpected transactional update/delete count for {Repo.Dialect.DisplayName}: {count}.");
        }

        GC.KeepAlive(value);

        return count;
    }
}
