namespace DbSqlLikeMem.TestTools.DML;

internal abstract class CheckConstraintsServiceTestBase(
    RepoService repo,
    FidelityTestContext context
) : BaseServiceTest(repo, context)
{
    protected async Task<int> InsertCheckConstraintRowAsync(
        int id,
        int requiredNoDefault,
        int checkedRequired)
    {
        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    RequiredNoDefault,
    CheckedRequired
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("requiredNoDefault")},
    {Repo.Dialect.Parameter("checkedRequired")}
)
""", addParameters: command =>
        {
            AddParameter(command, "id", DbType.Int32, id);
            AddParameter(command, "requiredNoDefault", DbType.Int32, requiredNoDefault);
            AddParameter(command, "checkedRequired", DbType.Int32, checkedRequired);
        });

        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected check-constraint insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        return affected;
    }

    protected async Task<(int RequiredNoDefault, int NullableWithDefault, int? NullableNoDefault, int CheckedRequired, int? CheckedNullable)> ReadCheckConstraintRowAsync(
        int id)
    {
        var reader = await Repo.ExecuteReaderAsync($"""
SELECT
    RequiredNoDefault,
    NullableWithDefault,
    NullableNoDefault,
    CheckedRequired,
    CheckedNullable
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""", addParameters: command =>
            AddParameter(command, "id", DbType.Int32, id));

        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        row.Length.Should().Be(5);

        return (
            Convert.ToInt32(row[0], CultureInfo.InvariantCulture),
            Convert.ToInt32(row[1], CultureInfo.InvariantCulture),
            NormalizeNullableInt(row[2]),
            Convert.ToInt32(row[3], CultureInfo.InvariantCulture),
            NormalizeNullableInt(row[4]));
    }

    protected static int? NormalizeNullableInt(object? value)
        => value is null or DBNull
            ? null
            : Convert.ToInt32(value, CultureInfo.InvariantCulture);
}

/// <summary>
/// EN: Inserts rows that satisfy the check constraints and validates the persisted defaults and nullable values.
/// PT-br: Insere linhas que satisfazem as restricoes check e valida os defaults persistidos e os valores anulaveis.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT-br: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT-br: Contexto do cenario com os nomes atuais das tabelas.</param>
internal class CheckConstraintsValidInsertServiceTest(
    RepoService repo,
    FidelityTestContext context
) : CheckConstraintsServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row that satisfies the check constraints and returns the persisted projection.
    /// PT-br: Insere uma linha que satisfaz as restricoes check e retorna a projecao persistida.
    /// </summary>
    /// <param name="args">EN: Optional row id, required column value, and check column value. PT-br: Id da linha, valor da coluna obrigatoria e valor da coluna de check opcionais.</param>
    /// <returns>EN: The persisted row projection. PT-br: A projecao da linha persistida.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 1;
        var requiredNoDefault = args.Length > 1 ? (int)args[1] : 10;
        var checkedRequired = args.Length > 2 ? (int)args[2] : 5;

        var affected = await InsertCheckConstraintRowAsync(id, requiredNoDefault, checkedRequired);
        var values = await ReadCheckConstraintRowAsync(id);

        GC.KeepAlive(id);
        GC.KeepAlive(requiredNoDefault);
        GC.KeepAlive(checkedRequired);
        return new
        {
            affected,
            requiredNoDefault = values.RequiredNoDefault,
            nullableWithDefault = values.NullableWithDefault,
            nullableNoDefault = values.NullableNoDefault,
            checkedRequired = values.CheckedRequired,
            checkedNullable = values.CheckedNullable
        };
    }
}

/// <summary>
/// EN: Attempts to insert a row that violates a check constraint and returns true when the provider rejects it.
/// PT-br: Tenta inserir uma linha que viola uma restricao check e retorna true quando o provedor a rejeita.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT-br: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT-br: Contexto do cenario com os nomes atuais das tabelas.</param>
internal class CheckConstraintsInvalidInsertServiceTest(
    RepoService repo,
    FidelityTestContext context
) : CheckConstraintsServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row that violates a check constraint and returns true when the provider rejects it.
    /// PT-br: Insere uma linha que viola uma restricao check e retorna true quando o provedor a rejeita.
    /// </summary>
    /// <param name="args">EN: Optional row id and required column value. PT-br: Id da linha e valor da coluna obrigatoria opcionais.</param>
    /// <returns>EN: True when the insert fails as expected. PT-br: True quando o insert falha como esperado.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 2;
        var requiredNoDefault = args.Length > 1 ? (int)args[1] : 10;

        try
        {
            await InsertCheckConstraintRowAsync(id, requiredNoDefault, 0);
        }
        catch
        {
            var count = Convert.ToInt32(await Repo.ExecuteScalarAsync(Repo.Dialect.CountRows(Context.TbUsersFullName)), CultureInfo.InvariantCulture);
            if (count != 0)
            {
                throw new InvalidOperationException($"Unexpected check-constraint insert residue for {Repo.Dialect.DisplayName}: {count}.");
            }

            GC.KeepAlive(id);
            GC.KeepAlive(requiredNoDefault);
            return true;
        }

        throw new InvalidOperationException($"Expected {Repo.Dialect.DisplayName} to reject INSERT that violates CHECK constraints.");
    }
}

/// <summary>
/// EN: Updates a row into an invalid check state and returns the persisted row when the provider rejects the update.
/// PT-br: Atualiza uma linha para um estado invalido de check e retorna a linha persistida quando o provedor rejeita o update.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT-br: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT-br: Contexto do cenario com os nomes atuais das tabelas.</param>
internal class CheckConstraintsInvalidUpdateServiceTest(
    RepoService repo,
    FidelityTestContext context
) : CheckConstraintsServiceTestBase(repo, context),
    IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a valid row, attempts an invalid update, and returns the persisted values when the provider rejects the update.
    /// PT-br: Insere uma linha valida, tenta um update invalido e retorna os valores persistidos quando o provedor rejeita o update.
    /// </summary>
    /// <param name="args">EN: Optional row id, required column value, and check column value. PT-br: Id da linha, valor da coluna obrigatoria e valor da coluna de check opcionais.</param>
    /// <returns>EN: The persisted row projection after the rejected update. PT-br: A projecao da linha persistida apos o update rejeitado.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 3;
        var requiredNoDefault = args.Length > 1 ? (int)args[1] : 10;
        var checkedRequired = args.Length > 2 ? (int)args[2] : 5;

        await InsertCheckConstraintRowAsync(id, requiredNoDefault, checkedRequired);

        try
        {
            await Repo.ExecuteNonQueryAsync($"""
UPDATE {Context.TbUsersFullName}
SET CheckedNullable = {Repo.Dialect.Parameter("checkedNullable")}
WHERE Id = {Repo.Dialect.Parameter("id")}
""", addParameters: command =>
            {
                AddParameter(command, "checkedNullable", DbType.Int32, 0);
                AddParameter(command, "id", DbType.Int32, id);
            });
        }
        catch
        {
            var values = await ReadCheckConstraintRowAsync(id);

            GC.KeepAlive(id);
            GC.KeepAlive(requiredNoDefault);
            GC.KeepAlive(checkedRequired);
            return new
            {
                requiredNoDefault = values.RequiredNoDefault,
                nullableWithDefault = values.NullableWithDefault,
                nullableNoDefault = values.NullableNoDefault,
                checkedRequired = values.CheckedRequired,
                checkedNullable = values.CheckedNullable
            };
        }

        throw new InvalidOperationException($"Expected {Repo.Dialect.DisplayName} to reject UPDATE that violates CHECK constraints.");
    }
}
