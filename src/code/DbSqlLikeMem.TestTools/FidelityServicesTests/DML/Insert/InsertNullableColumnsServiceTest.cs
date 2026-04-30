namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Inserts a row that omits nullable columns and verifies the defaulted and null values.
/// PT: Insere uma linha que omite colunas anulaveis e verifica os valores padrao e nulos.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT: Contexto do cenario com os nomes atuais das tabelas.</param>
public class InsertNullableColumnsServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row while omitting nullable columns, then verifies the nullable column with a default and the nullable column without a default.
    /// PT: Insere uma linha omitindo colunas anulaveis e depois verifica a coluna anulavel com default e a coluna anulavel sem default.
    /// </summary>
    /// <param name="args">EN: Optional row id and required column value. PT: Id da linha e valor da coluna obrigatoria opcionais.</param>
    /// <returns>EN: The persisted column values. PT: Os valores persistidos das colunas.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 1;
        var requiredNoDefault = args.Length > 1 ? (int)args[1] : 10;

        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    RequiredNoDefault
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("requiredNoDefault")}
)
""", addParameters: command =>
        {
            AddParameter(command, "id", DbType.Int32, id);
            AddParameter(command, "requiredNoDefault", DbType.Int32, requiredNoDefault);
        });

        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected nullable-column insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        var reader = await Repo.ExecuteReaderAsync($"""
SELECT
    RequiredNoDefault,
    NullableWithDefault,
    NullableNoDefault
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""", addParameters: command =>
            AddParameter(command, "id", DbType.Int32, id));

        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        var persistedRequiredNoDefault = Convert.ToInt32(row[0], CultureInfo.InvariantCulture);
        var persistedNullableWithDefault = Convert.ToInt32(row[1], CultureInfo.InvariantCulture);
        var persistedNullableNoDefault = row[2] is null or DBNull
            ? (int?)null
            : Convert.ToInt32(row[2], CultureInfo.InvariantCulture);

        persistedRequiredNoDefault.Should().Be(requiredNoDefault);
        persistedNullableWithDefault.Should().Be(7);
        persistedNullableNoDefault.Should().BeNull();

        GC.KeepAlive(id);
        GC.KeepAlive(requiredNoDefault);
        return new
        {
            requiredNoDefault = persistedRequiredNoDefault,
            nullableWithDefault = persistedNullableWithDefault,
            nullableNoDefault = persistedNullableNoDefault
        };
    }
}
