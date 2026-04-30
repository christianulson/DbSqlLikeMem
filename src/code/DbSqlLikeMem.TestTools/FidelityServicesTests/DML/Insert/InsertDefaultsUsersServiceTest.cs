namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Inserts a user row without the default-backed columns and verifies the provider fills them.
/// PT: Insere uma linha de usuario sem as colunas apoiadas por default e verifica se o provedor as preenche.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT: Contexto do cenario com os nomes atuais das tabelas.</param>
public class InsertDefaultsUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a row that omits NOT NULL columns with defaults and validates the persisted default values.
    /// PT: Insere uma linha que omite colunas NOT NULL com default e valida os valores padrao persistidos.
    /// </summary>
    /// <param name="args">EN: Optional user id and name for the insert. PT: Id e nome opcionais do usuario para o insert.</param>
    /// <returns>EN: The persisted row projection. PT: A projecao da linha persistida.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var id = args.Length > 0 ? (int)args[0] : 1;
        var name = args.Length > 1 ? (string)args[1] : "Alice";

        var affected = await Repo.ExecuteNonQueryAsync($"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")}
)
""", addParameters: command =>
        {
            AddParameter(command, "id", DbType.Int32, id);
            AddParameter(command, "name", DbType.String, name);
        });

        if (affected < 1)
        {
            throw new InvalidOperationException($"Unexpected default-column insert rowcount for {Repo.Dialect.DisplayName}: {affected}.");
        }

        var reader = await Repo.ExecuteReaderAsync($"""
SELECT
    Name,
    IsActive,
    Balance
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""", addParameters: command =>
            AddParameter(command, "id", DbType.Int32, id));

        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(1);

        var row = rows[0];
        NormalizeNullableText(row[0]).Should().Be(name);
        Convert.ToBoolean(row[1], CultureInfo.InvariantCulture).Should().BeTrue();
        Convert.ToDecimal(row[2], CultureInfo.InvariantCulture).Should().Be(0m);

        GC.KeepAlive(id);
        GC.KeepAlive(name);
        return new
        {
            affected,
            name = NormalizeNullableText(row[0]),
            isActive = Convert.ToBoolean(row[1], CultureInfo.InvariantCulture),
            balance = Convert.ToDecimal(row[2], CultureInfo.InvariantCulture)
        };
    }

    /// <summary>
    /// EN: Normalizes a nullable text value returned by the provider for comparisons in tests.
    /// PT: Normaliza um valor textual anulavel retornado pelo provedor para comparacoes em testes.
    /// </summary>
    /// <param name="value">EN: Database value to normalize. PT: Valor do banco a normalizar.</param>
    /// <returns>EN: The normalized string or null. PT: A string normalizada ou null.</returns>
    private static string? NormalizeNullableText(object? value)
        => value is null or DBNull
            ? null
            : Convert.ToString(value, CultureInfo.InvariantCulture);
}
