namespace DbSqlLikeMem.TestTools.DML;

/// <summary>
/// EN: Inserts multiple user rows in one statement and verifies defaults and nullable columns.
/// PT-br: Insere varias linhas de usuario em uma instrucao e verifica defaults e colunas anulaveis.
/// </summary>
/// <param name="repo">EN: Repository used to execute SQL commands. PT-br: Repositorio usado para executar comandos SQL.</param>
/// <param name="context">EN: Scenario context with the current table names. PT-br: Contexto do cenario com os nomes atuais das tabelas.</param>
public class BatchInsertDefaultsUsersServiceTest(
        RepoService repo,
        FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Inserts a multi-row users batch and validates row count, defaults, and implicit null columns.
    /// PT-br: Insere um lote multi-linha de usuarios e valida contagem, defaults e colunas null implicitas.
    /// </summary>
    /// <param name="args">EN: Optional batch size. PT-br: Tamanho opcional do lote.</param>
    /// <returns>EN: The number of persisted rows. PT-br: A quantidade de linhas persistidas.</returns>
    public virtual async Task<object?> RunTestAsync(params object[] args)
    {
        var rowCount = args.Length > 0 ? (int)args[0] : 3;
        var values = new (int id, string name)[rowCount];
        for (var i = 0; i < rowCount; i++)
        {
            var id = i + 1;
            values[i] = (id, $"User-{id}");
        }

        await Repo.ExecuteNonQueryAsync(Repo.Dialect.InsertUsers(Context, values));

        var reader = await Repo.ExecuteReaderAsync($"""
SELECT
    Id,
    Name,
    Email,
    IsActive,
    Balance
FROM {Context.TbUsersFullName}
ORDER BY Id
""");

        reader.Should().NotBeEmpty();

        var rows = reader[0];
        rows.Should().HaveCount(rowCount);

        for (var i = 0; i < rowCount; i++)
        {
            var row = rows[i];
            var id = i + 1;
            Convert.ToInt32(row[0], CultureInfo.InvariantCulture).Should().Be(id);
            Convert.ToString(row[1], CultureInfo.InvariantCulture).Should().Be($"User-{id}");
            (row[2] is null or DBNull).Should().BeTrue();
            Convert.ToBoolean(row[3], CultureInfo.InvariantCulture).Should().BeTrue();
            Convert.ToDecimal(row[4], CultureInfo.InvariantCulture).Should().Be(0m);
        }

        return rows.Count;
    }
}
