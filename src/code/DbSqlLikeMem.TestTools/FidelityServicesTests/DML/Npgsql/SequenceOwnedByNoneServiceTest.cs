namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Detaches a PostgreSQL sequence from a table column and verifies it survives the table drop.
/// PT-br: Desvincula uma sequence PostgreSQL de uma coluna de tabela e verifica se ela sobrevive a queda da tabela.
/// </summary>
public sealed class SequenceOwnedByNoneServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the sequence values before and after the owning table is dropped.
    /// PT-br: Retorna os valores da sequence antes e depois de a tabela proprietaria ser removida.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceOwnedByNoneAsync();

    /// <summary>
    /// EN: Returns the sequence values before and after the owning table is dropped.
    /// PT-br: Retorna os valores da sequence antes e depois de a tabela proprietaria ser removida.
    /// </summary>
    public async Task<long[]> RunSequenceOwnedByNoneAsync()
    {
        await ExecuteNonQueryAsync($"CREATE TABLE {Context.TbUsersFullName} (Id BIGINT NOT NULL PRIMARY KEY)");
        await ExecuteNonQueryAsync($"CREATE SEQUENCE {Context.Seq} START WITH 1 INCREMENT BY 1");
        await ExecuteNonQueryAsync($"ALTER SEQUENCE {Context.Seq} OWNED BY {Context.TbUsersFullName}.Id");
        await ExecuteNonQueryAsync($"ALTER SEQUENCE {Context.Seq} OWNED BY NONE");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        await ExecuteNonQueryAsync($"DROP TABLE {Context.TbUsersFullName}");
        var second = await ExecuteScalarLongAsync($"SELECT nextval('{Context.Seq}')");
        return new[] { first, second };
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    private async Task<long> ExecuteScalarLongAsync(string sql)
    {
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        return Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }
}
