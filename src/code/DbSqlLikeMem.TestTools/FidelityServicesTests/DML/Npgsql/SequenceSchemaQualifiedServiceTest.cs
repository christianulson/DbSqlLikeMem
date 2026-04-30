namespace DbSqlLikeMem.Npgsql.Test.Fidelity.DML;

/// <summary>
/// EN: Creates and queries a schema-qualified sequence for PostgreSQL fidelity.
/// PT: Cria e consulta uma sequence qualificada por schema para fidelidade do PostgreSQL.
/// </summary>
public sealed class SequenceSchemaQualifiedServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context),
        IBaseServiceTest
{
    /// <summary>
    /// EN: Returns the values produced by schema-qualified sequence access during an insert round-trip.
    /// PT: Retorna os valores produzidos pelo acesso a sequence qualificada por schema durante um round-trip de insert.
    /// </summary>
    public async Task<object?> RunTestAsync(params object[] args)
        => await RunSequenceSchemaQualifiedAsync();

    /// <summary>
    /// EN: Returns the values produced by schema-qualified sequence access during an insert round-trip.
    /// PT: Retorna os valores produzidos pelo acesso a sequence qualificada por schema durante um round-trip de insert.
    /// </summary>
    public async Task<long[]> RunSequenceSchemaQualifiedAsync()
    {
        var schema = $"sales_{Context.UId}";
        var sequence = $"seq_orders_{Context.UId}";

        await ExecuteNonQueryAsync($"CREATE SCHEMA {schema}");
        await ExecuteNonQueryAsync($"CREATE SEQUENCE {schema}.{sequence} START WITH 7 INCREMENT BY 4");
        await ExecuteNonQueryAsync($"CREATE TABLE {schema}.orders (id bigint not null)");

        var first = await ExecuteScalarLongAsync($"SELECT nextval('{schema}.{sequence}')");
        var inserted = await ExecuteScalarLongAsync($"INSERT INTO {schema}.orders (id) VALUES (nextval('{schema}.{sequence}')) RETURNING id");
        var current = await ExecuteScalarLongAsync($"SELECT currval('{schema}.{sequence}')");
        return new[] { first, inserted, current };
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
