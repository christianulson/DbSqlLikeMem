using DbSqlLikeMem.Npgsql.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Npgsql.Test.Fidelity.Query;

/// <summary>
/// EN: Runs PostgreSQL fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade do PostgreSQL para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<NpgsqlConnectionMock, NpgsqlConnection>(
    helper,
    new NpgsqlProviderSqlDialect(),
    () => new NpgsqlConnectionMock(),
    s => new NpgsqlConnection(s)
    )
{
    /// <summary>
    /// EN: Gets the column names to be used in the projection of the select query.
    /// PT: Obtém os nomes das colunas a serem usadas na projeção da consulta de seleção.
    /// </summary>
    /// <returns></returns>
    protected override string[] ApplyProjectionColumnNames()
    => ["userid", "username", "note"];

    /// <summary>
    /// EN: Normalizes the column names in the snapshot to match the expected format for PostgreSQL.
    /// PT: Normaliza os nomes das colunas no snapshot para corresponder ao formato esperado para o PostgreSQL.
    /// </summary>
    /// <param name="columnNames"></param>
    /// <returns></returns>
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
    {
        var normalized = new string[columnNames.Length];
        for (var i = 0; i < columnNames.Length; i++)
        {
            normalized[i] = columnNames[i] switch
            {
                "Name" => "name",
                "Id" => "id",
                _ => columnNames[i]
            };
        }

        return normalized;
    }

    /// <inheritdoc />
    protected override DateTime NormalizeParameterDateTimeInput(DateTime value)
        => value.Kind == DateTimeKind.Unspecified
            ? DateTime.SpecifyKind(value, DateTimeKind.Utc)
            : value;

    /// <inheritdoc />
    protected override decimal TextMatchAlreadyValue => 0m;

}
