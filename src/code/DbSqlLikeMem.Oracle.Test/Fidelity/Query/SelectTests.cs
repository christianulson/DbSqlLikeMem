using DbSqlLikeMem.Oracle.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;

namespace DbSqlLikeMem.Oracle.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Oracle fidelity tests for the shared primary-key select scenario.
/// PT-br: Executa testes de fidelidade do Oracle para o cenario compartilhado de selecao por chave primaria.
/// </summary>
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<OracleConnectionMock, OracleConnection>(
    helper,
    new OracleProviderSqlDialect(),
    () => new OracleConnectionMock(),
    s => new OracleConnection(s)
    )
{
    /// <summary>
    /// EN: Gets the column names to be used in the projection of the select query.
    /// PT-br: Obtém os nomes das colunas a serem usadas na projeção da consulta de seleção.
    /// </summary>
    protected override string[] ApplyProjectionColumnNames()
    => ["USERID", "USERNAME", "NOTE"];


    /// <summary>
    /// EN: Normalizes the column names in the snapshot to match the expected format for Oracle.
    /// PT-br: Normaliza os nomes das colunas no snapshot para corresponder ao formato esperado para o Oracle.
    /// </summary>
    protected override string[] NormalizeSnapshotColumnNames(string[] columnNames)
    {
        var normalized = new string[columnNames.Length];
        for (var i = 0; i < columnNames.Length; i++)
            normalized[i] = columnNames[i].ToUpperInvariant();

        return normalized;
    }

    /// <inheritdoc />
    protected override decimal TextMatchAlreadyValue => 0m;

    /// <summary>
    /// EN: Formats the decimal value as a fixed two-decimal invariant string for Oracle snapshot assertions.
    /// PT-br: Formata o valor decimal como uma string invariavel com duas casas decimais para as assercoes de snapshot do Oracle.
    /// </summary>
    /// <param name="value"></param>
    /// <returns></returns>
    protected override string AmountText(decimal value)
    => value.ToString("0.00", CultureInfo.InvariantCulture);
}
