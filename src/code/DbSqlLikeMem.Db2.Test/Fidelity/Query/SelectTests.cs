using DbSqlLikeMem.Db2.TestTools;
using DbSqlLikeMem.TestTools.Tests.Query;
#if NET462
using DB2Connection = IBM.Data.DB2.Core.DB2Connection;
#endif

namespace DbSqlLikeMem.Db2.Test.Fidelity.Query;

/// <summary>
/// EN: Runs Db2 fidelity tests for the shared primary-key select scenario.
/// PT: Executa testes de fidelidade do Db2 para o cenario compartilhado de selecao por chave primaria.
/// </summary>
[FidelityNativeClientSkip]
public class SelectTests(
    ITestOutputHelper helper
    ) : SelectTestsBase<Db2ConnectionMock, DB2Connection>(
    helper,
    new Db2ProviderSqlDialect(),
    () => new Db2ConnectionMock(Get(Db2DbVersions.Default, _ => new Db2DbMock(_) { ThreadSafe = true })),
    Db2ConnectionFactory.Create
    )
{
    /// <summary>
    /// EN: Gets the column names to be used in the projection of the select query.
    /// PT: Obtém os nomes das colunas a serem usadas na projeção da consulta de seleção.
    /// </summary>
    /// <returns></returns>
    protected override string[] ApplyProjectionColumnNames()
    => ["USERID", "USERNAME", "NOTE"];


    /// <summary>
    /// EN: Normalizes the column names in the snapshot to match the expected format for Db2.
    /// PT: Normaliza os nomes das colunas no snapshot para corresponder ao formato esperado para o Db2.
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
}

