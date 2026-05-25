namespace DbSqlLikeMem.SqlServer.Test;

/// <summary>
/// EN: Runs shared stored procedure signature tests using the SQL Server mock connection.
/// PT-br: Executa os testes compartilhados de assinatura de procedure usando a conexão simulada de SQL Server.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT-br: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<SqlServerMockException>(helper)
{
    /// <inheritdoc />
    protected override DbSqlLikeMem.TestTools.ProviderSqlDialect Dialect { get; } = new DbSqlLikeMem.SqlServer.TestTools.SqlServerProviderSqlDialect();

    /// <summary>
    /// EN: Creates a SQL Server mock connection used by stored procedure signature tests.
    /// PT-br: Cria uma conexão simulada de SQL Server usada pelos testes de assinatura de procedure.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new SqlServerConnectionMock();
}
