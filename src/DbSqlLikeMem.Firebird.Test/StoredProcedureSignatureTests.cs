namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Verifies stored procedure signature behavior for Firebird.
/// PT: Verifica o comportamento de assinatura de procedures para Firebird.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class StoredProcedureSignatureTests(
        ITestOutputHelper helper
    ) : StoredProcedureSignatureTestsBase<FirebirdMockException>(helper)
{
    /// <summary>
    /// EN: Creates the Firebird connection mock used in tests.
    /// PT: Cria o simulado de conexão Firebird usado nos testes.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new FirebirdConnectionMock();

    /// <inheritdoc />
    protected override bool SupportsDateTimeOffsetInputOutputParameters => false;
}
