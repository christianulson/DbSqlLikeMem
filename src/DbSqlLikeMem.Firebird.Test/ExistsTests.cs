namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Validates EXISTS and NOT EXISTS semantics for Firebird.
/// PT: Valida a semântica de EXISTS e NOT EXISTS para Firebird.
/// </summary>
/// <param name="helper">
/// EN: xUnit output helper used by the shared base test class.
/// PT: Helper de saída do xUnit usado pela classe base de testes compartilhada.
/// </param>
public sealed class ExistsTests(
        ITestOutputHelper helper
    ) : ExistsTestsBase(helper)
{
    /// <summary>
    /// EN: Creates the Firebird mock connection used in the shared EXISTS tests.
    /// PT: Cria a conexao mock Firebird usada nos testes compartilhados de EXISTS.
    /// </summary>
    protected override DbConnectionMockBase CreateConnection() => new FirebirdConnectionMock();
}
