namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the DbSqlLikeMem benchmark session implementation shared across providers.
/// PT: Fornece a implementacao de sessao de benchmark do DbSqlLikeMem compartilhada entre providers.
/// </summary>
internal abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <summary>
    /// EN: Creates a temporary users table from the shared source scenario and validates the projected rows within the same session.
    /// PT-br: Cria uma tabela temporaria de usuarios a partir do cenario compartilhado e valida as linhas projetadas na mesma sessao.
    /// </summary>
    protected override void RunTempTableCreateAndUse()
    {
        using var scope = CreateTemporaryTableScope();
        try
        {
            var rows = scope.Service.RunCreateTemporaryTableAsSelectThenSelectAsync(
                scope.Context).GetAwaiter().GetResult();
            GC.KeepAlive(rows);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>
    /// EN: Verifies a transaction rollback restores the connection-scoped temporary table contents to the pre-transaction state.
    /// PT-br: Verifica se um rollback de transacao restaura o conteudo da tabela temporaria de escopo da conexao ao estado anterior.
    /// </summary>
    protected override void RunTempTableRollback()
    {
        if (!Dialect.SupportsSavepoints)
        {
            throw new NotSupportedException($"{Dialect.DisplayName} does not support temp-table rollback benchmark.");
        }

        using var scope = CreateTemporaryUsersScope();

        try
        {
            scope.Service.RunTempTableRollback().GetAwaiter().GetResult();
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>
    /// EN: Verifies connection-scoped temporary tables are not visible from a different logical connection.
    /// PT-br: Verifica se tabelas temporarias de escopo da conexao nao ficam visiveis em outra conexao logica.
    /// </summary>
    protected override void RunTempTableCrossConnectionIsolation()
    {
        using var scope = CreateTemporaryUsersScope();

        try
        {
            var value = scope.Service.RunTemporaryTableCrossConnectionIsolation().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            scope.Dispose();
        }
    }

    /// <summary>
    /// EN: Executes the stored procedure benchmark against the in-memory DbSqlLikeMem mock runtime.
    /// PT: Executa o benchmark de procedimento armazenado contra o runtime mock em memoria DbSqlLikeMem.
    /// </summary>
    protected override void RunStoredProcedureCall()
    {
        var count = RunPreparedStoredProcedureCall("StoredProcedureCall", 10, "benchmark");
        GC.KeepAlive(count);
    }
}
