using System.Globalization;
using DbSqlLikeMem.TestTools.Benchmarks;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the DbSqlLikeMem benchmark session implementation shared across providers.
/// PT: Fornece a implementacao de sessao de benchmark do DbSqlLikeMem compartilhada entre providers.
/// </summary>
public abstract class DbSqlLikeMemBenchmarkSessionBase(ProviderSqlDialect dialect)
    : BenchmarkSessionBase(dialect, BenchmarkEngine.DbSqlLikeMem)
{
    /// <summary>
    /// EN: Creates a temporary users table from the shared source scenario and validates the projected rows within the same session.
    /// PT-br: Cria uma tabela temporaria de usuarios a partir do cenario compartilhado e valida as linhas projetadas na mesma sessao.
    /// </summary>
    protected override void RunTempTableCreateAndUse()
    {
        var uId = NextToken();
        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryTableScenario<DbConnection>(Dialect));
        try
        {
            service.CreateScenario(users, uId);
            var rows = service.RunCreateTemporaryTableAsSelectThenSelect(users, uId);
            GC.KeepAlive(rows);
        }
        finally
        {
            try
            {
                service.DropScenario(users, uId);
            }
            catch
            {
                SafeDropTable(connection, users, uId);
            }
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

        var users = NewUsersTableName();
        using var connection = CreateConnection();
        connection.Open();
        var service = CreateTemporaryTableService(connection, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect));
        service.CreateScenario(users);

        try
        {
            using var tx = connection.BeginTransaction();
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 1, "Alice"), tx);
            ExecuteNonQuery(connection, Dialect.Savepoint(NewSavepointName()), tx);
            ExecuteNonQuery(connection, Dialect.InsertUser(users, 2, "Bob"), tx);
            tx.Rollback();

            var count = Convert.ToInt32(
                ExecuteScalar(connection, Dialect.CountRows(users)),
                CultureInfo.InvariantCulture);

            if (count != 0)
            {
                throw new InvalidOperationException($"Expected rollback to clear temp-table rows for {Dialect.DisplayName}, got {count}.");
            }

            GC.KeepAlive(count);
        }
        finally
        {
            service.DropScenario(users);
        }
    }

    /// <summary>
    /// EN: Verifies connection-scoped temporary tables are not visible from a different logical connection.
    /// PT-br: Verifica se tabelas temporarias de escopo da conexao nao ficam visiveis em outra conexao logica.
    /// </summary>
    protected override void RunTempTableCrossConnectionIsolation()
    {
        var users = NewUsersTableName();
        using var connection1 = CreateConnection();
        connection1.Open();
        var service = CreateTemporaryTableService(connection1, BenchmarkScenarioFactory.CreateTemporaryUsersScenario<DbConnection>(Dialect), CreateConnection);
        service.CreateScenario(users);

        try
        {
            var value = service.RunTemporaryTableCrossConnectionIsolation(users);
            GC.KeepAlive(value);
        }
        finally
        {
            try
            {
                service.DropScenario(users);
            }
            catch
            {
                SafeDropTemporaryTable(connection1, users);
            }
        }
    }
}
