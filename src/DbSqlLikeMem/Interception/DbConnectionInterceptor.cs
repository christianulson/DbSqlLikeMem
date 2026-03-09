namespace DbSqlLikeMem;

/// <summary>
/// EN: Receives lifecycle and execution callbacks for connections wrapped by the interception pipeline.
/// PT: Recebe callbacks de ciclo de vida e execucao para conexoes encapsuladas pelo pipeline de interceptacao.
/// </summary>
public abstract class DbConnectionInterceptor
{
    /// <summary>
    /// EN: Runs before the wrapped connection opens.
    /// PT: Executa antes de a conexao encapsulada abrir.
    /// </summary>
    /// <param name="connection">EN: Wrapped connection. PT: Conexao encapsulada.</param>
    public virtual void ConnectionOpening(DbConnection connection)
    {
    }

    /// <summary>
    /// EN: Runs after the wrapped connection opens successfully.
    /// PT: Executa apos a conexao encapsulada abrir com sucesso.
    /// </summary>
    /// <param name="connection">EN: Wrapped connection. PT: Conexao encapsulada.</param>
    public virtual void ConnectionOpened(DbConnection connection)
    {
    }

    /// <summary>
    /// EN: Runs before the wrapped connection closes.
    /// PT: Executa antes de a conexao encapsulada fechar.
    /// </summary>
    /// <param name="connection">EN: Wrapped connection. PT: Conexao encapsulada.</param>
    public virtual void ConnectionClosing(DbConnection connection)
    {
    }

    /// <summary>
    /// EN: Runs after the wrapped connection closes successfully.
    /// PT: Executa apos a conexao encapsulada fechar com sucesso.
    /// </summary>
    /// <param name="connection">EN: Wrapped connection. PT: Conexao encapsulada.</param>
    public virtual void ConnectionClosed(DbConnection connection)
    {
    }

    /// <summary>
    /// EN: Runs after a wrapped command is created for the intercepted connection.
    /// PT: Executa apos um comando encapsulado ser criado para a conexao interceptada.
    /// </summary>
    /// <param name="connection">EN: Wrapped connection. PT: Conexao encapsulada.</param>
    /// <param name="command">EN: Wrapped command. PT: Comando encapsulado.</param>
    public virtual void CommandCreated(DbConnection connection, DbCommand command)
    {
    }

    /// <summary>
    /// EN: Runs before a wrapped command executes.
    /// PT: Executa antes de um comando encapsulado ser executado.
    /// </summary>
    /// <param name="context">EN: Interception context. PT: Contexto da interceptacao.</param>
    public virtual void CommandExecuting(DbCommandExecutionContext context)
    {
    }

    /// <summary>
    /// EN: Runs after a wrapped command executes successfully.
    /// PT: Executa apos um comando encapsulado ser executado com sucesso.
    /// </summary>
    /// <param name="context">EN: Interception context. PT: Contexto da interceptacao.</param>
    /// <param name="result">EN: Command result. PT: Resultado do comando.</param>
    public virtual void CommandExecuted(DbCommandExecutionContext context, object? result)
    {
    }

    /// <summary>
    /// EN: Runs when a wrapped command execution fails.
    /// PT: Executa quando a execucao de um comando encapsulado falha.
    /// </summary>
    /// <param name="context">EN: Interception context. PT: Contexto da interceptacao.</param>
    /// <param name="exception">EN: Exception raised by the inner command. PT: Excecao gerada pelo comando interno.</param>
    public virtual void CommandFailed(DbCommandExecutionContext context, Exception exception)
    {
    }

    /// <summary>
    /// EN: Runs before a wrapped connection starts a transaction.
    /// PT: Executa antes de uma conexao encapsulada iniciar uma transacao.
    /// </summary>
    /// <param name="context">EN: Transaction starting context. PT: Contexto de inicio da transacao.</param>
    public virtual void TransactionStarting(DbTransactionStartingContext context)
    {
    }

    /// <summary>
    /// EN: Runs after a wrapped connection starts a transaction successfully.
    /// PT: Executa apos uma conexao encapsulada iniciar uma transacao com sucesso.
    /// </summary>
    /// <param name="context">EN: Transaction context. PT: Contexto da transacao.</param>
    public virtual void TransactionStarted(DbTransactionInterceptionContext context)
    {
    }

    /// <summary>
    /// EN: Runs before a wrapped transaction executes commit or rollback.
    /// PT: Executa antes de uma transacao encapsulada executar commit ou rollback.
    /// </summary>
    /// <param name="context">EN: Transaction context. PT: Contexto da transacao.</param>
    public virtual void TransactionExecuting(DbTransactionInterceptionContext context)
    {
    }

    /// <summary>
    /// EN: Runs after a wrapped transaction executes commit or rollback successfully.
    /// PT: Executa apos uma transacao encapsulada executar commit ou rollback com sucesso.
    /// </summary>
    /// <param name="context">EN: Transaction context. PT: Contexto da transacao.</param>
    public virtual void TransactionExecuted(DbTransactionInterceptionContext context)
    {
    }

    /// <summary>
    /// EN: Runs when a wrapped transaction operation fails.
    /// PT: Executa quando uma operacao de transacao encapsulada falha.
    /// </summary>
    /// <param name="context">EN: Transaction context. PT: Contexto da transacao.</param>
    /// <param name="exception">EN: Exception raised by the inner transaction. PT: Excecao gerada pela transacao interna.</param>
    public virtual void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
    {
    }
}
