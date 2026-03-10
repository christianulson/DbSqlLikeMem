#if NET5_0_OR_GREATER
using System.Diagnostics;
using System.Runtime.CompilerServices;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Publishes interception spans through <see cref="ActivitySource"/>.
/// PT: Publica spans de interceptacao por meio de <see cref="ActivitySource"/>.
/// </summary>
public sealed class ActivitySourceDbConnectionInterceptor : DbConnectionInterceptor
{
    private readonly ActivitySource _activitySource;
    private readonly ConditionalWeakTable<object, Activity> _activities = new();

    /// <summary>
    /// EN: Creates an interceptor that writes to the default interception activity source.
    /// PT: Cria um interceptor que escreve no activity source padrao da interceptacao.
    /// </summary>
    public ActivitySourceDbConnectionInterceptor()
        : this(new ActivitySource(DbInterceptionActivityNames.SourceName))
    {
    }

    /// <summary>
    /// EN: Creates an interceptor that writes to the supplied activity source.
    /// PT: Cria um interceptor que escreve no activity source informado.
    /// </summary>
    /// <param name="activitySource">EN: Activity source used to publish spans. PT: Activity source usado para publicar spans.</param>
    public ActivitySourceDbConnectionInterceptor(ActivitySource activitySource)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(activitySource, nameof(activitySource));
        _activitySource = activitySource;
    }

    /// <summary>
    /// EN: Gets the underlying activity source.
    /// PT: Obtem o activity source subjacente.
    /// </summary>
    public ActivitySource ActivitySource => _activitySource;

    /// <inheritdoc />
    public override void ConnectionOpening(DbConnection connection)
        => Start(connection, DbInterceptionActivityNames.ConnectionOpen, activity =>
        {
            activity.SetTag("db.connection.state", connection.State.ToString());
        });

    /// <inheritdoc />
    public override void ConnectionOpened(DbConnection connection)
        => Stop(connection, activity =>
        {
            activity.SetTag("db.connection.state", connection.State.ToString());
        });

    /// <inheritdoc />
    public override void ConnectionClosing(DbConnection connection)
        => Start(connection, DbInterceptionActivityNames.ConnectionClose, activity =>
        {
            activity.SetTag("db.connection.state", connection.State.ToString());
        });

    /// <inheritdoc />
    public override void ConnectionClosed(DbConnection connection)
        => Stop(connection, activity =>
        {
            activity.SetTag("db.connection.state", connection.State.ToString());
        });

    /// <inheritdoc />
    public override void CommandExecuting(DbCommandExecutionContext context)
        => Start(context, DbInterceptionActivityNames.Command, activity =>
        {
            activity.SetTag("db.operation", context.ExecutionKind.ToString());
            activity.SetTag("db.statement", context.Command.CommandText);
        });

    /// <inheritdoc />
    public override void CommandExecuted(DbCommandExecutionContext context, object? result)
        => Stop(context, activity =>
        {
            activity.SetTag("db.operation", context.ExecutionKind.ToString());
            activity.SetTag("db.statement", context.Command.CommandText);
            if (result is not null)
                activity.SetTag("db.result", result.ToString());
        });

    /// <inheritdoc />
    public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
        => Stop(context, activity =>
        {
            activity.SetTag("db.operation", context.ExecutionKind.ToString());
            activity.SetTag("db.statement", context.Command.CommandText);
            activity.SetTag("otel.status_code", "ERROR");
            activity.SetTag("otel.status_description", exception.Message);
        }, exception);

    /// <inheritdoc />
    public override void TransactionStarting(DbTransactionStartingContext context)
        => Start(GetTransactionBeginKey(context.Connection), DbInterceptionActivityNames.TransactionBegin, activity =>
        {
            activity.SetTag("db.transaction.operation", DbTransactionOperationKind.Begin.ToString());
            activity.SetTag("db.transaction.isolation_level", context.IsolationLevel.ToString());
        });

    /// <inheritdoc />
    public override void TransactionStarted(DbTransactionInterceptionContext context)
        => Stop(GetTransactionBeginKey(context.Connection), activity =>
        {
            activity.SetTag("db.transaction.operation", context.OperationKind.ToString());
            activity.SetTag("db.transaction.isolation_level", context.Transaction.IsolationLevel.ToString());
        });

    /// <inheritdoc />
    public override void TransactionExecuting(DbTransactionInterceptionContext context)
        => Start(context, DbInterceptionActivityNames.TransactionOperation, activity =>
        {
            activity.SetTag("db.transaction.operation", context.OperationKind.ToString());
            activity.SetTag("db.transaction.isolation_level", context.Transaction.IsolationLevel.ToString());
        });

    /// <inheritdoc />
    public override void TransactionExecuted(DbTransactionInterceptionContext context)
        => Stop(context, activity =>
        {
            activity.SetTag("db.transaction.operation", context.OperationKind.ToString());
            activity.SetTag("db.transaction.isolation_level", context.Transaction.IsolationLevel.ToString());
        });

    /// <inheritdoc />
    public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
        => Stop(context, activity =>
        {
            activity.SetTag("db.transaction.operation", context.OperationKind.ToString());
            activity.SetTag("db.transaction.isolation_level", context.Transaction.IsolationLevel.ToString());
            activity.SetTag("otel.status_code", "ERROR");
            activity.SetTag("otel.status_description", exception.Message);
        }, exception);

    private void Start(object key, string activityName, Action<Activity> configure)
    {
        var activity = _activitySource.StartActivity(activityName, ActivityKind.Internal);
        if (activity is null)
            return;

        configure(activity);
        _activities.Remove(key);
        _activities.Add(key, activity);
    }

    private void Stop(object key, Action<Activity> configure, Exception? exception = null)
    {
        if (!_activities.TryGetValue(key, out var activity))
            return;

        configure(activity);
        if (exception is not null)
        {
            activity.SetTag("exception.type", exception.GetType().FullName);
            activity.SetTag("exception.message", exception.Message);
        }

        activity.Stop();
        _activities.Remove(key);
    }

    private static object GetTransactionBeginKey(DbConnection connection)
        => connection;
}
#else
namespace DbSqlLikeMem;

/// <summary>
/// EN: No-op activity interceptor used on target frameworks without <c>ActivitySource</c>.
/// PT: Interceptor de activity sem operacao usado em target frameworks sem <c>ActivitySource</c>.
/// </summary>
public sealed class ActivitySourceDbConnectionInterceptor : DbConnectionInterceptor
{
}
#endif
