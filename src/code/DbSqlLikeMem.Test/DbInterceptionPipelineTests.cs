using System.Collections;
using System.Diagnostics;

namespace DbSqlLikeMem.Test;

/// <summary>
/// EN: Covers the initial ADO.NET interception pipeline wrappers and callback order.
/// PT: Cobre os wrappers iniciais do pipeline de interceptacao ADO.NET e a ordem dos callbacks.
/// </summary>
public sealed class DbInterceptionPipelineTests(
        ITestOutputHelper helper
    ) : XUnitTestBase(helper)
{
    /// <summary>
    /// EN: Ensures command creation and non-query execution notify interceptors and allow command mutation.
    /// PT: Garante que a criacao do comando e a execucao non-query notifiquem interceptors e permitam mutacao do comando.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_CreateCommandAndExecuteNonQuery_ShouldNotifyInterceptorsAndAllowMutation()
    {
        var innerConnection = new FakeDbConnection();
        var events = new List<string>();
        using var connection = DbInterceptionPipeline.Wrap(
            innerConnection,
            new RecordingInterceptor("first", events),
            new RecordingInterceptor("second", events)
            {
                OnCommandCreated = static (_, command) => command.CommandText = "rewritten sql"
            });

        using var command = connection.CreateCommand();
        var affected = command.ExecuteNonQuery();

        affected.Should().Be(7);
        innerConnection.LastExecutedCommandText.Should().Be("rewritten sql");
        events.Should().Equal(
            "first:created",
            "second:created",
            "first:executing:NonQuery",
            "second:executing:NonQuery",
            "second:executed:NonQuery:7",
            "first:executed:NonQuery:7");
    }

    /// <summary>
    /// EN: Ensures open and close dispatch lifecycle callbacks in pipeline order.
    /// PT: Garante que open e close despachem callbacks de ciclo de vida na ordem do pipeline.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_OpenAndClose_ShouldNotifyLifecycleCallbacks()
    {
        var events = new List<string>();
        using var connection = DbInterceptionPipeline.Wrap(
            new FakeDbConnection(),
            new RecordingInterceptor("first", events),
            new RecordingInterceptor("second", events));

        connection.Open();
        connection.Close();

        events.Should().Equal(
            "first:opening",
            "second:opening",
            "second:opened",
            "first:opened",
            "first:closing",
            "second:closing",
            "second:closed",
            "first:closed");
    }

    /// <summary>
    /// EN: Ensures failures flow through the failure callback chain in reverse order.
    /// PT: Garante que falhas passem pela cadeia de callback de falha em ordem reversa.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_ExecuteScalarFailure_ShouldNotifyFailureCallbacks()
    {
        var innerConnection = new FakeDbConnection { ThrowOnScalar = true };
        var events = new List<string>();
        using var connection = DbInterceptionPipeline.Wrap(
            innerConnection,
            new RecordingInterceptor("first", events),
            new RecordingInterceptor("second", events));
        using var command = connection.CreateCommand();

        var ex = FluentActions.Invoking(() => command.ExecuteScalar()).Should().Throw<InvalidOperationException>().Which;

        ex.Message.Should().Be("boom");
        events.Should().Equal(
            "first:created",
            "second:created",
            "first:executing:Scalar",
            "second:executing:Scalar",
            "second:failed:Scalar:boom",
            "first:failed:Scalar:boom");
    }

    /// <summary>
    /// EN: Ensures transactions created by the wrapper keep the wrapped connection on the public surface.
    /// PT: Garante que transacoes criadas pelo wrapper mantenham a conexao encapsulada na superficie publica.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_BeginTransaction_ShouldExposeWrappedConnection()
    {
        using var connection = DbInterceptionPipeline.Wrap(new FakeDbConnection());

        using var transaction = connection.BeginTransaction();

        transaction.Connection.Should().Be(connection);
    }

    /// <summary>
    /// EN: Ensures transaction begin, commit, and rollback notify interceptors in the expected order.
    /// PT: Garante que inicio, commit e rollback de transacao notifiquem interceptors na ordem esperada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_TransactionLifecycle_ShouldNotifyInterceptors()
    {
        var events = new List<string>();
        using var connection = DbInterceptionPipeline.Wrap(
            new FakeDbConnection(),
            new RecordingInterceptor("first", events),
            new RecordingInterceptor("second", events));

        using var committed = connection.BeginTransaction(IsolationLevel.Serializable);
        committed.Commit();

        using var rolledBack = connection.BeginTransaction(IsolationLevel.ReadCommitted);
        rolledBack.Rollback();

        events.Should().Equal(
            "first:tx-starting:Serializable",
            "second:tx-starting:Serializable",
            "second:tx-started:Begin",
            "first:tx-started:Begin",
            "first:tx-executing:Commit",
            "second:tx-executing:Commit",
            "second:tx-executed:Commit",
            "first:tx-executed:Commit",
            "first:tx-starting:ReadCommitted",
            "second:tx-starting:ReadCommitted",
            "second:tx-started:Begin",
            "first:tx-started:Begin",
            "first:tx-executing:Rollback",
            "second:tx-executing:Rollback",
            "second:tx-executed:Rollback",
            "first:tx-executed:Rollback");
    }

    /// <summary>
    /// EN: Ensures the delegate-based interceptor supports lightweight wrapping scenarios.
    /// PT: Garante que o interceptor baseado em delegates suporte cenarios leves de wrapping.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void WithInterceptors_DelegatingInterceptor_ShouldCaptureEvents()
    {
        var innerConnection = new FakeDbConnection();
        var events = new List<string>();
        using var connection = innerConnection.WithInterceptors(
            new DelegatingDbConnectionInterceptor
            {
                OnConnectionOpening = static _ => { },
                OnCommandCreated = (_, command) =>
                {
                    command.CommandText = "delegated sql";
                    events.Add("created");
                },
                OnCommandExecuting = context => events.Add($"executing:{context.ExecutionKind}"),
                OnCommandExecuted = (_, result) => events.Add($"executed:{result}")
            });

        connection.Open();
        using var command = connection.CreateCommand();
        _ = command.ExecuteNonQuery();

        innerConnection.LastExecutedCommandText.Should().Be("delegated sql");
        events.Should().Equal(
            "created",
            "executing:NonQuery",
            "executed:7");
    }

    /// <summary>
    /// EN: Ensures disposing wrappers also disposes the inner ADO.NET objects.
    /// PT: Garante que descartar wrappers tambem descarte os objetos ADO.NET internos.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_Dispose_ShouldDisposeInnerObjects()
    {
        var innerConnection = new FakeDbConnection();
        var connection = DbInterceptionPipeline.Wrap(innerConnection);
        var command = connection.CreateCommand();
        var transaction = connection.BeginTransaction();

        transaction.Dispose();
        command.Dispose();
        connection.Dispose();

        innerConnection.TransactionDisposed.Should().BeTrue();
        innerConnection.CommandDisposed.Should().BeTrue();
        innerConnection.ConnectionDisposed.Should().BeTrue();
    }

    /// <summary>
    /// EN: Ensures the built-in recording interceptor captures connection, command, and transaction events with details.
    /// PT: Garante que o interceptor interno de gravacao capture eventos de conexao, comando e transacao com detalhes.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_RecordingInterceptor_ShouldCaptureDetailedEvents()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        using var connection = DbInterceptionPipeline.Wrap(new FakeDbConnection(), recorder);

        connection.Open();
        using (var transaction = connection.BeginTransaction(IsolationLevel.Serializable))
        {
            using var command = connection.CreateCommand();
            command.CommandText = "select 42";
            _ = command.ExecuteScalar();
            transaction.Commit();
        }
        connection.Close();

        var events = recorder.Events;

        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.ConnectionOpening);
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.TransactionStarting
            && x.TransactionOperationKind == DbTransactionOperationKind.Begin
            && x.IsolationLevel == IsolationLevel.Serializable);
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuting
            && x.CommandExecutionKind == DbCommandExecutionKind.Scalar
            && x.CommandText == "select 42");
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuted
            && Equals(x.Result, 42));
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.TransactionExecuted
            && x.TransactionOperationKind == DbTransactionOperationKind.Commit);
        events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.ConnectionClosed);

        var formatted = recorder.GetFormattedEvents();
        formatted.Should().Contain(x => x.Contains("event=ConnectionOpening", StringComparison.Ordinal));
        formatted.Should().Contain(x => x.Contains("commandKind=Scalar", StringComparison.Ordinal));
        formatted.Should().Contain(x => x.Contains("transactionKind=Commit", StringComparison.Ordinal));

        recorder.Clear();
        recorder.Events.Should().BeEmpty();
    }

    /// <summary>
    /// EN: Ensures the fault-injection interceptor can fail command execution before the inner command runs.
    /// PT: Garante que o interceptor de injecao de falha possa falhar a execucao do comando antes de o comando interno executar.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_FaultInjectionInterceptor_ShouldFailCommandBeforeInnerExecution()
    {
        var innerConnection = new FakeDbConnection();
        using var connection = innerConnection.WithInterceptors(
            new FaultInjectionDbConnectionInterceptor
            {
                ShouldFailCommand = static context => context.ExecutionKind == DbCommandExecutionKind.NonQuery,
                CommandExceptionFactory = static context => new IOException($"blocked:{context.Command.CommandText}")
            });
        using var command = connection.CreateCommand();
        command.CommandText = "delete from users";

        var ex = FluentActions.Invoking(() => command.ExecuteNonQuery()).Should().Throw<IOException>().Which;

        ex.Message.Should().Be("blocked:delete from users");
        innerConnection.LastExecutedCommandText.Should().BeNull();
    }

    /// <summary>
    /// EN: Ensures the fault-injection interceptor can fail transaction start and connection open paths.
    /// PT: Garante que o interceptor de injecao de falha possa falhar os fluxos de inicio de transacao e abertura de conexao.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_FaultInjectionInterceptor_ShouldFailConnectionAndTransactionOperations()
    {
        using var blockedOpen = new FakeDbConnection().WithInterceptors(
            new FaultInjectionDbConnectionInterceptor
            {
                ShouldFailConnectionOpen = static _ => true,
                ConnectionOpenExceptionFactory = static _ => new IOException("open-blocked")
            });

        var openEx = FluentActions.Invoking(() => blockedOpen.Open()).Should().Throw<IOException>().Which;
        openEx.Message.Should().Be("open-blocked");

        using var blockedTransaction = new FakeDbConnection().WithInterceptors(
            new FaultInjectionDbConnectionInterceptor
            {
                ShouldFailTransactionStart = static context => context.IsolationLevel == IsolationLevel.Serializable,
                TransactionStartExceptionFactory = static _ => new IOException("tx-start-blocked")
            });
        blockedTransaction.Open();

        var txEx = FluentActions.Invoking(() => blockedTransaction.BeginTransaction(IsolationLevel.Serializable)).Should().Throw<IOException>().Which;
        txEx.Message.Should().Be("tx-start-blocked");
    }

    /// <summary>
    /// EN: Ensures the logging interceptor emits structured messages for connection, command, and transaction flows.
    /// PT: Garante que o interceptor de logging emita mensagens estruturadas para fluxos de conexao, comando e transacao.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_LoggingInterceptor_ShouldEmitStructuredMessages()
    {
        var messages = new List<string>();
        using var connection = new FakeDbConnection().WithInterceptors(
            new LoggingDbConnectionInterceptor(messages.Add));

        connection.Open();
        using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            using var command = connection.CreateCommand();
            command.CommandText = "select 42";
            _ = command.ExecuteScalar();
            transaction.Commit();
        }
        connection.Close();

        messages.Should().Contain(x => x.Contains("event=ConnectionOpening", StringComparison.Ordinal));
        messages.Should().Contain(x => x.Contains("event=CommandCreated", StringComparison.Ordinal)
            && x.Contains("sql=select 1", StringComparison.Ordinal));
        messages.Should().Contain(x => x.Contains("event=CommandExecuted", StringComparison.Ordinal)
            && x.Contains("sql=select 42", StringComparison.Ordinal)
            && x.Contains("commandKind=Scalar", StringComparison.Ordinal)
            && x.Contains("result=42", StringComparison.Ordinal));
        messages.Should().Contain(x => x.Contains("event=TransactionStarting", StringComparison.Ordinal)
            && x.Contains("transactionKind=Begin", StringComparison.Ordinal)
            && x.Contains("isolation=ReadCommitted", StringComparison.Ordinal));
        messages.Should().Contain(x => x.Contains("event=TransactionExecuted", StringComparison.Ordinal)
            && x.Contains("transactionKind=Commit", StringComparison.Ordinal));
        messages.Should().Contain(x => x.Contains("event=ConnectionClosed", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Ensures the text-writer interceptor writes one formatted line per captured event.
    /// PT: Garante que o interceptor de text writer escreva uma linha formatada por evento capturado.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_TextWriterInterceptor_ShouldWriteFormattedLines()
    {
        using var writer = new StringWriter();
        using var connection = new FakeDbConnection().WithInterceptors(
            new TextWriterDbConnectionInterceptor(writer));

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 42";
        _ = command.ExecuteScalar();
        connection.Close();

        var text = writer.ToString();
        text.Should().Contain("event=ConnectionOpening");
        text.Should().Contain("event=CommandCreated");
        text.Should().Contain("sql=select 42");
        text.Should().Contain("commandKind=Scalar");
        text.Should().Contain("event=ConnectionClosed");
    }

    /// <summary>
    /// EN: Ensures interception options compose the configured built-in interceptors and custom interceptors.
    /// PT: Garante que as opcoes de interceptacao componham os interceptors nativos configurados e os interceptors customizados.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void WithInterception_Options_ShouldComposeConfiguredInterceptors()
    {
        var logLines = new List<string>();
        using var writer = new StringWriter();
        var customEvents = new List<string>();
        var recorder = new RecordingDbConnectionInterceptor();
        using var connection = new FakeDbConnection().WithInterception(options =>
        {
            options.EnableRecording = true;
            options.RecordingInterceptor = recorder;
            options.LogAction = logLines.Add;
            options.TextWriter = writer;
            options.AdditionalInterceptors.Add(new DelegatingDbConnectionInterceptor
            {
                OnCommandExecuting = context => customEvents.Add(context.Command.CommandText)
            });
        });

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 42";
        _ = command.ExecuteScalar();
        connection.Close();

        logLines.Should().Contain(x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        writer.ToString().Should().Contain("event=CommandExecuted");
        customEvents.Should().Equal("select 42");
        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Ensures the generic interception connection factory wraps and opens connections produced by an inner factory delegate.
    /// PT: Garante que a factory generica de conexao com interceptacao encapsule e abra conexoes produzidas por um delegate interno.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void WithInterceptionFactory_ShouldCreateWrappedConnections()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var factory = new Func<DbConnection>(() => new FakeDbConnection())
            .WithInterceptionFactory(recorder);

        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 123";
        _ = command.ExecuteScalar();

        connection.State.Should().Be(ConnectionState.Open);
        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.ConnectionOpened);
        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Ensures the interception connection factory can compose options-based interceptors for each created connection.
    /// PT: Garante que a factory de conexao com interceptacao consiga compor interceptors baseados em opcoes para cada conexao criada.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void WithInterceptionFactory_Options_ShouldComposeBuiltInInterceptors()
    {
        var lines = new List<string>();
        var factory = new Func<DbConnection>(() => new FakeDbConnection())
            .WithInterceptionFactory(options =>
            {
                options.LogAction = lines.Add;
                options.EnableRecording = true;
            });

        using var connection = factory.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 456";
        _ = command.ExecuteScalar();

        lines.Should().Contain(x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Ensures the interception connection factory also accepts a prebuilt options instance directly.
    /// PT: Garante que a factory de conexao com interceptacao tambem aceite diretamente uma instancia pronta de opcoes.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void WithInterceptionFactory_OptionsInstance_ShouldComposeBuiltInInterceptors()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var options = new DbInterceptionOptions
        {
            EnableRecording = true,
            RecordingInterceptor = recorder
        };

        var factory = new Func<DbConnection>(() => new FakeDbConnection())
            .WithInterceptionFactory(options);

        using var connection = factory.CreateOpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = "select 654";
        _ = command.ExecuteScalar();

        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
    }

    /// <summary>
    /// EN: Ensures fluent option helpers configure the interception pipeline without manual property wiring.
    /// PT: Garante que os helpers fluentes de opcoes configurem o pipeline de interceptacao sem wiring manual de propriedades.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbInterceptionOptions_FluentHelpers_ShouldComposePipeline()
    {
        var recorder = new RecordingDbConnectionInterceptor();
        var lines = new List<string>();
        using var writer = new StringWriter();
        var customCommands = new List<string>();

        using var connection = new FakeDbConnection().WithInterception(options => options
            .UseRecording(recorder)
            .UseLogging(lines.Add)
            .UseTextWriter(writer)
            .AddInterceptor(new DelegatingDbConnectionInterceptor
            {
                OnCommandExecuting = context => customCommands.Add(context.Command.CommandText)
            }));

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 789";
        _ = command.ExecuteScalar();

        recorder.Events.Should().Contain(x => x.EventKind == DbInterceptionEventKind.CommandExecuted);
        lines.Should().Contain(x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        writer.ToString().Should().Contain("event=CommandExecuted");
        customCommands.Should().Equal("select 789");
    }

    /// <summary>
    /// EN: Ensures the static pipeline API can configure interception options inline without using extension helpers.
    /// PT: Garante que a API estatica do pipeline consiga configurar opcoes de interceptacao inline sem usar extensions.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_WithInlineOptions_ShouldComposePipeline()
    {
        var lines = new List<string>();
        using var connection = DbInterceptionPipeline.Wrap(
            new FakeDbConnection(),
            options => options.UseLogging(lines.Add));

        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "select 999";
        _ = command.ExecuteScalar();

        lines.Should().Contain(x => x.Contains("event=CommandExecuted", StringComparison.Ordinal));
        lines.Should().Contain(x => x.Contains("sql=select 999", StringComparison.Ordinal));
    }

    /// <summary>
    /// EN: Ensures the shared formatter emits the normalized representation used by logging and recording helpers.
    /// PT: Garante que o formatter compartilhado emita a representacao normalizada usada pelos helpers de logging e gravacao.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void DbInterceptionEventFormatter_Format_ShouldNormalizeKnownFields()
    {
        var text = DbInterceptionEventFormatter.Format(new DbInterceptionEvent
        {
            EventKind = DbInterceptionEventKind.CommandFailed,
            ConnectionState = ConnectionState.Open,
            CommandText = "select 1",
            CommandExecutionKind = DbCommandExecutionKind.Reader,
            PerformanceMetricsDelta = "sql.parse[hits=1,ms=0.123]",
            Exception = new InvalidOperationException("broken")
        });

        text.Should().Contain("event=CommandFailed");
        text.Should().Contain("state=Open");
        text.Should().Contain("sql=select 1");
        text.Should().Contain("commandKind=Reader");
        text.Should().Contain("performanceDelta=sql.parse[hits=1,ms=0.123]");
        text.Should().Contain("exception=broken");
    }

    /// <summary>
    /// EN: Ensures the diagnostic-listener interceptor publishes typed payloads with the documented event names.
    /// PT: Garante que o interceptor de diagnostic listener publique payloads tipados com os nomes de evento documentados.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_DiagnosticListenerInterceptor_ShouldPublishKnownEvents()
    {
        var listener = new DiagnosticListener("DbSqlLikeMem.Test.Interception");
        var observer = new DiagnosticEventObserver();
        using var subscription = listener.Subscribe(observer);
        using var connection = new FakeDbConnection().WithInterceptors(
            new DiagnosticListenerDbConnectionInterceptor(listener));

        connection.Open();
        using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            using var command = connection.CreateCommand();
            command.CommandText = "select 42";
            _ = command.ExecuteScalar();
            transaction.Commit();
        }
        connection.Close();

        observer.Events.Should().Contain(x => x.Key == DbInterceptionDiagnosticNames.ConnectionOpening);
        observer.Events.Should().Contain(x => x.Key == DbInterceptionDiagnosticNames.CommandExecuting);
        observer.Events.Should().Contain(x => x.Key == DbInterceptionDiagnosticNames.TransactionExecuted);
        observer.Events.Should().AllSatisfy(x => x.Value.Should().BeOfType<DbInterceptionEvent>());

        var commandEvent = observer.Events.Single(x => x.Key == DbInterceptionDiagnosticNames.CommandExecuting);
        var payload = commandEvent.Value.Should().BeOfType<DbInterceptionEvent>().Which;
        payload.CommandExecutionKind.Should().Be(DbCommandExecutionKind.Scalar);
        payload.CommandText.Should().Be("select 42");
    }

#if NET8_0_OR_GREATER
    /// <summary>
    /// EN: Ensures the activity-source interceptor emits activities with the documented names and tags.
    /// PT: Garante que o interceptor de activity source emita activities com os nomes e tags documentados.
    /// </summary>
    [Fact]
    [Trait("Category", "Core")]
    public void Wrap_ActivitySourceInterceptor_ShouldPublishActivities()
    {
        var activities = new List<Activity>();
        using var activitySource = new ActivitySource("DbSqlLikeMem.Test.Activity");
        using var listener = new ActivityListener
        {
            ShouldListenTo = source => source.Name == activitySource.Name,
            Sample = static(ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = activity => activities.Add(activity)
        };
        ActivitySource.AddActivityListener(listener);

        using var connection = new FakeDbConnection().WithInterceptors(
            new ActivitySourceDbConnectionInterceptor(activitySource));

        connection.Open();
        using (var transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted))
        {
            using var command = connection.CreateCommand();
            command.CommandText = "select 42";
            _ = command.ExecuteScalar();
            transaction.Commit();
        }
        connection.Close();

        activities.Should().Contain(x => x.OperationName == DbInterceptionActivityNames.ConnectionOpen);
        activities.Should().Contain(x => x.OperationName == DbInterceptionActivityNames.Command);
        activities.Should().Contain(x => x.OperationName == DbInterceptionActivityNames.TransactionOperation);

        var commandActivity = activities.Single(x => x.OperationName == DbInterceptionActivityNames.Command);
        commandActivity.Tags.FirstOrDefault(x => x.Key == "db.operation").Value.Should().Be("Scalar");
        commandActivity.Tags.FirstOrDefault(x => x.Key == "db.statement").Value.Should().Be("select 42");
    }
#endif

#pragma warning disable CS8764, CS8765

    private sealed class RecordingInterceptor : DbConnectionInterceptor
    {
        private readonly string _name;
        private readonly List<string> _events;

        public RecordingInterceptor(string name, List<string> events)
        {
            _name = name;
            _events = events;
        }

        public Action<DbConnection, DbCommand>? OnCommandCreated { get; init; }

        public override void ConnectionOpening(DbConnection connection) => _events.Add($"{_name}:opening");

        public override void ConnectionOpened(DbConnection connection) => _events.Add($"{_name}:opened");

        public override void ConnectionClosing(DbConnection connection) => _events.Add($"{_name}:closing");

        public override void ConnectionClosed(DbConnection connection) => _events.Add($"{_name}:closed");

        public override void CommandCreated(DbConnection connection, DbCommand command)
        {
            OnCommandCreated?.Invoke(connection, command);
            _events.Add($"{_name}:created");
        }

        public override void CommandExecuting(DbCommandExecutionContext context)
            => _events.Add($"{_name}:executing:{context.ExecutionKind}");

        public override void CommandExecuted(DbCommandExecutionContext context, object? result)
            => _events.Add($"{_name}:executed:{context.ExecutionKind}:{result}");

        public override void CommandFailed(DbCommandExecutionContext context, Exception exception)
            => _events.Add($"{_name}:failed:{context.ExecutionKind}:{exception.Message}");

        public override void TransactionStarting(DbTransactionStartingContext context)
            => _events.Add($"{_name}:tx-starting:{context.IsolationLevel}");

        public override void TransactionStarted(DbTransactionInterceptionContext context)
            => _events.Add($"{_name}:tx-started:{context.OperationKind}");

        public override void TransactionExecuting(DbTransactionInterceptionContext context)
            => _events.Add($"{_name}:tx-executing:{context.OperationKind}");

        public override void TransactionExecuted(DbTransactionInterceptionContext context)
            => _events.Add($"{_name}:tx-executed:{context.OperationKind}");

        public override void TransactionFailed(DbTransactionInterceptionContext context, Exception exception)
            => _events.Add($"{_name}:tx-failed:{context.OperationKind}:{exception.Message}");
    }

    private sealed class FakeDbConnection : DbConnection
    {
        private ConnectionState _state = ConnectionState.Closed;

        public string? LastExecutedCommandText { get; set; }

        public bool ThrowOnScalar { get; init; }

        public bool ConnectionDisposed { get; private set; }

        public bool CommandDisposed { get; set; }

        public bool TransactionDisposed { get; set; }

        public override string? ConnectionString { get; set; } = string.Empty;

        public override string Database => "fake";

        public override string DataSource => "fake";

        public override string ServerVersion => "1.0";

        public override ConnectionState State => _state;

        public override void ChangeDatabase(string databaseName)
        {
        }

        public override void Close() => _state = ConnectionState.Closed;

        public override void Open() => _state = ConnectionState.Open;

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel)
            => new FakeDbTransaction(this, isolationLevel);

        protected override DbCommand CreateDbCommand() => new FakeDbCommand(this);

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                ConnectionDisposed = true;

            base.Dispose(disposing);
        }
    }

    private sealed class FakeDbCommand : DbCommand
    {
        private readonly FakeDbConnection _connection;
        private readonly FakeDbParameterCollection _parameters = new();
        private DbTransaction? _transaction;

        public FakeDbCommand(FakeDbConnection connection) => _connection = connection;

        public override string? CommandText { get; set; } = "select 1";

        public override int CommandTimeout { get; set; }

        public override CommandType CommandType { get; set; }

        public override bool DesignTimeVisible { get; set; }

        public override UpdateRowSource UpdatedRowSource { get; set; }

        protected override DbConnection? DbConnection
        {
            get => _connection;
            set => throw new NotSupportedException();
        }

        protected override DbParameterCollection DbParameterCollection => _parameters;

        protected override DbTransaction? DbTransaction
        {
            get => _transaction;
            set => _transaction = value;
        }

        public override void Cancel()
        {
        }

        public override int ExecuteNonQuery()
        {
            _connection.LastExecutedCommandText = CommandText;
            return 7;
        }

        public override object? ExecuteScalar()
        {
            _connection.LastExecutedCommandText = CommandText;
            if (_connection.ThrowOnScalar)
                throw new InvalidOperationException("boom");

            return 42;
        }

        public override void Prepare()
        {
        }

        protected override DbParameter CreateDbParameter() => new FakeDbParameter();

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            _connection.LastExecutedCommandText = CommandText;
            var table = new DataTable();
            table.Columns.Add("value", typeof(int));
            table.Rows.Add(1);
            return table.CreateDataReader();
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _connection.CommandDisposed = true;

            base.Dispose(disposing);
        }
    }

    private sealed class FakeDbTransaction : DbTransaction
    {
        private readonly FakeDbConnection _connection;
        private readonly IsolationLevel _isolationLevel;

        public FakeDbTransaction(FakeDbConnection connection, IsolationLevel isolationLevel)
        {
            _connection = connection;
            _isolationLevel = isolationLevel;
        }

        public override IsolationLevel IsolationLevel => _isolationLevel;

        protected override DbConnection DbConnection => _connection;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                _connection.TransactionDisposed = true;

            base.Dispose(disposing);
        }
    }

    private sealed class FakeDbParameterCollection : DbParameterCollection
    {
        private readonly List<DbParameter> _items = [];

        public override int Count => _items.Count;

        public override object SyncRoot => ((ICollection)_items).SyncRoot;

        public override int Add(object value)
        {
            _items.Add((DbParameter)value);
            return _items.Count - 1;
        }

        public override void AddRange(Array values)
        {
            foreach (var value in values)
                Add(value!);
        }

        public override void Clear() => _items.Clear();

        public override bool Contains(object value) => _items.Contains((DbParameter)value);

        public override bool Contains(string value) => _items.Any(x => x.ParameterName == value);

        public override void CopyTo(Array array, int index) => ((ICollection)_items).CopyTo(array, index);

        public override IEnumerator GetEnumerator() => _items.GetEnumerator();

        public override int IndexOf(object value) => _items.IndexOf((DbParameter)value);

        public override int IndexOf(string parameterName) => _items.FindIndex(x => x.ParameterName == parameterName);

        public override void Insert(int index, object value) => _items.Insert(index, (DbParameter)value);

        public override void Remove(object value) => _items.Remove((DbParameter)value);

        public override void RemoveAt(int index) => _items.RemoveAt(index);

        public override void RemoveAt(string parameterName)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                _items.RemoveAt(index);
        }

        protected override DbParameter GetParameter(int index) => _items[index];

        protected override DbParameter GetParameter(string parameterName)
            => _items[IndexOf(parameterName)];

        protected override void SetParameter(int index, DbParameter value) => _items[index] = value;

        protected override void SetParameter(string parameterName, DbParameter value)
        {
            var index = IndexOf(parameterName);
            if (index >= 0)
                _items[index] = value;
            else
                _items.Add(value);
        }
    }

    private sealed class FakeDbParameter : DbParameter
    {
        public override DbType DbType { get; set; }

        public override ParameterDirection Direction { get; set; }

        public override bool IsNullable { get; set; }

        public override string? ParameterName { get; set; } = string.Empty;

        public override string? SourceColumn { get; set; } = string.Empty;

        public override object? Value { get; set; }

        public override bool SourceColumnNullMapping { get; set; }

        public override int Size { get; set; }

        public override void ResetDbType()
        {
        }
    }

    private sealed class DiagnosticEventObserver : IObserver<KeyValuePair<string, object?>>
    {
        public List<KeyValuePair<string, object?>> Events { get; } = [];

        public void OnCompleted()
        {
        }

        public void OnError(Exception error)
        {
        }

        public void OnNext(KeyValuePair<string, object?> value) => Events.Add(value);
    }

#pragma warning restore CS8764, CS8765
}
