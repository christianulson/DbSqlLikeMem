using Docker.DotNet.Models;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using IBM.Data.Db2;

namespace DbSqlLikeMem.Benchmarks.Sessions.External;

/// <summary>
/// EN: Runs the Db2 benchmark session against an IBM Db2 container started by Testcontainers.
/// PT-br: Executa a sessão de benchmark do Db2 contra um container IBM Db2 iniciado pelo Testcontainers.
/// </summary>
public sealed class Db2TestcontainersSession()
    : ExternalBenchmarkSessionBase(new Db2Dialect(), BenchmarkEngine.Testcontainers)
{
    private const string Image = "icr.io/db2_community/db2:12.1.0.0";
    private const string DatabaseName = "BENCH";
    private const string Username = "db2inst1";
    private const string Password = "db2inst1";
    private const ushort Db2Port = 50000;

    private IContainer? _container;

    /// <summary>
    /// EN: Starts the external Db2 runtime, waits until the database is really accepting client connections,
    /// and returns a stable ADO.NET connection string for the benchmark operations.
    /// PT-br: Inicia o runtime externo do Db2, espera até que o banco esteja realmente aceitando conexões de cliente
    /// e retorna uma string de conexão ADO.NET estável para as operações do benchmark.
    /// </summary>
    /// <returns>
    /// EN: A Db2 connection string pointing to the started container.
    /// PT-br: Uma string de conexão do Db2 apontando para o container iniciado.
    /// </returns>
    protected override string StartExternalRuntime()
    {
        _container = new ContainerBuilder(Image)
            .WithPortBinding(Db2Port, true)
            .WithEnvironment("LICENSE", "accept")
            .WithEnvironment("DB2INSTANCE", Username)
            .WithEnvironment("DB2INST1_PASSWORD", Password)
            .WithEnvironment("DBNAME", DatabaseName)
            .WithWaitStrategy(
                Wait.ForUnixContainer()
                    .UntilInternalTcpPortIsAvailable(Db2Port)
                    .UntilMessageIsLogged("Setup has completed")
                    .UntilMessageIsLogged("INSTANCE", o => o.WithTimeout(TimeSpan.FromMinutes(15))))
            .WithCreateParameterModifier(parameters =>
            {
                parameters.HostConfig ??= new HostConfig();
                parameters.HostConfig.Privileged = true;
            })
            .Build();

        _container.StartAsync().GetAwaiter().GetResult();

        return BuildConnectionString(_container);
    }

    /// <summary>
    /// EN: Waits until Db2 can complete a real client roundtrip, not only until the container process is running.
    /// This avoids SQL30081N recv errors caused by the database still finishing its internal startup sequence.
    /// PT-br: Aguarda até que o Db2 consiga completar um roundtrip real de cliente, e não apenas até o processo do
    /// container estar em execução. Isso evita erros SQL30081N recv causados pelo banco ainda estar finalizando a
    /// sequência interna de inicialização.
    /// </summary>
    /// <param name="connectionString">
    /// EN: The Db2 connection string used for the readiness probe.
    /// PT-br: A string de conexão do Db2 usada na sonda de prontidão.
    /// </param>
    protected override void EnsureExternalRuntimeIsReady(string connectionString)
    {
        var deadline = DateTime.UtcNow.AddMinutes(8);
        Exception? lastError = null;

        while (DateTime.UtcNow < deadline)
        {
            try
            {
                using var connection = new DB2Connection(connectionString);
                connection.Open();

                using var command = connection.CreateCommand();
                command.CommandText = "SELECT 1 FROM SYSIBM.SYSDUMMY1";
                _ = command.ExecuteScalar();
                return;
            }
            catch (Exception ex)
            {
                lastError = ex;
                Thread.Sleep(TimeSpan.FromSeconds(2));
            }
        }

        throw new TimeoutException(
            "Db2 runtime became reachable, but the database did not become ready for client connections within the expected time.",
            lastError);
    }

    /// <summary>
    /// EN: Creates a provider-specific Db2 connection using a configuration that avoids stale pooled sockets
    /// during long benchmark runs.
    /// PT-br: Cria uma conexão Db2 específica do provider usando uma configuração que evita sockets em pool
    /// obsoletos durante execuções longas de benchmark.
    /// </summary>
    /// <param name="connectionString">
    /// EN: The connection string for the started Db2 container.
    /// PT-br: A string de conexão do container Db2 iniciado.
    /// </param>
    /// <returns>
    /// EN: A Db2 ADO.NET connection instance.
    /// PT-br: Uma instância de conexão ADO.NET do Db2.
    /// </returns>
    protected override DbConnection CreateProviderConnection(string connectionString)
    {
        return new DB2Connection(connectionString);
    }

    /// <summary>
    /// EN: Disposes the Db2 test container and releases Docker resources.
    /// PT-br: Descarta o container de teste do Db2 e libera os recursos do Docker.
    /// </summary>
    protected override void DisposeOwnedRuntime()
    {
        if (_container is not null)
        {
            _container.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Builds the final Db2 connection string using the mapped Docker host port.
    /// PT-br: Monta a string de conexão final do Db2 usando a porta mapeada do host Docker.
    /// </summary>
    /// <param name="container">
    /// EN: The started test container.
    /// PT-br: O container de teste iniciado.
    /// </param>
    /// <returns>
    /// EN: A normalized Db2 connection string for benchmark execution.
    /// PT-br: Uma string de conexão Db2 normalizada para a execução dos benchmarks.
    /// </returns>
    private static string BuildConnectionString(IContainer container)
    {
        var builder = new DB2ConnectionStringBuilder
        {
            Server = $"127.0.0.1:{container.GetMappedPublicPort(Db2Port)}",
            Database = DatabaseName,
            UserID = Username,
            Password = Password,
            Pooling = false,
            PersistSecurityInfo = true
        };

        return builder.ConnectionString;
    }
}
