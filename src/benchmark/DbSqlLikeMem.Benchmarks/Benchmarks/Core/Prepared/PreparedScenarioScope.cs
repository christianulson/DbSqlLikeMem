namespace DbSqlLikeMem.Benchmarks.Core;
public abstract partial class BenchmarkSessionBase
{
    internal sealed class PreparedScenarioScope<TScenario, TService>(
        RepoService repo,
        FidelityTestContext context,
        TScenario scenario,
        TService service) : IDisposable
        where TScenario : class, ITestScenario
    {
        public RepoService Repo { get; } = repo;

        public FidelityTestContext Context { get; } = context;

        public TScenario Scenario { get; } = scenario;

        public TService Service { get; } = service;

        public DbConnection Connection => Repo.Cnn;

        public void Dispose()
        {
#pragma warning disable CA1031 // Do not catch general exception types
            try
            {
                Scenario.DropScenarioAsync().GetAwaiter().GetResult();
            }
            catch
            {
                // Ignore cleanup failures during benchmark teardown.
            }
            finally
            {
                Repo.Dispose();
            }
#pragma warning restore CA1031 // Do not catch general exception types
        }
    }

}
