namespace DbSqlLikeMem.TestTools;

internal static class TestEnv
{
    /// <summary>
    /// EN: Gets the lazy flag that enables container-backed test execution.
    /// PT: Obtem a flag preguiçosa que habilita a execucao de testes com container.
    /// </summary>
    internal static readonly Lazy<bool> RunContainerTests = new(() =>
        Environment.GetEnvironmentVariable("RUN_CONTAINER_TESTS") == "true");
}
