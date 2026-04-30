#if !NET8_0_OR_GREATER
using Xunit.Abstractions;
using Xunit.Sdk;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Discovers fidelity facts and marks them skipped when the test class or method exposes a skip marker.
/// PT: Descobre facts de fidelidade e os marca como ignorados quando a classe ou o metodo do teste expoe um marcador de skip.
/// </summary>
public sealed class FidelityFactDiscoverer : IXunitTestCaseDiscoverer
{
    private readonly IMessageSink diagnosticMessageSink;

    /// <summary>
    /// EN: Creates a discoverer that can build fidelity fact test cases.
    /// PT: Cria um discoverer que pode montar test cases de fact de fidelidade.
    /// </summary>
    /// <param name="diagnosticMessageSink">EN: Sink used for xUnit diagnostic messages. PT: Canal usado para mensagens diagnosticas do xUnit.</param>
    public FidelityFactDiscoverer(IMessageSink diagnosticMessageSink)
    {
        this.diagnosticMessageSink = diagnosticMessageSink;
    }

    /// <inheritdoc />
    public IEnumerable<IXunitTestCase> Discover(
        ITestFrameworkDiscoveryOptions discoveryOptions,
        ITestMethod testMethod,
        IAttributeInfo factAttribute)
    {
        var testCase = new FidelityXunitTestCase(
            diagnosticMessageSink,
            discoveryOptions.MethodDisplayOrDefault(),
            discoveryOptions.MethodDisplayOptionsOrDefault(),
            testMethod,
            null);

        return new[] { testCase };
    }
}
#endif


