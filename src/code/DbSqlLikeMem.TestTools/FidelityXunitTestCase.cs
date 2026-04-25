#if !NET8_0_OR_GREATER
using System;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Builds xUnit v2 fidelity test cases that resolve provider-specific skip reasons before execution.
/// PT: Monta test cases de fidelidade do xUnit v2 que resolvem motivos de skip especificos do provedor antes da execucao.
/// </summary>
public sealed class FidelityXunitTestCase : XunitTestCase
{
    private const string SkipMarkerTypeName = "DbSqlLikeMem.TestTools.FidelityNativeClientSkipAttribute";

    /// <summary>
    /// EN: Creates the deserialization instance required by xUnit.
    /// PT: Cria a instancia de desserializacao exigida pelo xUnit.
    /// </summary>
#pragma warning disable CS0618
    public FidelityXunitTestCase()
    {
    }
#pragma warning restore CS0618

    /// <summary>
    /// EN: Creates a fidelity test case for the current method.
    /// PT: Cria um test case de fidelidade para o metodo atual.
    /// </summary>
    /// <param name="diagnosticMessageSink">EN: Sink used for xUnit diagnostic messages. PT: Canal usado para mensagens diagnosticas do xUnit.</param>
    /// <param name="defaultMethodDisplay">EN: Default method display selected by the runner. PT: Exibicao padrao de metodo selecionada pelo runner.</param>
    /// <param name="defaultMethodDisplayOptions">EN: Default method display options selected by the runner. PT: Opcoes padrao de exibicao de metodo selecionadas pelo runner.</param>
    /// <param name="testMethod">EN: Test method associated with this case. PT: Metodo de teste associado a este caso.</param>
    /// <param name="testMethodArguments">EN: Arguments used by the test method. PT: Argumentos usados pelo metodo de teste.</param>
    public FidelityXunitTestCase(
        IMessageSink diagnosticMessageSink,
        TestMethodDisplay defaultMethodDisplay,
        TestMethodDisplayOptions defaultMethodDisplayOptions,
        ITestMethod testMethod,
        object[]? testMethodArguments)
        : base(diagnosticMessageSink, defaultMethodDisplay, defaultMethodDisplayOptions, testMethod, testMethodArguments)
    {
    }

    /// <inheritdoc />
    protected override string? GetSkipReason(IAttributeInfo factAttribute)
    {
        return ResolveSkipReason() ?? base.GetSkipReason(factAttribute);
    }

    private string? ResolveSkipReason()
    {
        if (TestMethod.TestClass.Class is IReflectionTypeInfo reflectionType)
        {
            var skipReason = GetSkipReason(reflectionType.Type);
            if (!string.IsNullOrWhiteSpace(skipReason))
                return skipReason;
        }

        if (TestMethod.Method is IReflectionMethodInfo reflectionMethod
            && reflectionMethod.MethodInfo.DeclaringType is not null)
        {
            var skipReason = GetSkipReason(reflectionMethod.MethodInfo.DeclaringType);
            if (!string.IsNullOrWhiteSpace(skipReason))
                return skipReason;
        }

        return null;
    }

    private static string? GetSkipReason(Type type)
    {
        for (var current = type; current is not null; current = current.BaseType)
        {
            foreach (var attributeData in current.GetCustomAttributesData())
            {
                if (attributeData.AttributeType.FullName != SkipMarkerTypeName)
                    continue;

                if (Activator.CreateInstance(attributeData.AttributeType) is not IFidelityTestSkipProvider skipProvider)
                    continue;

                var skipReason = skipProvider.GetSkipReason();
                if (!string.IsNullOrWhiteSpace(skipReason))
                    return skipReason;
            }
        }

        return null;
    }
}
#endif

