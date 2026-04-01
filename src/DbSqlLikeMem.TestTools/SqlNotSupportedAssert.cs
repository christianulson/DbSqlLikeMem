using FluentAssertions;

namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides shared checks for dialect not-supported errors.
/// PT: Fornece verificacoes compartilhadas para erros de funcionalidade nao suportada por dialeto.
/// </summary>
public static class SqlNotSupportedAssertions
{
    /// <summary>
    /// EN: Asserts the action throws <see cref="NotSupportedException"/> containing the expected feature token.
    /// PT: Garante que a ação lance <see cref="NotSupportedException"/> contendo o token de funcionalidade esperado.
    /// </summary>
    /// <param name="action">EN: Action expected to throw. PT: Ação esperada para lançar exceção.</param>
    /// <param name="expectedFeatureToken">EN: Feature token expected in the message. PT: Token da funcionalidade esperado na mensagem.</param>
    /// <returns>EN: Captured exception instance. PT: Instância da exceção capturada.</returns>
    public static NotSupportedException ThrowsWithFeature(Action action, string expectedFeatureToken)
    {
        var ex = action.Should().Throw<NotSupportedException>().Which;
        ex.Message.Contains(expectedFeatureToken, StringComparison.OrdinalIgnoreCase).Should().BeTrue();
        return ex;
    }
}
