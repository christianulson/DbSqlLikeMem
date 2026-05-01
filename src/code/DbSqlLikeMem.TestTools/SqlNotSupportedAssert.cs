namespace DbSqlLikeMem.TestTools;

/// <summary>
/// EN: Provides shared checks for dialect not-supported errors.
/// PT-br: Fornece verificacoes compartilhadas para erros de funcionalidade nao suportada por dialeto.
/// </summary>
public static class SqlNotSupportedAssertions
{
    /// <summary>
    /// EN: Asserts the action throws <see cref="NotSupportedException"/> containing the expected feature token.
    /// PT-br: Garante que a ação lance <see cref="NotSupportedException"/> contendo o token de funcionalidade esperado.
    /// </summary>
    /// <param name="action">EN: Action expected to throw. PT-br: Ação esperada para lançar exceção.</param>
    /// <param name="expectedFeatureToken">EN: Feature token expected in the message. PT-br: Token da funcionalidade esperado na mensagem.</param>
    /// <returns>EN: Captured exception instance. PT-br: Instância da exceção capturada.</returns>
    public static NotSupportedException ThrowsWithFeature(Action action, string expectedFeatureToken)
    {
        var ex = action.Should().Throw<NotSupportedException>().Which;
        ex.Message.Contains(expectedFeatureToken, StringComparison.OrdinalIgnoreCase).Should().BeTrue();
        return ex;
    }
}
