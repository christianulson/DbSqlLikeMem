namespace DbSqlLikeMem.Firebird.Test;

/// <summary>
/// EN: Contains tests for Firebird dialect version gates.
/// PT: Contem testes para os gates de versao do dialeto Firebird.
/// </summary>
public sealed class FirebirdDialectVersionTests
{
    /// <summary>
    /// EN: Verifies FUNCTION DDL support starts at Firebird 3.0 and stays enabled afterwards.
    /// PT: Verifica se o suporte a FUNCTION DDL inicia no Firebird 3.0 e permanece habilitado depois disso.
    /// </summary>
    [Theory]
    [InlineData(FirebirdDbVersions.Version2_1, false)]
    [InlineData(FirebirdDbVersions.Version2_5, false)]
    [InlineData(FirebirdDbVersions.Version3_0, true)]
    [InlineData(FirebirdDbVersions.Version4_0, true)]
    [InlineData(FirebirdDbVersions.Version5_0, true)]
    public void SupportsFunctionDdl_ShouldFollowVersionGate(int version, bool expected)
    {
        var dialect = new FirebirdDialect(version);

        dialect.SupportsFunctionDdl.Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies window function support follows the ROW_NUMBER gate and keeps the window frame gate aligned.
    /// PT: Verifica se o suporte a window functions segue o gate de ROW_NUMBER e mantém o gate de window frame alinhado.
    /// </summary>
    [Theory]
    [InlineData(FirebirdDbVersions.Version2_1, false)]
    [InlineData(FirebirdDbVersions.Version2_5, false)]
    [InlineData(FirebirdDbVersions.Version3_0, true)]
    [InlineData(FirebirdDbVersions.Version4_0, true)]
    [InlineData(FirebirdDbVersions.Version5_0, true)]
    public void SupportsWindowFunctions_ShouldFollowVersionGate(int version, bool expected)
    {
        var dialect = new FirebirdDialect(version);

        dialect.SupportsWindowFunctions.Should().Be(expected);
        dialect.SupportsWindowFrameClause.Should().Be(expected);
    }

    /// <summary>
    /// EN: Verifies the Firebird concat null contract stays aligned with the legacy behavior.
    /// PT: Verifica se o contrato de nulidade da concatenacao Firebird permanece alinhado ao comportamento legado.
    /// </summary>
    [Fact]
    public void ConcatNullContract_ShouldStayEnabled()
    {
        var dialect = new FirebirdDialect(FirebirdDbVersions.Default);

        dialect.ConcatReturnsNullOnNullInput.Should().BeTrue();
        dialect.PlusStringConcatReturnsNullOnNullInput.Should().BeTrue();
        dialect.ConcatFunctionReturnsNullOnNullInput.Should().BeTrue();
    }
}
