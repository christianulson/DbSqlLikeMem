namespace DbSqlLikeMem.Firebird;

/// <summary>
/// EN: Extension for verions Firebird versions supported by this provider mock.
/// PT-br: Extesão para versões do Firebird suportadas por este mock de provedor.
/// </summary>
public static class FirebirdDbVersions
{
    /// <summary>
    /// EN: Firebird 2.1 version token.
    /// PT-br: Token de versao do Firebird 2.1.
    /// </summary>
    public const int Version2_1 = 21;

    /// <summary>
    /// EN: Firebird 2.5 version token.
    /// PT-br: Token de versao do Firebird 2.5.
    /// </summary>
    public const int Version2_5 = 25;

    /// <summary>
    /// EN: Firebird 3.0 version token.
    /// PT-br: Token de versao do Firebird 3.0.
    /// </summary>
    public const int Version3_0 = 30;

    /// <summary>
    /// EN: Firebird 4.0 version token.
    /// PT-br: Token de versao do Firebird 4.0.
    /// </summary>
    public const int Version4_0 = 40;

    /// <summary>
    /// EN: Firebird 5.0 version token.
    /// PT-br: Token de versao do Firebird 5.0.
    /// </summary>
    public const int Version5_0 = 50;

    /// <summary>
    /// EN: Default Firebird version used by the mock when none is specified.
    /// PT-br: Versao padrao do Firebird usada pelo mock quando nenhuma e informada.
    /// </summary>
    public const int Default = Version5_0;

    /// <summary>
    /// EN: Returns Firebird versions supported by this provider mock.
    /// PT-br: Retorna as versões do Firebird suportadas por este mock de provedor.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return Version2_1;
        yield return Version2_5;
        yield return Version3_0;
        yield return Version4_0;
        yield return Version5_0;
    }
}