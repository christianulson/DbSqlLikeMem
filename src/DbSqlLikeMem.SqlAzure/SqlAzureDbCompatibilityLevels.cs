namespace DbSqlLikeMem.SqlAzure;

/// <summary>
/// EN: Compatibility level value that emulates SQL Server.
/// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server.
/// </summary>
public static class SqlAzureDbCompatibilityLevels
{
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2008 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2008.
    /// </summary>
    public const int SqlServer2008 = 100;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2012 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2012.
    /// </summary>
    public const int SqlServer2012 = 110;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2014 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2014.
    /// </summary>
    public const int SqlServer2014 = 120;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2016 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2016.
    /// </summary>
    public const int SqlServer2016 = 130;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2017 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2017.
    /// </summary>
    public const int SqlServer2017 = 140;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2019 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2019.
    /// </summary>
    public const int SqlServer2019 = 150;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2022 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2022.
    /// </summary>
    public const int SqlServer2022 = 160;
    /// <summary>
    /// EN: Compatibility level value that emulates SQL Server 2025 behavior.
    /// PT: Valor de nivel de compatibilidade que emula o comportamento do SQL Server 2025.
    /// </summary>
    public const int SqlServer2025 = 170;

    /// <summary>
    /// EN: Default compatibility level used when no explicit version is provided.
    /// PT: Nivel de compatibilidade padrao usado quando nenhuma versao explicita e informada.
    /// </summary>
    public const int Default = SqlServer2022;

    /// <summary>
    /// EN: Returns all SQL Azure compatibility levels supported by this mock provider.
    /// PT: Retorna todos os niveis de compatibilidade do SQL Azure suportados por este provedor simulado.
    /// </summary>
    public static IEnumerable<int> Versions()
    {
        yield return SqlServer2008;
        yield return SqlServer2012;
        yield return SqlServer2014;
        yield return SqlServer2016;
        yield return SqlServer2017;
        yield return SqlServer2019;
        yield return SqlServer2022;
        yield return SqlServer2025;
    }

    internal static int ToSqlServerDialectVersion(int compatibilityLevel)
        => compatibilityLevel switch
        {
            SqlServer2008 => 2008,
            SqlServer2012 => 2012,
            SqlServer2014 => 2014,
            SqlServer2016 => 2016,
            SqlServer2017 => 2017,
            SqlServer2019 => 2019,
            SqlServer2022 => 2022,
            SqlServer2025 => 2025,
            _ => compatibilityLevel,
        };
}