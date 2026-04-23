namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes SQL Server-specific hints and metadata function support for a SQL dialect.
/// PT: Expõe suporte especifico de SQL Server para hints e funcoes de metadados em um dialeto SQL.
/// </summary>
internal interface ISqlServerDialectFeatures
{
    /// <summary>
    /// EN: Indicates whether SQL Server table hints are supported.
    /// PT: Indica se hints de tabela do SQL Server sao suportados.
    /// </summary>
    bool SupportsSqlServerTableHints { get; }
    /// <summary>
    /// EN: Indicates whether SQL Server query hints are supported.
    /// PT: Indica se hints de consulta do SQL Server sao suportados.
    /// </summary>
    bool SupportsSqlServerQueryHints { get; }
    /// <summary>
    /// EN: Indicates whether a SQL Server metadata function is supported by the current dialect/version.
    /// PT: Indica se uma funcao de metadados do SQL Server e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSqlServerMetadataFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether a SQL Server metadata identifier is supported by the current dialect/version.
    /// PT: Indica se um identificador de metadados do SQL Server e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSqlServerMetadataIdentifier(string identifier);
    /// <summary>
    /// EN: Indicates whether a SQL Server scalar function is supported by the current dialect/version.
    /// PT: Indica se uma funcao escalar do SQL Server e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSqlServerScalarFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether a SQL Server date function is supported by the current dialect/version.
    /// PT: Indica se uma funcao de data do SQL Server e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSqlServerDateFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether a SQL Server aggregate function is supported by the current dialect/version.
    /// PT: Indica se uma funcao agregada do SQL Server e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSqlServerAggregateFunction(string functionName);
}
