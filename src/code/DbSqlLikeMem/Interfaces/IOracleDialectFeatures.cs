namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes Oracle-specific DDL and helper function support for a SQL dialect.
/// PT-br: Expõe suporte especifico de Oracle para DDL e funcoes auxiliares em um dialeto SQL.
/// </summary>
internal interface IOracleDialectFeatures
{
    /// <summary>
    /// EN: Indicates whether Oracle CREATE FUNCTION DDL is supported.
    /// PT-br: Indica se o DDL CREATE FUNCTION do Oracle e suportado.
    /// </summary>
    bool SupportsOracleCreateFunctionDdl { get; }
    /// <summary>
    /// EN: Indicates whether an Oracle-specific conversion helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle especifico de conversao e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleSpecificConversionFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle SCN helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de SCN e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleScnFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle analytics/modeling helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de analytics/modelagem e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleAnalyticsFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle clustering/data mining helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de clustering/data mining e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleClusterFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle container identifier helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de identificador de container e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleContainerFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle rowid helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de rowid e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleRowIdFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle user environment helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de ambiente do usuario e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleUserEnvFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle validation helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de validacao e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleValidationFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle JSON transform helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de JSON transform e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleJsonTransformFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle collation helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de collation e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleCollationFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle NLS helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de NLS e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleNlsFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle hash helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de hash e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleHashFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle SYS helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle SYS e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleSysFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether an Oracle reserved identifier is supported by the current dialect/version.
    /// PT-br: Indica se um identificador reservado Oracle e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleReservedIdentifier(string identifier);
    /// <summary>
    /// EN: Indicates whether an Oracle time/date helper is supported by the current dialect/version.
    /// PT-br: Indica se um helper Oracle de tempo/data e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsOracleTimeFunction(string functionName);
}
