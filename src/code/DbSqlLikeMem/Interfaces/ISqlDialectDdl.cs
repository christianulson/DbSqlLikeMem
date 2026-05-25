namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes DDL and sequence capability checks for a SQL dialect.
/// PT-br: Expõe verificações de capacidade de DDL e sequence para um dialeto SQL.
/// </summary>
internal interface ISqlDialectDdl
{
    /// <summary>
    /// EN: Indicates whether MERGE statements are supported.
    /// PT-br: Indica se instrucoes MERGE sao suportadas.
    /// </summary>
    bool SupportsMerge { get; }
    /// <summary>
    /// EN: Indicates whether trigger DDL is supported.
    /// PT-br: Indica se DDL de trigger e suportado.
    /// </summary>
    bool SupportsTriggers { get; }
    /// <summary>
    /// EN: Indicates whether sequence DDL is supported.
    /// PT-br: Indica se DDL de sequence e suportado.
    /// </summary>
    bool SupportsSequenceDdl { get; }
    /// <summary>
    /// EN: Indicates whether CREATE SEQUENCE and ALTER SEQUENCE accept OWNED BY in the current dialect.
    /// PT-br: Indica se CREATE SEQUENCE e ALTER SEQUENCE aceitam OWNED BY no dialeto atual.
    /// </summary>
    bool SupportsSequenceOwnership { get; }
    /// <summary>
    /// EN: Indicates whether function DDL is supported.
    /// PT-br: Indica se DDL de function e suportado.
    /// </summary>
    bool SupportsFunctionDdl { get; }
    /// <summary>
    /// EN: Indicates whether CREATE OR REPLACE FUNCTION DDL is supported.
    /// PT-br: Indica se DDL CREATE OR REPLACE FUNCTION e suportado.
    /// </summary>
    bool SupportsCreateOrReplaceFunctionDdl { get; }
    /// <summary>
    /// EN: Indicates whether CREATE TABLE statements are supported by the current dialect.
    /// PT-br: Indica se instrucoes CREATE TABLE sao suportadas pelo dialeto atual.
    /// </summary>
    bool SupportsCreateTableDdl { get; }
    /// <summary>
    /// EN: Indicates whether CREATE OR REPLACE TABLE statements are supported by the current dialect.
    /// PT-br: Indica se instrucoes CREATE OR REPLACE TABLE sao suportadas pelo dialeto atual.
    /// </summary>
    bool SupportsCreateOrReplaceTableDdl { get; }
    /// <summary>
    /// EN: Indicates whether ALTER TABLE ADD COLUMN is supported by the current dialect.
    /// PT-br: Indica se ALTER TABLE ADD COLUMN e suportado pelo dialeto atual.
    /// </summary>
    bool SupportsAlterTableAddColumn { get; }
}
