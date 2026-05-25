namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes sequence syntax, parser compatibility, table hints, and auxiliary naming support for a SQL dialect.
/// PT-br: Expõe sintaxe de sequence, compatibilidade de parser, table hints e suporte a nomes auxiliares para um dialeto SQL.
/// </summary>
internal interface ISqlDialectCompatibility
{
    bool SupportsNextValueForSequenceExpression { get; }
    bool SupportsPreviousValueForSequenceExpression { get; }
    /// <summary>
    /// EN: Indicates whether a sequence-related scalar function name is supported by the current dialect/version.
    /// PT-br: Indica se um nome de funcao escalar relacionado a sequence e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsSequenceFunctionCall(string functionName);
    bool SupportsSequenceDotValueExpression(string suffix);
    bool SupportsDoubleAtIdentifierSyntax { get; }
    bool SupportsSqlCalcFoundRowsModifier { get; }
    /// <summary>
    /// EN: Indicates whether a LAST/FOUND ROWS helper function is supported by the current dialect/version.
    /// PT-br: Indica se uma funcao auxiliar LAST/FOUND ROWS e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsLastFoundRowsFunction(string functionName);
    /// <summary>
    /// EN: Indicates whether a LAST/FOUND ROWS helper identifier is supported by the current dialect/version.
    /// PT-br: Indica se um identificador auxiliar LAST/FOUND ROWS e suportado pelo dialeto/versao atual.
    /// </summary>
    bool SupportsLastFoundRowsIdentifier(string identifier);
    /// <summary>
    /// EN: Indicates whether parser-level INSERT ... SELECT upsert suffix compatibility is enabled.
    /// PT-br: Indica se a compatibilidade do sufixo de upsert em INSERT ... SELECT no parser esta habilitada.
    /// </summary>
    bool AllowsParserInsertSelectUpsertSuffix { get; }
    /// <summary>
    /// EN: Indicates whether DELETE without FROM compatibility is enabled in the parser.
    /// PT-br: Indica se a compatibilidade de DELETE sem FROM esta habilitada no parser.
    /// </summary>
    bool AllowsParserDeleteWithoutFromCompatibility { get; }
    /// <summary>
    /// EN: Indicates whether LIMIT/OFFSET compatibility is enabled in the parser.
    /// PT-br: Indica se a compatibilidade LIMIT/OFFSET esta habilitada no parser.
    /// </summary>
    bool AllowsParserLimitOffsetCompatibility { get; }
    /// <summary>
    /// EN: Returns the parser cache suffix used to isolate dialect modes that change tokenization or parsing.
    /// PT-br: Retorna o sufixo de cache do parser usado para isolar modos de dialeto que mudam tokenizacao ou parsing.
    /// </summary>
    string ParserCacheKeySuffix { get; }
    /// <summary>
    /// EN: Indicates whether MySQL index hints are supported.
    /// PT-br: Indica se hints de indice do MySQL sao suportados.
    /// </summary>
    bool SupportsMySqlIndexHints { get; }
    /// <summary>
    /// EN: Indicates whether MariaDB-specific functions are supported.
    /// PT-br: Indica se funcoes especificas do MariaDB sao suportadas.
    /// </summary>
    bool SupportsMariaDbFunctions { get; }
    /// <summary>
    /// EN: Indicates whether DB2 trigger DDL is supported.
    /// PT-br: Indica se DDL de trigger do DB2 e suportado.
    /// </summary>
    bool SupportsDb2TriggerDdl { get; }
    /// <summary>
    /// EN: Indicates whether DB2 procedure DDL is supported.
    /// PT-br: Indica se DDL de procedure do DB2 e suportado.
    /// </summary>
    bool SupportsDb2ProcedureDdl { get; }
    /// <summary>
    /// EN: Indicates whether PostgreSQL CREATE FUNCTION DDL is supported.
    /// PT-br: Indica se DDL CREATE FUNCTION do PostgreSQL e suportado.
    /// </summary>
    bool SupportsPostgreSqlCreateFunctionDdl { get; }
    /// <summary>
    /// EN: Indicates whether inline-return CREATE FUNCTION DDL is supported.
    /// PT-br: Indica se DDL CREATE FUNCTION com retorno inline e suportado.
    /// </summary>
    bool SupportsInlineReturnCreateFunctionDdl { get; }
    /// <summary>
    /// EN: Indicates whether hash-prefixed identifiers are allowed.
    /// PT-br: Indica se identificadores iniciados por hash sao permitidos.
    /// </summary>
    bool AllowsHashIdentifiers { get; }
    /// <summary>
    /// EN: Resolves the temporary table scope for a table name and optional schema.
    /// PT-br: Resolve o escopo de uma tabela temporaria a partir do nome da tabela e schema opcional.
    /// </summary>
    TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName);
    /// <summary>
    /// EN: Maps a token to a binary operator supported by the current dialect.
    /// PT-br: Mapeia um token para um operador binario suportado pelo dialeto atual.
    /// </summary>
    bool TryMapBinaryOperator(string token, out SqlBinaryOp op);
}
