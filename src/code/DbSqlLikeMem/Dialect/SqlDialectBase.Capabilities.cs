namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    /// <summary>
    /// EN: Indicates whether FETCH FIRST pagination is supported.
    /// PT-br: Indica se a paginacao FETCH FIRST e suportada.
    /// </summary>
    public virtual bool SupportsFetchFirst => false;
    /// <summary>
    /// EN: Indicates whether TOP pagination is supported.
    /// PT-br: Indica se a paginacao TOP e suportada.
    /// </summary>
    public virtual bool SupportsTop => false;

    /// <summary>
    /// EN: Indicates whether ON DUPLICATE KEY UPDATE is supported.
    /// PT-br: Indica se ON DUPLICATE KEY UPDATE e suportado.
    /// </summary>
    public virtual bool SupportsOnDuplicateKeyUpdate => false;

    /// <summary>
    /// EN: Indicates whether ON CONFLICT clauses are supported.
    /// PT-br: Indica se clausulas ON CONFLICT sao suportadas.
    /// </summary>
    public virtual bool SupportsOnConflictClause => false;

    /// <summary>
    /// EN: Indicates whether RETURNING is supported.
    /// PT-br: Indica se RETURNING e suportado.
    /// </summary>
    public virtual bool SupportsReturning => false;

    /// <summary>
    /// EN: Gets whether INSERT statements support RETURNING in this dialect.
    /// PT-br: Obtém se instruções INSERT suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsInsertReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether UPDATE statements support RETURNING in this dialect.
    /// PT-br: Obtém se instruções UPDATE suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsUpdateReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether DELETE statements support RETURNING in this dialect.
    /// PT-br: Obtém se instruções DELETE suportam RETURNING neste dialeto.
    /// </summary>
    public virtual bool SupportsDeleteReturning => SupportsReturning;

    /// <summary>
    /// EN: Gets whether RETURNING is allowed for joined or multi-table DELETE statements.
    /// PT-br: Obtém se RETURNING é permitido em instruções DELETE com join ou multi-tabela.
    /// </summary>
    public virtual bool SupportsDeleteReturningWithJoin => true;

    /// <summary>
    /// EN: Gets whether RETURNING clauses accept aggregate functions in this dialect.
    /// PT-br: Obtém se cláusulas RETURNING aceitam funções de agregação neste dialeto.
    /// </summary>
    public virtual bool SupportsAggregateFunctionsInReturningClause => true;

    /// <summary>
    /// EN: Indicates whether MERGE statements are supported.
    /// PT-br: Indica se instrucoes MERGE sao suportadas.
    /// </summary>
    public virtual bool SupportsMerge => false;

    /// <summary>
    /// EN: Indicates whether triggers are supported.
    /// PT-br: Indica se triggers sao suportados.
    /// </summary>
    public virtual bool SupportsTriggers => true;

    /// <summary>
    /// EN: Indicates whether sequence DDL is supported.
    /// PT-br: Indica se DDL de sequencia e suportado.
    /// </summary>
    public virtual bool SupportsSequenceDdl => false;

    /// <summary>
    /// EN: Indicates whether CREATE SEQUENCE and ALTER SEQUENCE accept OWNED BY in this dialect.
    /// PT-br: Indica se CREATE SEQUENCE e ALTER SEQUENCE aceitam OWNED BY neste dialeto.
    /// </summary>
    public virtual bool SupportsSequenceOwnership => false;

    /// <summary>
    /// EN: Indicates whether function DDL is supported.
    /// PT-br: Indica se DDL de funcao e suportado.
    /// </summary>
    public virtual bool SupportsFunctionDdl => false;

    /// <summary>
    /// EN: Indicates whether CREATE OR REPLACE FUNCTION is supported.
    /// PT-br: Indica se CREATE OR REPLACE FUNCTION e suportado.
    /// </summary>
    public virtual bool SupportsCreateOrReplaceFunctionDdl => false;

    /// <summary>
    /// EN: Indicates whether CREATE TABLE statements are supported by this dialect.
    /// PT-br: Indica se instrucoes CREATE TABLE sao suportadas por este dialeto.
    /// </summary>
    public virtual bool SupportsCreateTableDdl => true;

    /// <summary>
    /// EN: Indicates whether CREATE OR REPLACE TABLE statements are supported by this dialect.
    /// PT-br: Indica se instrucoes CREATE OR REPLACE TABLE sao suportadas por este dialeto.
    /// </summary>
    public virtual bool SupportsCreateOrReplaceTableDdl => false;

    /// <summary>
    /// EN: Indicates whether ALTER TABLE ADD COLUMN is supported.
    /// PT-br: Indica se ALTER TABLE ADD COLUMN e suportado.
    /// </summary>
    public virtual bool SupportsAlterTableAddColumn => false;

    public virtual bool SupportsNextValueForSequenceExpression
        => SupportsRegisteredScalarCall("NEXT_VALUE_FOR");

    public virtual bool SupportsPreviousValueForSequenceExpression
        => SupportsRegisteredScalarCall("PREVIOUS_VALUE_FOR");

    public virtual bool SupportsSequenceDotValueExpression(string suffix)
        => TryGetScalarFunctionDefinition(suffix, out var definition)
            && definition is not null
            && definition.AllowsCall;

    /// <summary>
    /// EN: Gets whether a sequence-style scalar call is supported in this dialect.
    /// PT-br: Obtém se uma chamada escalar no estilo sequence é suportada neste dialeto.
    /// </summary>
    public virtual bool SupportsSequenceFunctionCall(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    /// <summary>
    /// EN: Indicates whether double-at identifier syntax is supported.
    /// PT-br: Indica se a sintaxe de identificador com duplo arroba e suportada.
    /// </summary>
    public virtual bool SupportsDoubleAtIdentifierSyntax => false;

    /// <summary>
    /// EN: Indicates whether SQL_CALC_FOUND_ROWS is supported.
    /// PT-br: Indica se SQL_CALC_FOUND_ROWS e suportado.
    /// </summary>
    public virtual bool SupportsSqlCalcFoundRowsModifier => false;

    /// <summary>
    /// EN: Gets whether a LAST/FOUND ROWS helper function is supported in this dialect.
    /// PT-br: Obtém se uma função auxiliar LAST/FOUND ROWS é suportada neste dialeto.
    /// </summary>
    public virtual bool SupportsLastFoundRowsFunction(string functionName)
        => SupportsRegisteredScalarCall(functionName);

    /// <summary>
    /// EN: Gets whether a LAST/FOUND ROWS identifier token is supported in this dialect.
    /// PT-br: Obtém se um token identificador LAST/FOUND ROWS é suportado neste dialeto.
    /// </summary>
    public virtual bool SupportsLastFoundRowsIdentifier(string identifier)
        => SupportsRegisteredScalarIdentifier(identifier);

    /// <summary>
    /// EN: Indicates whether OFFSET/FETCH pagination is supported.
    /// PT-br: Indica se a paginacao OFFSET/FETCH e suportada.
    /// </summary>
    public virtual bool SupportsOffsetFetch => false;

    /// <summary>
    /// EN: Indicates whether OFFSET/FETCH requires an ORDER BY clause.
    /// PT-br: Indica se OFFSET/FETCH exige uma clausula ORDER BY.
    /// </summary>
    public virtual bool RequiresOrderByForOffsetFetch => false;

    public virtual bool SupportsOrderByNullsModifier => false;

    /// <summary>
    /// EN: Indicates whether DELETE without FROM is supported.
    /// PT-br: Indica se DELETE sem FROM e suportado.
    /// </summary>
    public virtual bool SupportsDeleteWithoutFrom => false;

    /// <summary>
    /// EN: Indicates whether DELETE target aliases are supported.
    /// PT-br: Indica se aliases de alvo em DELETE sao suportados.
    /// </summary>
    public virtual bool SupportsDeleteTargetAlias => true;

    /// <summary>
    /// EN: Gets whether MySQL-style UPDATE target JOIN (subquery) syntax is supported.
    /// PT-br: Obtém se a sintaxe UPDATE alvo JOIN (subquery) no estilo MySQL é suportada.
    /// </summary>
    public virtual bool SupportsUpdateJoinFromSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether SQL Server/PostgreSQL-style UPDATE ... FROM ... JOIN (subquery) syntax is supported.
    /// PT-br: Obtém se a sintaxe UPDATE ... FROM ... JOIN (subquery) no estilo SQL Server/PostgreSQL é suportada.
    /// </summary>
    public virtual bool SupportsUpdateFromJoinSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether SQL Server/MySQL-style DELETE target FROM ... JOIN (subquery) syntax is supported.
    /// PT-br: Obtém se a sintaxe DELETE alvo FROM ... JOIN (subquery) no estilo SQL Server/MySQL é suportada.
    /// </summary>
    public virtual bool SupportsDeleteTargetFromJoinSubquerySyntax => false;

    /// <summary>
    /// EN: Gets whether PostgreSQL-style DELETE FROM ... USING (subquery) syntax is supported.
    /// PT-br: Obtém se a sintaxe DELETE FROM ... USING (subquery) no estilo PostgreSQL é suportada.
    /// </summary>
    public virtual bool SupportsDeleteUsingSubquerySyntax => false;

    /// <summary>
    /// EN: Calculates the affected-row count reported by INSERT/UPSERT operations for this dialect.
    /// PT-br: Calcula a contagem de linhas afetadas reportada por operacoes INSERT/UPSERT para este dialeto.
    /// </summary>
    public virtual int GetInsertUpsertAffectedRowCount(int insertedCount, int updatedCount)
        => insertedCount + updatedCount;

    /// <summary>
    /// EN: Indicates whether common table expressions are supported.
    /// PT-br: Indica se common table expressions sao suportadas.
    /// </summary>
    public virtual bool SupportsWithCte => false;

    /// <summary>
    /// EN: Indicates whether recursive CTEs are supported.
    /// PT-br: Indica se CTEs recursivas sao suportadas.
    /// </summary>
    public virtual bool SupportsWithRecursive => true;

    /// <summary>
    /// EN: Indicates whether MATERIALIZED hints are supported on CTEs.
    /// PT-br: Indica se hints MATERIALIZED sao suportados em CTEs.
    /// </summary>
    public virtual bool SupportsWithMaterializedHint => false;

    /// <summary>
    /// EN: Indicates whether the null-safe equality operator is supported.
    /// PT-br: Indica se o operador de igualdade null-safe e suportado.
    /// </summary>
    public virtual bool SupportsNullSafeEq => false;

    /// <summary>
    /// EN: Indicates whether JSON arrow operators are supported.
    /// PT-br: Indica se operadores de seta JSON sao suportados.
    /// </summary>
    public virtual bool SupportsJsonArrowOperators => false;
    /// <summary>
    /// EN: Gets whether JSON_EXTRACT is supported as a scalar helper in this dialect.
    /// PT-br: Obtém se JSON_EXTRACT é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonExtractFunction
        => SupportsRegisteredScalarCall("JSON_EXTRACT");

    /// <summary>
    /// EN: Gets whether JSON_VALUE is supported as a scalar helper in this dialect.
    /// PT-br: Obtém se JSON_VALUE é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonValueFunction
        => SupportsRegisteredScalarCall("JSON_VALUE");

    /// <summary>
    /// EN: Gets whether JSON_QUERY is supported as a scalar helper in this dialect.
    /// PT-br: Obtém se JSON_QUERY é suportada como helper escalar neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonQueryFunction
        => SupportsRegisteredScalarCall("JSON_QUERY");

    /// <summary>
    /// EN: Gets whether OPENJSON is supported as a table function in this dialect.
    /// PT-br: Obtém se OPENJSON é suportada como função de tabela neste dialeto.
    /// </summary>
    public virtual bool SupportsOpenJsonFunction
        => TryGetTableFunctionDefinition(SqlConst.OPENJSON, out _);

    /// <summary>
    /// EN: Gets whether JSON_TABLE is supported as a table function in this dialect.
    /// PT-br: Obtém se JSON_TABLE é suportada como função de tabela neste dialeto.
    /// </summary>
    public virtual bool SupportsJsonTableFunction
        => TryGetTableFunctionDefinition(SqlConst.JSON_TABLE, out _);

    public virtual bool SupportsJsonValueReturningClause => false;

    /// <summary>
    /// EN: Indicates whether quoted identifiers shared across providers are accepted.
    /// PT-br: Indica se identificadores entre aspas compartilhados entre providers sao aceitos.
    /// </summary>
    public virtual bool AllowsParserCrossDialectQuotedIdentifiers => false;

    /// <summary>
    /// EN: Indicates whether JSON operators shared across providers are accepted.
    /// PT-br: Indica se operadores JSON compartilhados entre providers sao aceitos.
    /// </summary>
    public virtual bool AllowsParserCrossDialectJsonOperators => false;

    /// <summary>
    /// EN: Indicates whether INSERT ... SELECT upsert suffixes are accepted.
    /// PT-br: Indica se sufixos de upsert em INSERT ... SELECT sao aceitos.
    /// </summary>
    public virtual bool AllowsParserInsertSelectUpsertSuffix => false;

    /// <summary>
    /// EN: Indicates whether DELETE without FROM compatibility parsing is accepted.
    /// PT-br: Indica se o parsing de compatibilidade para DELETE sem FROM e aceito.
    /// </summary>
    public virtual bool AllowsParserDeleteWithoutFromCompatibility => false;

    /// <summary>
    /// EN: Indicates whether LIMIT/OFFSET compatibility parsing is accepted.
    /// PT-br: Indica se o parsing de compatibilidade para LIMIT/OFFSET e aceito.
    /// </summary>
    public virtual bool AllowsParserLimitOffsetCompatibility => false;

    /// <summary>
    /// EN: Returns the parser cache suffix used to isolate dialect modes that change tokenization or parsing.
    /// PT-br: Retorna o sufixo de cache do parser usado para isolar modos de dialeto que mudam tokenizacao ou parsing.
    /// </summary>
    public virtual string ParserCacheKeySuffix => string.Empty;

    /// <summary>
    /// EN: Indicates whether SQL Server table hints are supported.
    /// PT-br: Indica se table hints do SQL Server sao suportados.
    /// </summary>
    public virtual bool SupportsSqlServerTableHints => false;

    /// <summary>
    /// EN: Indicates whether SQL Server query hints are supported.
    /// PT-br: Indica se query hints do SQL Server sao suportados.
    /// </summary>
    public virtual bool SupportsSqlServerQueryHints => false;

    /// <summary>
    /// EN: Indicates whether MySQL index hints are supported.
    /// PT-br: Indica se index hints do MySQL sao suportados.
    /// </summary>
    public virtual bool SupportsMySqlIndexHints => false;
    /// <summary>
    /// EN: Indicates whether MariaDB-specific scalar and special functions are supported by this dialect.
    /// PT-br: Indica se funcoes escalares e especiais especificas do MariaDB sao suportadas por este dialeto.
    /// </summary>
    public virtual bool SupportsMariaDbFunctions => false;
    public virtual bool SupportsDb2TriggerDdl => false;
    public virtual bool SupportsDb2ProcedureDdl => false;
    public virtual bool SupportsOracleCreateFunctionDdl => false;
    public virtual bool SupportsPostgreSqlCreateFunctionDdl => false;
    public virtual bool SupportsInlineReturnCreateFunctionDdl => false;
    /// <summary>
    /// EN: Checks whether a SQL Server metadata function is supported by this dialect.
    /// PT-br: Verifica se uma funcao de metadados do SQL Server e suportada por este dialeto.
    /// </summary>
    public virtual bool SupportsSqlServerMetadataFunction(string functionName) => false;
    /// <summary>
    /// EN: Checks whether a SQL Server metadata identifier is supported by this dialect.
    /// PT-br: Verifica se um identificador de metadados do SQL Server e suportado por este dialeto.
    /// </summary>
    public virtual bool SupportsSqlServerMetadataIdentifier(string identifier) => false;
    /// <summary>
    /// EN: Checks whether a SQL Server scalar function is supported by this dialect.
    /// PT-br: Verifica se uma funcao escalar do SQL Server e suportada por este dialeto.
    /// </summary>
    public virtual bool SupportsSqlServerScalarFunction(string functionName) => false;
    /// <summary>
    /// EN: Checks whether a SQL Server date function is supported by this dialect.
    /// PT-br: Verifica se uma funcao de data do SQL Server e suportada por este dialeto.
    /// </summary>
    public virtual bool SupportsSqlServerDateFunction(string functionName) => false;
    /// <summary>
    /// EN: Checks whether a SQL Server aggregate function is supported by this dialect.
    /// PT-br: Verifica se uma funcao de agregacao do SQL Server e suportada por este dialeto.
    /// </summary>
    public virtual bool SupportsSqlServerAggregateFunction(string functionName) => false;

    /// <summary>
    /// EN: Indicates whether hash-prefixed identifiers are accepted.
    /// PT-br: Indica se identificadores com hash na frente sao aceitos.
    /// </summary>
    public virtual bool AllowsHashIdentifiers => false;

    /// <summary>
    /// EN: Gets the temporary table scope used by the dialect.
    /// PT-br: Obtém o escopo de tabela temporaria usado pelo dialeto.
    /// </summary>
    public virtual TemporaryTableScope GetTemporaryTableScope(string tableName, string? schemaName)
    {
        _ = schemaName;
        _ = tableName;
        return TemporaryTableScope.None;
    }

    /// <summary>
    /// EN: Tries to map a parsed binary operator token to a dialect operator.
    /// PT-br: Tenta mapear um token de operador binario analisado para o operador do dialeto.
    /// </summary>
    public bool TryMapBinaryOperator(string token, out SqlBinaryOp op)
        => _binOps.TryGetValue(token, out op);

    private bool SupportsRegisteredScalarIdentifier(string functionName)
        => !string.IsNullOrWhiteSpace(functionName)
            && TryGetScalarFunctionDefinition(functionName, out var definition)
            && definition!.AllowsIdentifier;
}
