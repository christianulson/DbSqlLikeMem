namespace DbSqlLikeMem.MySql;

/// <summary>
/// EN: SQL dialect implementation for the MySQL family supported by this provider.
/// PT: Implementação de dialeto SQL para a família MySQL suportada por este provedor.
/// </summary>
internal partial class MySqlDialect : SqlDialectBase
{
    internal const string DialectName = "mysql";

    /// <summary>
    /// EN: Initializes the MySQL dialect for the requested simulated version.
    /// PT: Inicializa o dialeto MySQL para a versão simulada informada.
    /// </summary>
    /// <param name="version">EN: Simulated MySQL version. PT: Versão simulada do MySQL.</param>
    internal MySqlDialect(
        int version
        ) : this(DialectName, version)
    {
    }

    //TODO: implementar ANSI_QUOTES
    //TODO: implementar PIPES_AS_CONCAT

    /// <summary>
    /// EN: Initializes a MySQL-family dialect with a custom provider name and version.
    /// PT: Inicializa um dialeto da família MySQL com nome de provedor e versão customizados.
    /// </summary>
    /// <param name="dialectName">EN: Dialect/provider name exposed to the parser and executor. PT: Nome do dialeto/provedor exposto ao parser e executor.</param>
    /// <param name="version">EN: Simulated dialect version. PT: Versão simulada do dialeto.</param>
    protected MySqlDialect(
        string dialectName,
        int version
        ) : base(
        name: dialectName,
        version: version,
        keywords: ["REGEXP"],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.AND, SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>("&&", SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>(SqlConst.OR, SqlBinaryOp.Or),
            new KeyValuePair<string, SqlBinaryOp>("||", SqlBinaryOp.Or), 
            new KeyValuePair<string, SqlBinaryOp>("=", SqlBinaryOp.Eq),
            new KeyValuePair<string, SqlBinaryOp>("<>", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>("!=", SqlBinaryOp.Neq),
            new KeyValuePair<string, SqlBinaryOp>(">", SqlBinaryOp.Greater),
            new KeyValuePair<string, SqlBinaryOp>(">=", SqlBinaryOp.GreaterOrEqual),
            new KeyValuePair<string, SqlBinaryOp>("<", SqlBinaryOp.Less),
            new KeyValuePair<string, SqlBinaryOp>("<=", SqlBinaryOp.LessOrEqual),
            new KeyValuePair<string, SqlBinaryOp>("<=>", SqlBinaryOp.NullSafeEq),
        ],
        operators:
        [
            "<=>", "->>", "->",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    {
        RegisterScalarFunctions(version);
        RegisterTableFunctions(version);
        SqlSharedWindowFunctionRegistry.Register(this);
    }

    partial void RegisterScalarFunctions(int version);

    partial void RegisterTableFunctions(int version);

    internal const int WithCteMinVersion = 80;
    internal const int MergeMinVersion = int.MaxValue;
    internal const int WindowFunctionsMinVersion = 80;
    internal const int JsonFunctionsMinVersion = 57;
    internal const int JsonArrowOperatorsMinVersion = 57;
    /// <summary>
    /// EN: Gets or sets AllowsBacktickIdentifiers.
    /// PT: Obtém ou define AllowsBacktickIdentifiers.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;
    /// <summary>
    /// EN: Gets or sets AllowsDoubleQuoteIdentifiers.
    /// PT: Obtém ou define AllowsDoubleQuoteIdentifiers.
    /// </summary>
    public override bool AllowsDoubleQuoteIdentifiers => false; // keep tokenizer behavior: " as string
    /// <summary>
    /// EN: Gets or sets IdentifierEscapeStyle.
    /// PT: Obtém ou define IdentifierEscapeStyle.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.backtick;

    /// <summary>
    /// EN: Implements IsStringQuote.
    /// PT: Implementa IsStringQuote.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    /// <summary>
    /// EN: Gets or sets StringEscapeStyle.
    /// PT: Obtém ou define StringEscapeStyle.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.backslash;
    /// <summary>
    /// EN: Uses case-insensitive textual comparisons in the in-memory executor for deterministic tests.
    /// PT: Usa comparações textuais case-insensitive no executor em memória para testes determinísticos.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Enables implicit numeric/string comparison only when both values are numeric-convertible.
    /// PT: Habilita comparação implícita numérica/string apenas quando ambos os valores são conversíveis para número.
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Keeps LIKE case-insensitive by default in the mock provider.
    /// PT: Mantém LIKE case-insensitive por padrão no provedor simulado.
    /// </summary>
    public override bool LikeIsCaseInsensitive => true;

    public override bool RegexIsCaseInsensitive => true;

    public override char? LikeDefaultEscapeCharacter => '\\';


    /// <summary>
    /// EN: Gets or sets SupportsHashLineComment.
    /// PT: Obtém ou define SupportsHashLineComment.
    /// </summary>
    public override bool SupportsHashLineComment => true;


    /// <summary>
    /// EN: Gets or sets SupportsLimitOffset.
    /// PT: Obtém ou define SupportsLimitOffset.
    /// </summary>
    public override bool SupportsLimitOffset => true;
    /// <summary>
    /// EN: Enables SQL Server-style OFFSET/FETCH syntax as parser compatibility mode.
    /// PT: Habilita sintaxe OFFSET/FETCH estilo SQL Server como modo de compatibilidade do parser.
    /// </summary>
    public override bool SupportsOffsetFetch => false;
    /// <summary>
    /// EN: Gets or sets SupportsOnDuplicateKeyUpdate.
    /// PT: Obtém ou define SupportsOnDuplicateKeyUpdate.
    /// </summary>
    public override bool SupportsOnDuplicateKeyUpdate => true;

    /// <summary>
    /// EN: Gets or sets SupportsDeleteWithoutFrom.
    /// PT: Obtém ou define SupportsDeleteWithoutFrom.
    /// </summary>
    public override bool SupportsDeleteWithoutFrom => true; // MySQL accepts DELETE [FROM] tbl
    public override bool SupportsUpdateJoinFromSubquerySyntax => true;
    public override bool SupportsDeleteTargetFromJoinSubquerySyntax => true;

    /// <summary>
    /// EN: Gets or sets SupportsWithCte.
    /// PT: Obtém ou define SupportsWithCte.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;

    /// <inheritdoc />
    public override bool SupportsAlterTableAddColumn => true;
    public override bool SupportsFunctionDdl => true;
    public override bool SupportsInlineReturnCreateFunctionDdl => true;

    /// <summary>
    /// EN: Indicates whether recursive CTE syntax is supported by the configured MySQL version.
    /// PT: Indica se a sintaxe de CTE recursiva é suportada pela versão configurada do MySQL.
    /// </summary>
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window functions are supported by the configured MySQL version.
    /// PT: Indica se funções de janela SQL são suportadas pela versão configurada do MySQL.
    /// </summary>
    public override bool SupportsWindowFunctions => Version >= WindowFunctionsMinVersion;

    /// <summary>
    /// EN: Indicates whether SQL window frame clauses are supported by the configured version.
    /// PT: Indica se cláusulas de frame de janela SQL são suportadas pela versão configurada.
    /// </summary>
    public override bool SupportsWindowFrameClause => Version >= WindowFunctionsMinVersion;
    /// <summary>
    /// EN: Gets or sets SupportsNullSafeEq.
    /// PT: Obtém ou define SupportsNullSafeEq.
    /// </summary>
    public override bool SupportsNullSafeEq => true;

    /// <summary>
    /// EN: Gets the null-substitution function names recognized by this dialect.
    /// PT: Obtém os nomes de funções de substituição de nulos reconhecidos por este dialeto.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["IFNULL"];

    /// <summary>
    /// EN: Indicates whether string concatenation returns <c>NULL</c> when any operand is <c>NULL</c>.
    /// PT: Indica se a concatenação de strings retorna <c>NULL</c> quando qualquer operando é <c>NULL</c>.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => true;
    /// <summary>
    /// EN: Gets or sets SupportsJsonArrowOperators.
    /// PT: Obtém ou define SupportsJsonArrowOperators.
    /// </summary>
    public override bool SupportsJsonArrowOperators => Version >= JsonArrowOperatorsMinVersion;

    /// <summary>
    /// EN: Indicates whether parser-level cross-dialect JSON operators are accepted for compatibility.
    /// PT: Indica se operadores JSON entre dialetos são aceitos pelo parser para compatibilidade.
    /// </summary>
    public override bool AllowsParserCrossDialectJsonOperators => Version >= JsonArrowOperatorsMinVersion;

    /// <summary>
    /// EN: Indicates whether MySQL index hints are supported in SQL generation.
    /// PT: Indica se hints de índice do MySQL são suportados na geração de SQL.
    /// </summary>
    public override bool SupportsMySqlIndexHints => true;

    public override bool SupportsAggregateOrderByForStringAggregates => true;

    public override bool SupportsAggregateSeparatorKeywordForStringAggregates => true;

    public override bool SupportsMatchAgainstPredicate => true;

    public override bool SupportsSqlCalcFoundRowsModifier => true;

    public override int GetInsertUpsertAffectedRowCount(int insertedCount, int updatedCount)
        => insertedCount + (updatedCount * 2);
}
