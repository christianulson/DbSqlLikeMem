namespace DbSqlLikeMem.Sqlite;

internal sealed class SqliteDialect : SqlDialectBase
{
    internal const string DialectName = "sqlite";

    internal SqliteDialect(
        int version
        ) : base(
        name: DialectName,
        version: version,
        keywords: ["REGEXP"],
        binOps:
        [
            new KeyValuePair<string, SqlBinaryOp>("AND", SqlBinaryOp.And),
            new KeyValuePair<string, SqlBinaryOp>("OR", SqlBinaryOp.Or),
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
            "<=>",
            "->>", "->",
            ">=", "<=", "<>", "!=", "==",
            "&&", "||"
        ])
    { }

 
    internal const int WithCteMinVersion = 3;
    internal const int MergeMinVersion = int.MaxValue;
    /// <summary>
    /// EN: Gets or sets allows backtick identifiers.
    /// PT: Obtém ou define allows backtick identifiers.
    /// </summary>
    public override bool AllowsBacktickIdentifiers => true;

    /// <summary>
    /// EN: Gets or sets identifier escape style.
    /// PT: Obtém ou define identifier escape style.
    /// </summary>
    public override SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Determines whether the character is treated as a string quote delimiter.
    /// PT: Determina se o caractere é tratado como delimitador de string.
    /// </summary>
    public override bool IsStringQuote(char ch) => ch is '\'' or '"';
    /// <summary>
    /// EN: Gets or sets string escape style.
    /// PT: Obtém ou define string escape style.
    /// </summary>
    public override SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// EN: Gets whether hash line comment is supported.
    /// PT: Obtém se há suporte a hash line comment.
    /// </summary>
    public override bool SupportsHashLineComment => true;


    /// <summary>
    /// EN: Gets whether limit offset is supported.
    /// PT: Obtém se há suporte a limit offset.
    /// </summary>
    public override bool SupportsLimitOffset => true;
    /// <summary>
    /// EN: Enables OFFSET/FETCH compatibility syntax for shared smoke tests.
    /// PT: Habilita sintaxe de compatibilidade OFFSET/FETCH para testes smoke compartilhados.
    /// </summary>
    public override bool SupportsOffsetFetch => true;
    /// <summary>
    /// EN: Gets whether on duplicate key update is supported.
    /// PT: Obtém se há suporte a on duplicate key update.
    /// </summary>
    public override bool SupportsOnDuplicateKeyUpdate => true;
    /// <summary>
    /// EN: Gets whether on conflict clause is supported.
    /// PT: Obtém se há suporte a on conflict clause.
    /// </summary>
    public override bool SupportsOnConflictClause => true;
    /// <summary>
    /// EN: Gets whether order by nulls modifier is supported.
    /// PT: Obtém se há suporte a order by nulls modifier.
    /// </summary>
    public override bool SupportsOrderByNullsModifier => true;

    /// <summary>
    /// EN: Gets whether delete target alias is supported.
    /// PT: Obtém se há suporte a delete target alias.
    /// </summary>
    public override bool SupportsDeleteTargetAlias => false;

    /// <summary>
    /// EN: Gets whether with cte is supported.
    /// PT: Obtém se há suporte a with cte.
    /// </summary>
    public override bool SupportsWithCte => Version >= WithCteMinVersion;
    /// <summary>
    /// EN: Gets whether with recursive is supported.
    /// PT: Obtém se há suporte a with recursive.
    /// </summary>
    public override bool SupportsWithRecursive => Version >= WithCteMinVersion;
    /// <summary>
    /// EN: Gets whether with materialized hint is supported.
    /// PT: Obtém se há suporte a with materialized hint.
    /// </summary>
    public override bool SupportsWithMaterializedHint => true;
    /// <summary>
    /// EN: Gets whether null safe eq is supported.
    /// PT: Obtém se há suporte a null safe eq.
    /// </summary>
    public override bool SupportsNullSafeEq => true;
    /// <summary>
    /// EN: Gets or sets null substitute function names.
    /// PT: Obtém ou define null substitute function names.
    /// </summary>
    public override IReadOnlyCollection<string> NullSubstituteFunctionNames => ["IFNULL"];
    /// <summary>
    /// EN: Gets or sets concat returns null on null input.
    /// PT: Obtém ou define concat returns null on null input.
    /// </summary>
    public override bool ConcatReturnsNullOnNullInput => false;
    /// <summary>
    /// EN: Gets whether json arrow operators is supported.
    /// PT: Obtém se há suporte a json arrow operators.
    /// </summary>
    public override bool SupportsJsonArrowOperators => true;
    /// <summary>
    /// EN: Gets whether json extract function is supported.
    /// PT: Obtém se há suporte a json extract function.
    /// </summary>
    public override bool SupportsJsonExtractFunction => true;

    /// <summary>
    /// EN: Gets or sets text comparison.
    /// PT: Obtém ou define text comparison.
    /// </summary>
    public override StringComparison TextComparison => StringComparison.OrdinalIgnoreCase;

    /// <summary>
    /// EN: Gets whether implicit numeric string comparison is supported.
    /// PT: Obtém se há suporte a implicit numeric string comparison.
    /// </summary>
    public override bool SupportsImplicitNumericStringComparison => true;

    /// <summary>
    /// EN: Represents Supports Date Add Function.
    /// PT: Representa suporte Date Add Function.
    /// </summary>
    public override bool SupportsDateAddFunction(string functionName)
        => functionName.Equals("DATE_ADD", StringComparison.OrdinalIgnoreCase);
}
