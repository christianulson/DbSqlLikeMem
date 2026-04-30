namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes comparison, string, regex, and semantic helper behavior for a SQL dialect.
/// PT: Expõe comportamento de comparacao, strings, regex e helpers semanticos para um dialeto SQL.
/// </summary>
internal interface ISqlDialectSemantics
{
    /// <summary>
    /// EN: Returns the comparison rules used for text values in the current dialect.
    /// PT: Retorna as regras de comparacao usadas para valores de texto no dialeto atual.
    /// </summary>
    StringComparison TextComparison { get; }
    /// <summary>
    /// EN: Indicates whether numeric strings are compared implicitly against numeric values.
    /// PT: Indica se strings numericas sao comparadas implicitamente com valores numericos.
    /// </summary>
    bool SupportsImplicitNumericStringComparison { get; }
    /// <summary>
    /// EN: Indicates whether LIKE comparison is case insensitive by default.
    /// PT: Indica se a comparacao LIKE e case insensitive por padrao.
    /// </summary>
    bool LikeIsCaseInsensitive { get; }
    /// <summary>
    /// EN: Indicates whether IF() is supported by the current dialect/version.
    /// PT: Indica se IF() e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsIfFunction { get; }
    /// <summary>
    /// EN: Indicates whether IIF() is supported by the current dialect/version.
    /// PT: Indica se IIF() e suportada pelo dialeto/versao atual.
    /// </summary>
    bool SupportsIifFunction { get; }
    /// <summary>
    /// EN: Returns the function names that can replace null values in the current dialect.
    /// PT: Retorna os nomes de funcoes que podem substituir valores nulos no dialeto atual.
    /// </summary>
    IReadOnlyCollection<string> NullSubstituteFunctionNames { get; }
    /// <summary>
    /// EN: Returns the temporal function names available in the current dialect.
    /// PT: Retorna os nomes de funcoes temporais disponiveis no dialeto atual.
    /// </summary>
    IReadOnlyDictionary<string, SqlTemporalFunctionKind> TemporalFunctionNames { get; }
    /// <summary>
    /// EN: Returns the temporal function identifiers available in the current dialect.
    /// PT: Retorna os identificadores de funcoes temporais disponiveis no dialeto atual.
    /// </summary>
    IReadOnlyCollection<string> TemporalFunctionIdentifierNames { get; }
    /// <summary>
    /// EN: Returns the temporal function call names available in the current dialect.
    /// PT: Retorna os nomes de chamada de funcoes temporais disponiveis no dialeto atual.
    /// </summary>
    IReadOnlyCollection<string> TemporalFunctionCallNames { get; }
    /// <summary>
    /// EN: Indicates whether string concatenation with the plus operator returns null when any operand is null.
    /// PT: Indica se a concatenacao de strings com o operador mais retorna null quando qualquer operando e null.
    /// </summary>
    bool PlusStringConcatReturnsNullOnNullInput { get; }
    /// <summary>
    /// EN: Indicates whether CONCAT() returns null when any argument is null.
    /// PT: Indica se CONCAT() retorna null quando qualquer argumento e null.
    /// </summary>
    bool ConcatFunctionReturnsNullOnNullInput { get; }
    /// <summary>
    /// EN: Indicates whether the pipe operator (||) is treated as string concatenation.
    /// PT: Indica se o operador pipe (||) e tratado como concatenacao de strings.
    /// </summary>
    bool SupportsPipeConcatOperator { get; }
    /// <summary>
    /// EN: Legacy combined concat null behavior kept for compatibility with older call sites.
    /// PT: Comportamento legado combinado de concat null mantido por compatibilidade com call sites antigos.
    /// </summary>
    bool ConcatReturnsNullOnNullInput { get; }
    /// <summary>
    /// EN: Indicates whether invalid regular expressions evaluate to false.
    /// PT: Indica se expressoes regulares invalidas avaliam como false.
    /// </summary>
    bool RegexInvalidPatternEvaluatesToFalse { get; }
    /// <summary>
    /// EN: Indicates whether regex matching is case insensitive by default.
    /// PT: Indica se a correspondencia de regex e case insensitive por padrao.
    /// </summary>
    bool RegexIsCaseInsensitive { get; }
    /// <summary>
    /// EN: Checks whether two UNION column types are compatible.
    /// PT: Verifica se dois tipos de coluna de UNION sao compativeis.
    /// </summary>
    bool AreUnionColumnTypesCompatible(DbType first, DbType second);
    /// <summary>
    /// EN: Checks whether a type name maps to an integer cast target.
    /// PT: Verifica se um nome de tipo mapeia para um alvo de cast inteiro.
    /// </summary>
    bool IsIntegerCastTypeName(string typeName);
}
