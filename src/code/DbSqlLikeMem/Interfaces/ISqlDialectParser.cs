namespace DbSqlLikeMem;

/// <summary>
/// EN: Exposes parser-oriented rules for identifiers, strings, keywords, and token scanning.
/// PT-br: Expõe regras de parser para identificadores, strings, palavras-chave e varredura de tokens.
/// </summary>
internal interface ISqlDialectParser
{
    /// <summary>
    /// EN: Indicates whether backtick-delimited identifiers are accepted.
    /// PT-br: Indica se identificadores delimitados por backtick sao aceitos.
    /// </summary>
    bool AllowsBacktickIdentifiers { get; }
    /// <summary>
    /// EN: Indicates whether double-quote-delimited identifiers are accepted.
    /// PT-br: Indica se identificadores delimitados por aspas duplas sao aceitos.
    /// </summary>
    bool AllowsDoubleQuoteIdentifiers { get; }
    /// <summary>
    /// EN: Indicates whether bracket-delimited identifiers are accepted.
    /// PT-br: Indica se identificadores delimitados por colchetes sao aceitos.
    /// </summary>
    bool AllowsBracketIdentifiers { get; }
    /// <summary>
    /// EN: Gets the identifier escape style used by the dialect.
    /// PT-br: Obtém o estilo de escape de identificadores usado pelo dialeto.
    /// </summary>
    SqlIdentifierEscapeStyle IdentifierEscapeStyle { get; }
    /// <summary>
    /// EN: Gets the identifier quote pairs recognized by the dialect.
    /// PT-br: Obtém os pares de aspas de identificador reconhecidos pelo dialeto.
    /// </summary>
    IReadOnlyList<SqlQuotePair> IdentifierQuotes { get; }
    /// <summary>
    /// EN: Gets the string quote pairs recognized by the dialect.
    /// PT-br: Obtém os pares de aspas de string reconhecidos pelo dialeto.
    /// </summary>
    IReadOnlyList<SqlQuotePair> StringQuotes { get; }
    /// <summary>
    /// EN: Tries to resolve an identifier quote pair by its opening character.
    /// PT-br: Tenta resolver um par de aspas de identificador pelo caractere de abertura.
    /// </summary>
    bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair);
    /// <summary>
    /// EN: Tries to resolve a string quote pair by its opening character.
    /// PT-br: Tenta resolver um par de aspas de string pelo caractere de abertura.
    /// </summary>
    bool TryGetStringQuote(char begin, out SqlQuotePair pair);
    /// <summary>
    /// EN: Indicates whether the character starts a string quote sequence.
    /// PT-br: Indica se o caractere inicia uma sequencia de aspas de string.
    /// </summary>
    bool IsStringQuote(char ch);
    /// <summary>
    /// EN: Gets the string escape style used by the dialect.
    /// PT-br: Obtém o estilo de escape de strings usado pelo dialeto.
    /// </summary>
    SqlStringEscapeStyle StringEscapeStyle { get; }
    /// <summary>
    /// EN: Indicates whether dollar-quoted strings are supported.
    /// PT-br: Indica se strings entre dollar quotes sao suportadas.
    /// </summary>
    bool SupportsDollarQuotedStrings { get; }
    /// <summary>
    /// EN: Indicates whether the specified character can start a parameter token.
    /// PT-br: Indica se o caractere informado pode iniciar um token de parametro.
    /// </summary>
    bool IsParameterPrefix(char ch);
    /// <summary>
    /// EN: Indicates whether the specified text is treated as a keyword.
    /// PT-br: Indica se o texto informado e tratado como palavra-chave.
    /// </summary>
    bool IsKeyword(string text);
    /// <summary>
    /// EN: Indicates whether the specified span is treated as a keyword.
    /// PT-br: Indica se o span informado e tratado como palavra-chave.
    /// </summary>
    bool IsKeyword(ReadOnlySpan<char> text);
    /// <summary>
    /// EN: Gets the operator tokens recognized by the dialect.
    /// PT-br: Obtém os tokens de operador reconhecidos pelo dialeto.
    /// </summary>
    IReadOnlyList<string> Operators { get; }
    /// <summary>
    /// EN: Indicates whether hash-prefixed line comments are supported.
    /// PT-br: Indica se comentarios de linha com hash sao suportados.
    /// </summary>
    bool SupportsHashLineComment { get; }
    /// <summary>
    /// EN: Indicates whether the dialect accepts parser-friendly identifiers shared across providers.
    /// PT-br: Indica se o dialeto aceita identificadores amigaveis ao parser compartilhados entre providers.
    /// </summary>
    bool AllowsParserCrossDialectQuotedIdentifiers { get; }
    /// <summary>
    /// EN: Indicates whether the dialect accepts parser-friendly JSON operators shared across providers.
    /// PT-br: Indica se o dialeto aceita operadores JSON amigaveis ao parser compartilhados entre providers.
    /// </summary>
    bool AllowsParserCrossDialectJsonOperators { get; }
}
