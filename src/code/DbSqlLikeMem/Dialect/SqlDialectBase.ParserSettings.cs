using System.Collections.Generic;

namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    /// <summary>
    /// EN: Gets the string escape style used by the dialect.
    /// PT: Obtém o estilo de escape de strings usado pelo dialeto.
    /// </summary>
    public virtual SqlStringEscapeStyle StringEscapeStyle => SqlStringEscapeStyle.doubled_quote;

    /// <summary>
    /// EN: Indicates whether dollar-quoted strings are supported.
    /// PT: Indica se strings entre dollar quotes sao suportadas.
    /// </summary>
    public virtual bool SupportsDollarQuotedStrings => false;

    /// <summary>
    /// EN: Indicates whether the specified character can start a parameter token.
    /// PT: Indica se o caractere informado pode iniciar um token de parametro.
    /// </summary>
    public virtual bool IsParameterPrefix(char ch) => ch is '@' or ':' or '?';

    /// <summary>
    /// EN: Indicates whether the specified text is treated as a keyword.
    /// PT: Indica se o texto informado e tratado como palavra-chave.
    /// </summary>
    public virtual bool IsKeyword(string text)
        => SqlKeywords.IsKeyword(text) || _keywords.Contains(text);

    /// <summary>
    /// EN: Indicates whether the specified span is treated as a keyword.
    /// PT: Indica se o span informado e tratado como palavra-chave.
    /// </summary>
    public virtual bool IsKeyword(ReadOnlySpan<char> text)
    {
        if (SqlKeywords.IsKeyword(text))
            return true;

        foreach (var keyword in _keywords)
        {
            if (text.Equals(keyword, StringComparison.OrdinalIgnoreCase))
                return true;
        }

        return false;
    }

    /// <summary>
    /// EN: Gets the operator tokens recognized by the dialect.
    /// PT: Obtém os tokens de operador reconhecidos pelo dialeto.
    /// </summary>
    public IReadOnlyList<string> Operators { get; }

    /// <summary>
    /// EN: Indicates whether hash-prefixed line comments are supported.
    /// PT: Indica se comentarios de linha com hash sao suportados.
    /// </summary>
    public virtual bool SupportsHashLineComment => false;

    /// <summary>
    /// EN: Indicates whether LIMIT/OFFSET pagination is supported.
    /// PT: Indica se a paginacao LIMIT/OFFSET e suportada.
    /// </summary>
    public virtual bool SupportsLimitOffset => false;
}
