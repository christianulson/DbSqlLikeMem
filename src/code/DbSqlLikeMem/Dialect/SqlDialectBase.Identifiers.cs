namespace DbSqlLikeMem;

internal abstract partial class SqlDialectBase
{
    /// <summary>
    /// EN: Indicates whether backtick-delimited identifiers are accepted.
    /// PT: Indica se identificadores delimitados por crases sao aceitos.
    /// </summary>
    public virtual bool AllowsBacktickIdentifiers => false;

    /// <summary>
    /// EN: Indicates whether double-quote-delimited identifiers are accepted.
    /// PT: Indica se identificadores delimitados por aspas duplas sao aceitos.
    /// </summary>
    public virtual bool AllowsDoubleQuoteIdentifiers => true;

    /// <summary>
    /// EN: Indicates whether bracket-delimited identifiers are accepted.
    /// PT: Indica se identificadores delimitados por colchetes sao aceitos.
    /// </summary>
    public virtual bool AllowsBracketIdentifiers => false;

    /// <summary>
    /// EN: Gets the identifier escape style used by the dialect.
    /// PT: Obtém o estilo de escape de identificadores usado pelo dialeto.
    /// </summary>
    public virtual SqlIdentifierEscapeStyle IdentifierEscapeStyle => SqlIdentifierEscapeStyle.double_quote;

    /// <summary>
    /// EN: Gets the identifier quote pairs supported by the dialect.
    /// PT: Obtem os pares de aspas de identificador suportados pelo dialeto.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> IdentifierQuotes
    {
        get
        {
            // Double quote can be either string or identifier depending on dialect.
            bool dqIsString = IsStringQuote('"');
            var list = new List<SqlQuotePair>(3);
            if (AllowsBacktickIdentifiers) list.Add(new SqlQuotePair('`', '`'));
            if (AllowsBracketIdentifiers) list.Add(new SqlQuotePair('[', ']'));
            if (AllowsDoubleQuoteIdentifiers && !dqIsString) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// EN: Gets the string quote pairs supported by the dialect.
    /// PT: Obtem os pares de aspas de string suportados pelo dialeto.
    /// </summary>
    public virtual IReadOnlyList<SqlQuotePair> StringQuotes
    {
        get
        {
            var list = new List<SqlQuotePair>(2);
            if (IsStringQuote('\'')) list.Add(new SqlQuotePair('\'', '\''));
            if (IsStringQuote('"')) list.Add(new SqlQuotePair('"', '"'));
            return list;
        }
    }

    /// <summary>
    /// EN: Tries to resolve an identifier quote pair by its opening character.
    /// PT: Tenta resolver um par de aspas de identificador pelo caractere de abertura.
    /// </summary>
    public virtual bool TryGetIdentifierQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in IdentifierQuotes)
        {
            if (p.Begin == begin)
            {
                pair = p;
                return true;
            }
        }

        pair = default;
        return false;
    }

    /// <summary>
    /// EN: Tries to resolve a string quote pair by its opening character.
    /// PT: Tenta resolver um par de aspas de string pelo caractere de abertura.
    /// </summary>
    public virtual bool TryGetStringQuote(char begin, out SqlQuotePair pair)
    {
        foreach (var p in StringQuotes)
        {
            if (p.Begin == begin)
            {
                pair = p;
                return true;
            }
        }

        pair = default;
        return false;
    }

    /// <summary>
    /// EN: Indicates whether the character starts a string quote sequence.
    /// PT: Indica se o caractere inicia uma sequencia de aspas de string.
    /// </summary>
    public virtual bool IsStringQuote(char ch) => ch == '\'';
}
