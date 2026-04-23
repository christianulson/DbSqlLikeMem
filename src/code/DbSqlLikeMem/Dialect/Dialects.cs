using DbSqlLikeMem.Dialect;

namespace DbSqlLikeMem;

/// <summary>
/// EN: String escaping styles supported by the parser.
/// PT: Estilos de escape de string suportados pelo parser.
/// </summary>
internal enum SqlStringEscapeStyle { backslash, doubled_quote }

/// <summary>
/// EN: Identifier escaping styles supported by the parser.
/// PT: Estilos de escape de identificador suportados pelo parser.
/// </summary>
internal enum SqlIdentifierEscapeStyle { double_quote, backtick, bracket }

internal readonly record struct SqlQuotePair(char Begin, char End);

/// <summary>
/// EN: Defines the kind of temporal function evaluated.
/// PT: Define o tipo de funcao temporal avaliada.
/// </summary>
public enum SqlTemporalFunctionKind
{
    /// <summary>
    /// EN: Date value.
    /// PT: Valor de data.
    /// </summary>
    Date,
    /// <summary>
    /// EN: Time value.
    /// PT: Valor de tempo.
    /// </summary>
    Time,
    /// <summary>
    /// EN: DateTime value.
    /// PT: Valor de data e hora.
    /// </summary>
    DateTime,
    /// <summary>
    /// EN: DateTimeOffset value.
    /// PT: Valor de data e hora com fuso horario.
    /// </summary>
    DateTimeOffset
}

internal abstract partial class SqlDialectBase : ISqlDialect
{
}
