namespace DbSqlLikeMem.MySql;

#pragma warning disable CA1010 // Generic interface should also be implemented
/// <summary>
/// EN: Mock data reader for MySQL query results.
/// PT-br: Leitor de dados simulado para resultados MySQL.
/// </summary>
public class MySqlDataReaderMock(
#pragma warning restore CA1010 // Generic interface should also be implemented
    IList<TableResultMock> tables
    ) : DbDataReaderMockBase(tables)
{
    /// <summary>
    /// EN: Gets the value of the specified column, normalizing JSON strings to MySQL canonical format.
    /// PT-br: Obtém o valor da coluna especificada, normalizando strings JSON para o formato canônico do MySQL.
    /// </summary>
    public override object GetValue(int ordinal)
    {
        var value = base.GetValue(ordinal);
        if (value is string text && IsJsonString(text))
            return MySqlValueHelper.NormalizeJsonToMySqlFormat(text);
        return value;
    }

    private static bool IsJsonString(string text)
        => text.Length > 1
            && ((text[0] == '{' && text[^1] == '}')
                || (text[0] == '[' && text[^1] == ']'));
}
