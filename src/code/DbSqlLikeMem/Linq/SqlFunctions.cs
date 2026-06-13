namespace DbSqlLikeMem;

/// <summary>
/// EN: Provides marker methods for LINQ-to-SQL translation of full-text search functions.
/// PT-br: Fornece metodos marcadores para traducao LINQ-to-SQL de funcoes de busca textual.
/// </summary>
public static class SqlFunctions
{
    /// <summary>
    /// EN: Translates to CONTAINS(column, searchTerm) on SQL Server, Oracle, DB2, and Firebird.
    /// PT-br: Traduz para CONTAINS(column, searchTerm) no SQL Server, Oracle, DB2 e Firebird.
    /// </summary>
    public static int Contains(object column, string searchTerm)
        => throw new InvalidOperationException(
            "SqlFunctions.Contains is a marker for LINQ translation and must not be called directly.");

    /// <summary>
    /// EN: Translates to FREETEXT(column, searchTerm) on SQL Server.
    /// PT-br: Traduz para FREETEXT(column, searchTerm) no SQL Server.
    /// </summary>
    public static int FreeText(object column, string searchTerm)
        => throw new InvalidOperationException(
            "SqlFunctions.FreeText is a marker for LINQ translation and must not be called directly.");

    /// <summary>
    /// EN: Translates to MATCH(column) AGAINST(searchTerm) on MySQL and MariaDB.
    /// PT-br: Traduz para MATCH(column) AGAINST(searchTerm) no MySQL e MariaDB.
    /// </summary>
    public static int MatchAgainst(object column, string searchTerm)
        => throw new InvalidOperationException(
            "SqlFunctions.MatchAgainst is a marker for LINQ translation and must not be called directly.");

    /// <summary>
    /// EN: Translates to column MATCH searchTerm on SQLite.
    /// PT-br: Traduz para column MATCH searchTerm no SQLite.
    /// </summary>
    public static bool Match(object column, string searchTerm)
        => throw new InvalidOperationException(
            "SqlFunctions.Match is a marker for LINQ translation and must not be called directly.");

    /// <summary>
    /// EN: Translates to to_tsvector(column) @@ to_tsquery(searchTerm) on PostgreSQL.
    /// PT-br: Traduz para to_tsvector(column) @@ to_tsquery(searchTerm) no PostgreSQL.
    /// </summary>
    public static bool TsQuery(object column, string searchTerm)
        => throw new InvalidOperationException(
            "SqlFunctions.TsQuery is a marker for LINQ translation and must not be called directly.");
}
