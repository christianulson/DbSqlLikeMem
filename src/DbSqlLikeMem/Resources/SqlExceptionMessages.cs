using System.Globalization;
using System.Resources;

namespace DbSqlLikeMem.Resources;

public static class SqlExceptionMessages
{
    private static readonly ResourceManager ResourceManager =
        new("DbSqlLikeMem.Resources.SqlExceptionMessages", typeof(SqlExceptionMessages).Assembly);

    public static string DuplicateKey(object? value, string key) =>
        Format(nameof(DuplicateKey), value, key);

    public static string UnknownColumn(string column) =>
        Format(nameof(UnknownColumn), column);

    public static string ColumnCannotBeNull(string column) =>
        Format(nameof(ColumnCannotBeNull), column);

    public static string ForeignKeyFails(string column, string referencedTable) =>
        Format(nameof(ForeignKeyFails), column, referencedTable);

    public static string ReferencedRow(string table) =>
        Format(nameof(ReferencedRow), table);


    public static string ParameterAlreadyDefined(string parameterName) =>
        Format(nameof(ParameterAlreadyDefined), parameterName);

    public static string ParameterNotFoundInCollection(string parameterName) =>
        Format(nameof(ParameterNotFoundInCollection), parameterName);

    public static string InvalidCreateTemporaryTableStatement() =>
        Format(nameof(InvalidCreateTemporaryTableStatement));

    public static string InvalidCreateViewStatement() =>
        Format(nameof(InvalidCreateViewStatement));

    public static string InvalidDeleteExpectedFromKeyword() =>
        Format(nameof(InvalidDeleteExpectedFromKeyword));

    public static string UseExecuteReaderForSelect() =>
        Format(nameof(UseExecuteReaderForSelect));

    public static string UseExecuteReaderForSelectUnion() =>
        Format(nameof(UseExecuteReaderForSelectUnion));

    public static string ExecuteReaderWithoutSelectQuery() =>
        Format(nameof(ExecuteReaderWithoutSelectQuery));

    public static string InvalidDropViewStatement() =>
        Format(nameof(InvalidDropViewStatement));

    public static string ParameterNotFound(string parameterName) =>
        Format(nameof(ParameterNotFound), parameterName);

    public static string ColumnDoesNotAcceptNull() =>
        Format(nameof(ColumnDoesNotAcceptNull));

    public static string DataTooLongForColumn(string column) =>
        Format(nameof(DataTooLongForColumn), column);

    public static string DataTruncatedForColumn(string column) =>
        Format(nameof(DataTruncatedForColumn), column);

    public static string DapperMethodOverloadNotFound(string methodName) =>
        Format(nameof(DapperMethodOverloadNotFound), methodName);

    public static string DapperAddTypeMapMethodNotFound() =>
        Format(nameof(DapperAddTypeMapMethodNotFound));

    public static string DapperRuntimeNotFound() =>
        Format(nameof(DapperRuntimeNotFound));

    private static string Format(string key, params object?[] args)
    {
        var template = ResourceManager.GetString(key, CultureInfo.CurrentUICulture)
            ?? ResourceManager.GetString(key, CultureInfo.InvariantCulture)
            ?? key;

        return string.Format(CultureInfo.CurrentCulture, template, args);
    }
}
