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

    public static string BatchConnectionRequired() =>
        Format(nameof(BatchConnectionRequired));

    public static string BatchCommandsMustContainCommand() =>
        Format(nameof(BatchCommandsMustContainCommand));

    public static string BatchCommandsMustNotContainNull() =>
        Format(nameof(BatchCommandsMustNotContainNull));

    public static string BatchCommandTextRequired() =>
        Format(nameof(BatchCommandTextRequired));

    public static string BatchConnectionMustBeOpenCurrentState(object? state) =>
        Format(nameof(BatchConnectionMustBeOpenCurrentState), state);

    public static string MySqlBatchPrepareOnlyTextSupported() =>
        Format(nameof(MySqlBatchPrepareOnlyTextSupported));

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

    public static string NonQueryHandlerCouldNotProcessStatement() =>
        Format(nameof(NonQueryHandlerCouldNotProcessStatement));

    public static string ProcedureNameNotProvided() =>
        Format(nameof(ProcedureNameNotProvided));

    public static string InvalidCallStatement() =>
        Format(nameof(InvalidCallStatement));

    public static string LinqCouldNotExtractTableNameFromExpression(object? expression) =>
        Format(nameof(LinqCouldNotExtractTableNameFromExpression), expression);

    public static string TableAlreadyExists(string tableName) =>
        Format(nameof(TableAlreadyExists), tableName);

    public static string InvalidCreateTableStatement() =>
        Format(nameof(InvalidCreateTableStatement));

    public static string InvalidInsertSelectStatement() =>
        Format(nameof(InvalidInsertSelectStatement));

    public static string ColumnCountDoesNotMatchSelectList() =>
        Format(nameof(ColumnCountDoesNotMatchSelectList));

    public static string MergeCouldNotIdentifyTargetTable() =>
        Format(nameof(MergeCouldNotIdentifyTargetTable));

    public static string MergeUsingClauseNotFound() =>
        Format(nameof(MergeUsingClauseNotFound));

    public static string MergeSourceSelectInvalid() =>
        Format(nameof(MergeSourceSelectInvalid));

    public static string MergeOnClauseNotFound() =>
        Format(nameof(MergeOnClauseNotFound));

    public static string MergeOnConditionNotSupported() =>
        Format(nameof(MergeOnConditionNotSupported));

    public static string MergeCouldNotReadUsingSubquery() =>
        Format(nameof(MergeCouldNotReadUsingSubquery));

    public static string MergeUsingClauseUnbalancedParentheses() =>
        Format(nameof(MergeUsingClauseUnbalancedParentheses));

    public static string UpdateJoinInvalid() =>
        Format(nameof(UpdateJoinInvalid));

    public static string UpdateJoinOnlySimpleEqualityOnSupported() =>
        Format(nameof(UpdateJoinOnlySimpleEqualityOnSupported));

    public static string JoinOnMustReferenceTargetAndSubqueryAliases() =>
        Format(nameof(JoinOnMustReferenceTargetAndSubqueryAliases));

    public static string UpdateJoinOnlySingleSetAssignmentSupported() =>
        Format(nameof(UpdateJoinOnlySingleSetAssignmentSupported));

    public static string UpdateJoinSetMustAssignFromSubqueryToTargetAlias() =>
        Format(nameof(UpdateJoinSetMustAssignFromSubqueryToTargetAlias));

    public static string DeleteJoinInvalid() =>
        Format(nameof(DeleteJoinInvalid));

    public static string DeleteJoinOnlySimpleEqualityOnSupported() =>
        Format(nameof(DeleteJoinOnlySimpleEqualityOnSupported));

    public static string DeleteUsingWhereMustContainJoinEqualityCondition() =>
        Format(nameof(DeleteUsingWhereMustContainJoinEqualityCondition));

    public static string ResolvedConnectionTypeNotCompatible(string connectionType, string providerHint) =>
        Format(nameof(ResolvedConnectionTypeNotCompatible), connectionType, providerHint);

    public static string NoConcreteDbMockImplementationFound(object? assemblyCount) =>
        Format(nameof(NoConcreteDbMockImplementationFound), assemblyCount);

    public static string NoCompatibleDbMockConstructorFound(string dbType) =>
        Format(nameof(NoCompatibleDbMockConstructorFound), dbType);

    public static string CouldNotResolveConnectionFromDbMock(string dbType, string providerHint) =>
        Format(nameof(CouldNotResolveConnectionFromDbMock), dbType, providerHint);

    public static string NoCompatibleConnectionConstructorFound(string connectionType) =>
        Format(nameof(NoCompatibleConnectionConstructorFound), connectionType);

    public static string CannotMaterializeBatchCommandType(string batchCommandType) =>
        Format(nameof(CannotMaterializeBatchCommandType), batchCommandType);

    public static string BatchCommandTypeHasIncompatibleMembers(string batchCommandType) =>
        Format(nameof(BatchCommandTypeHasIncompatibleMembers), batchCommandType);

    public static string TableNotYetDefined(string tableName) =>
        Format(nameof(TableNotYetDefined), tableName);

    public static string ColumnAlreadyExistsInTable(string columnName, string tableName) =>
        Format(nameof(ColumnAlreadyExistsInTable), columnName, tableName);

    public static string SeedRowHasMoreValuesThanColumns(object? valueCount, string tableName, object? columnCount) =>
        Format(nameof(SeedRowHasMoreValuesThanColumns), valueCount, tableName, columnCount);

    public static string ViewAlreadyExists(string viewName) =>
        Format(nameof(ViewAlreadyExists), viewName);

    public static string ViewDoesNotExist(string viewName) =>
        Format(nameof(ViewDoesNotExist), viewName);

    public static string MultipleSchemasRequireExplicitName(object? schemaNames) =>
        Format(nameof(MultipleSchemasRequireExplicitName), schemaNames);

    public static string NoSchemaRegistered() =>
        Format(nameof(NoSchemaRegistered));

    public static string DuplicatePrimaryKeyColumns() =>
        Format(nameof(DuplicatePrimaryKeyColumns));

    public static string IndexAlreadyExists(string indexName) =>
        Format(nameof(IndexAlreadyExists), indexName);

    public static string StringColumnSizeRequired() =>
        Format(nameof(StringColumnSizeRequired));

    public static string DecimalPlacesRequiredForDbType(object? dbType) =>
        Format(nameof(DecimalPlacesRequiredForDbType), dbType);

    public static string DuplicateEnumItems() =>
        Format(nameof(DuplicateEnumItems));

    private static string Format(string key, params object?[] args)
    {
        var template = ResourceManager.GetString(key, CultureInfo.CurrentUICulture)
            ?? ResourceManager.GetString(key, CultureInfo.InvariantCulture)
            ?? key;

        return string.Format(CultureInfo.CurrentCulture, template, args);
    }
}
