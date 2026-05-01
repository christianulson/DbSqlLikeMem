using System.Resources;

namespace DbSqlLikeMem.Resources;

/// <summary>
/// EN: Provides localized SQL exception messages.
/// PT-br: Fornece mensagens localizadas de excecao SQL.
/// </summary>
public static class SqlExceptionMessages
{
    private static readonly ResourceManager ResourceManager =
        new("DbSqlLikeMem.Resources.SqlExceptionMessages", typeof(SqlExceptionMessages).Assembly);
    /// <summary>
    /// EN: Gets the formatted exception message for DuplicateKey.
    /// PT-br: Obtem a mensagem de excecao formatada para DuplicateKey.
    /// </summary>
    public static string DuplicateKey(object? value, string key) =>
        Format(nameof(DuplicateKey), value, key);
    /// <summary>
    /// EN: Gets the formatted exception message for UnknownColumn.
    /// PT-br: Obtem a mensagem de excecao formatada para UnknownColumn.
    /// </summary>
    public static string UnknownColumn(string column) =>
        Format(nameof(UnknownColumn), column);
    /// <summary>
    /// EN: Gets the formatted exception message for ColumnCannotBeNull.
    /// PT-br: Obtem a mensagem de excecao formatada para ColumnCannotBeNull.
    /// </summary>
    public static string ColumnCannotBeNull(string column) =>
        Format(nameof(ColumnCannotBeNull), column);
    /// <summary>
    /// EN: Gets the formatted exception message for ForeignKeyFails.
    /// PT-br: Obtem a mensagem de excecao formatada para ForeignKeyFails.
    /// </summary>
    public static string ForeignKeyFails(string column, string referencedTable) =>
        Format(nameof(ForeignKeyFails), column, referencedTable);
    /// <summary>
    /// EN: Gets the formatted exception message for ReferencedRow.
    /// PT-br: Obtem a mensagem de excecao formatada para ReferencedRow.
    /// </summary>
    public static string ReferencedRow(string table) =>
        Format(nameof(ReferencedRow), table);
    /// <summary>
    /// EN: Gets the formatted exception message for ParameterAlreadyDefined.
    /// PT-br: Obtem a mensagem de excecao formatada para ParameterAlreadyDefined.
    /// </summary>
    public static string ParameterAlreadyDefined(string parameterName) =>
        Format(nameof(ParameterAlreadyDefined), parameterName);
    /// <summary>
    /// EN: Gets the formatted exception message for ParameterNotFoundInCollection.
    /// PT-br: Obtem a mensagem de excecao formatada para ParameterNotFoundInCollection.
    /// </summary>
    public static string ParameterNotFoundInCollection(string parameterName) =>
        Format(nameof(ParameterNotFoundInCollection), parameterName);
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidCreateTemporaryTableStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidCreateTemporaryTableStatement.
    /// </summary>
    public static string InvalidCreateTemporaryTableStatement() =>
        Format(nameof(InvalidCreateTemporaryTableStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidCreateViewStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidCreateViewStatement.
    /// </summary>
    public static string InvalidCreateViewStatement() =>
        Format(nameof(InvalidCreateViewStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidDeleteExpectedFromKeyword.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidDeleteExpectedFromKeyword.
    /// </summary>
    public static string InvalidDeleteExpectedFromKeyword() =>
        Format(nameof(InvalidDeleteExpectedFromKeyword));
    /// <summary>
    /// EN: Gets the formatted exception message for UseExecuteReaderForSelect.
    /// PT-br: Obtem a mensagem de excecao formatada para UseExecuteReaderForSelect.
    /// </summary>
    public static string UseExecuteReaderForSelect() =>
        Format(nameof(UseExecuteReaderForSelect));
    /// <summary>
    /// EN: Gets the formatted exception message for UseExecuteReaderForSelectUnion.
    /// PT-br: Obtem a mensagem de excecao formatada para UseExecuteReaderForSelectUnion.
    /// </summary>
    public static string UseExecuteReaderForSelectUnion() =>
        Format(nameof(UseExecuteReaderForSelectUnion));
    /// <summary>
    /// EN: Gets the formatted exception message for ExecuteReaderWithoutSelectQuery.
    /// PT-br: Obtem a mensagem de excecao formatada para ExecuteReaderWithoutSelectQuery.
    /// </summary>
    public static string ExecuteReaderWithoutSelectQuery() =>
        Format(nameof(ExecuteReaderWithoutSelectQuery));
    /// <summary>
    /// EN: Gets the formatted exception message for BatchConnectionRequired.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchConnectionRequired.
    /// </summary>
    public static string BatchConnectionRequired() =>
        Format(nameof(BatchConnectionRequired));
    /// <summary>
    /// EN: Gets the formatted exception message for BatchCommandsMustContainCommand.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchCommandsMustContainCommand.
    /// </summary>
    public static string BatchCommandsMustContainCommand() =>
        Format(nameof(BatchCommandsMustContainCommand));
    /// <summary>
    /// EN: Gets the formatted exception message for BatchCommandsMustNotContainNull.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchCommandsMustNotContainNull.
    /// </summary>
    public static string BatchCommandsMustNotContainNull() =>
        Format(nameof(BatchCommandsMustNotContainNull));
    /// <summary>
    /// EN: Gets the formatted exception message for BatchCommandTextRequired.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchCommandTextRequired.
    /// </summary>
    public static string BatchCommandTextRequired() =>
        Format(nameof(BatchCommandTextRequired));
    /// <summary>
    /// EN: Gets the formatted exception message for BatchConnectionMustBeOpenCurrentState.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchConnectionMustBeOpenCurrentState.
    /// </summary>
    public static string BatchConnectionMustBeOpenCurrentState(object? state) =>
        Format(nameof(BatchConnectionMustBeOpenCurrentState), state);
    /// <summary>
    /// EN: Gets the formatted exception message for MySqlBatchPrepareOnlyTextSupported.
    /// PT-br: Obtem a mensagem de excecao formatada para MySqlBatchPrepareOnlyTextSupported.
    /// </summary>
    public static string MySqlBatchPrepareOnlyTextSupported() =>
        Format(nameof(MySqlBatchPrepareOnlyTextSupported));
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidDropViewStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidDropViewStatement.
    /// </summary>
    public static string InvalidDropViewStatement() =>
        Format(nameof(InvalidDropViewStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for ParameterNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para ParameterNotFound.
    /// </summary>
    public static string ParameterNotFound(string parameterName) =>
        Format(nameof(ParameterNotFound), parameterName);
    /// <summary>
    /// EN: Gets the formatted exception message for ColumnDoesNotAcceptNull.
    /// PT-br: Obtem a mensagem de excecao formatada para ColumnDoesNotAcceptNull.
    /// </summary>
    public static string ColumnDoesNotAcceptNull() =>
        Format(nameof(ColumnDoesNotAcceptNull));
    /// <summary>
    /// EN: Gets the formatted exception message for DataTooLongForColumn.
    /// PT-br: Obtem a mensagem de excecao formatada para DataTooLongForColumn.
    /// </summary>
    public static string DataTooLongForColumn(string column) =>
        Format(nameof(DataTooLongForColumn), column);
    /// <summary>
    /// EN: Gets the formatted exception message for DataTruncatedForColumn.
    /// PT-br: Obtem a mensagem de excecao formatada para DataTruncatedForColumn.
    /// </summary>
    public static string DataTruncatedForColumn(string column) =>
        Format(nameof(DataTruncatedForColumn), column);
    /// <summary>
    /// EN: Gets the formatted exception message for DapperMethodOverloadNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para DapperMethodOverloadNotFound.
    /// </summary>
    public static string DapperMethodOverloadNotFound(string methodName) =>
        Format(nameof(DapperMethodOverloadNotFound), methodName);
    /// <summary>
    /// EN: Gets the formatted exception message for DapperAddTypeMapMethodNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para DapperAddTypeMapMethodNotFound.
    /// </summary>
    public static string DapperAddTypeMapMethodNotFound() =>
        Format(nameof(DapperAddTypeMapMethodNotFound));
    /// <summary>
    /// EN: Gets the formatted exception message for DapperRuntimeNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para DapperRuntimeNotFound.
    /// </summary>
    public static string DapperRuntimeNotFound() =>
        Format(nameof(DapperRuntimeNotFound));
    /// <summary>
    /// EN: Gets the formatted exception message for NonQueryHandlerCouldNotProcessStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para NonQueryHandlerCouldNotProcessStatement.
    /// </summary>
    public static string NonQueryHandlerCouldNotProcessStatement() =>
        Format(nameof(NonQueryHandlerCouldNotProcessStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for ProcedureNameNotProvided.
    /// PT-br: Obtem a mensagem de excecao formatada para ProcedureNameNotProvided.
    /// </summary>
    public static string ProcedureNameNotProvided() =>
        Format(nameof(ProcedureNameNotProvided));
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidCallStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidCallStatement.
    /// </summary>
    public static string InvalidCallStatement() =>
        Format(nameof(InvalidCallStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for LinqCouldNotExtractTableNameFromExpression.
    /// PT-br: Obtem a mensagem de excecao formatada para LinqCouldNotExtractTableNameFromExpression.
    /// </summary>
    public static string LinqCouldNotExtractTableNameFromExpression(object? expression) =>
        Format(nameof(LinqCouldNotExtractTableNameFromExpression), expression);
    /// <summary>
    /// EN: Gets the formatted exception message for TableAlreadyExists.
    /// PT-br: Obtem a mensagem de excecao formatada para TableAlreadyExists.
    /// </summary>
    public static string TableAlreadyExists(string tableName) =>
        Format(nameof(TableAlreadyExists), tableName);
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidCreateTableStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidCreateTableStatement.
    /// </summary>
    public static string InvalidCreateTableStatement() =>
        Format(nameof(InvalidCreateTableStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for InvalidInsertSelectStatement.
    /// PT-br: Obtem a mensagem de excecao formatada para InvalidInsertSelectStatement.
    /// </summary>
    public static string InvalidInsertSelectStatement() =>
        Format(nameof(InvalidInsertSelectStatement));
    /// <summary>
    /// EN: Gets the formatted exception message for ColumnCountDoesNotMatchSelectList.
    /// PT-br: Obtem a mensagem de excecao formatada para ColumnCountDoesNotMatchSelectList.
    /// </summary>
    public static string ColumnCountDoesNotMatchSelectList() =>
        Format(nameof(ColumnCountDoesNotMatchSelectList));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeCouldNotIdentifyTargetTable.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeCouldNotIdentifyTargetTable.
    /// </summary>
    public static string MergeCouldNotIdentifyTargetTable() =>
        Format(nameof(MergeCouldNotIdentifyTargetTable));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeUsingClauseNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeUsingClauseNotFound.
    /// </summary>
    public static string MergeUsingClauseNotFound() =>
        Format(nameof(MergeUsingClauseNotFound));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeSourceSelectInvalid.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeSourceSelectInvalid.
    /// </summary>
    public static string MergeSourceSelectInvalid() =>
        Format(nameof(MergeSourceSelectInvalid));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeOnClauseNotFound.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeOnClauseNotFound.
    /// </summary>
    public static string MergeOnClauseNotFound() =>
        Format(nameof(MergeOnClauseNotFound));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeOnConditionNotSupported.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeOnConditionNotSupported.
    /// </summary>
    public static string MergeOnConditionNotSupported() =>
        Format(nameof(MergeOnConditionNotSupported));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeCouldNotReadUsingSubquery.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeCouldNotReadUsingSubquery.
    /// </summary>
    public static string MergeCouldNotReadUsingSubquery() =>
        Format(nameof(MergeCouldNotReadUsingSubquery));
    /// <summary>
    /// EN: Gets the formatted exception message for MergeUsingClauseUnbalancedParentheses.
    /// PT-br: Obtem a mensagem de excecao formatada para MergeUsingClauseUnbalancedParentheses.
    /// </summary>
    public static string MergeUsingClauseUnbalancedParentheses() =>
        Format(nameof(MergeUsingClauseUnbalancedParentheses));
    /// <summary>
    /// EN: Gets the formatted exception message for UpdateJoinInvalid.
    /// PT-br: Obtem a mensagem de excecao formatada para UpdateJoinInvalid.
    /// </summary>
    public static string UpdateJoinInvalid() =>
        Format(nameof(UpdateJoinInvalid));
    /// <summary>
    /// EN: Gets the formatted exception message for UpdateJoinOnlySimpleEqualityOnSupported.
    /// PT-br: Obtem a mensagem de excecao formatada para UpdateJoinOnlySimpleEqualityOnSupported.
    /// </summary>
    public static string UpdateJoinOnlySimpleEqualityOnSupported() =>
        Format(nameof(UpdateJoinOnlySimpleEqualityOnSupported));
    /// <summary>
    /// EN: Gets the formatted exception message for JoinOnMustReferenceTargetAndSubqueryAliases.
    /// PT-br: Obtem a mensagem de excecao formatada para JoinOnMustReferenceTargetAndSubqueryAliases.
    /// </summary>
    public static string JoinOnMustReferenceTargetAndSubqueryAliases() =>
        Format(nameof(JoinOnMustReferenceTargetAndSubqueryAliases));
    /// <summary>
    /// EN: Gets the formatted exception message for UpdateJoinOnlySingleSetAssignmentSupported.
    /// PT-br: Obtem a mensagem de excecao formatada para UpdateJoinOnlySingleSetAssignmentSupported.
    /// </summary>
    public static string UpdateJoinOnlySingleSetAssignmentSupported() =>
        Format(nameof(UpdateJoinOnlySingleSetAssignmentSupported));
    /// <summary>
    /// EN: Gets the formatted exception message for UpdateJoinSetMustAssignFromSubqueryToTargetAlias.
    /// PT-br: Obtem a mensagem de excecao formatada para UpdateJoinSetMustAssignFromSubqueryToTargetAlias.
    /// </summary>
    public static string UpdateJoinSetMustAssignFromSubqueryToTargetAlias() =>
        Format(nameof(UpdateJoinSetMustAssignFromSubqueryToTargetAlias));
    /// <summary>
    /// EN: Gets the formatted exception message for DeleteJoinInvalid.
    /// PT-br: Obtem a mensagem de excecao formatada para DeleteJoinInvalid.
    /// </summary>
    public static string DeleteJoinInvalid() =>
        Format(nameof(DeleteJoinInvalid));
    /// <summary>
    /// EN: Gets the formatted exception message for DeleteJoinOnlySimpleEqualityOnSupported.
    /// PT-br: Obtem a mensagem de excecao formatada para DeleteJoinOnlySimpleEqualityOnSupported.
    /// </summary>
    public static string DeleteJoinOnlySimpleEqualityOnSupported() =>
        Format(nameof(DeleteJoinOnlySimpleEqualityOnSupported));
    /// <summary>
    /// EN: Gets the formatted exception message for DeleteUsingWhereMustContainJoinEqualityCondition.
    /// PT-br: Obtem a mensagem de excecao formatada para DeleteUsingWhereMustContainJoinEqualityCondition.
    /// </summary>
    public static string DeleteUsingWhereMustContainJoinEqualityCondition() =>
        Format(nameof(DeleteUsingWhereMustContainJoinEqualityCondition));
    /// <summary>
    /// EN: Gets the formatted exception message for ResolvedConnectionTypeNotCompatible.
    /// PT-br: Obtem a mensagem de excecao formatada para ResolvedConnectionTypeNotCompatible.
    /// </summary>
    public static string ResolvedConnectionTypeNotCompatible(string connectionType, string providerHint) =>
        Format(nameof(ResolvedConnectionTypeNotCompatible), connectionType, providerHint);
    /// <summary>
    /// EN: Gets the formatted exception message for NoConcreteDbMockImplementationFound.
    /// PT-br: Obtem a mensagem de excecao formatada para NoConcreteDbMockImplementationFound.
    /// </summary>
    public static string NoConcreteDbMockImplementationFound(object? assemblyCount) =>
        Format(nameof(NoConcreteDbMockImplementationFound), assemblyCount);
    /// <summary>
    /// EN: Gets the formatted exception message for NoCompatibleDbMockConstructorFound.
    /// PT-br: Obtem a mensagem de excecao formatada para NoCompatibleDbMockConstructorFound.
    /// </summary>
    public static string NoCompatibleDbMockConstructorFound(string dbType) =>
        Format(nameof(NoCompatibleDbMockConstructorFound), dbType);
    /// <summary>
    /// EN: Gets the formatted exception message for CouldNotResolveConnectionFromDbMock.
    /// PT-br: Obtem a mensagem de excecao formatada para CouldNotResolveConnectionFromDbMock.
    /// </summary>
    public static string CouldNotResolveConnectionFromDbMock(string dbType, string providerHint) =>
        Format(nameof(CouldNotResolveConnectionFromDbMock), dbType, providerHint);
    /// <summary>
    /// EN: Gets the formatted exception message for NoCompatibleConnectionConstructorFound.
    /// PT-br: Obtem a mensagem de excecao formatada para NoCompatibleConnectionConstructorFound.
    /// </summary>
    public static string NoCompatibleConnectionConstructorFound(string connectionType) =>
        Format(nameof(NoCompatibleConnectionConstructorFound), connectionType);
    /// <summary>
    /// EN: Gets the formatted exception message for CannotMaterializeBatchCommandType.
    /// PT-br: Obtem a mensagem de excecao formatada para CannotMaterializeBatchCommandType.
    /// </summary>
    public static string CannotMaterializeBatchCommandType(string batchCommandType) =>
        Format(nameof(CannotMaterializeBatchCommandType), batchCommandType);
    /// <summary>
    /// EN: Gets the formatted exception message for BatchCommandTypeHasIncompatibleMembers.
    /// PT-br: Obtem a mensagem de excecao formatada para BatchCommandTypeHasIncompatibleMembers.
    /// </summary>
    public static string BatchCommandTypeHasIncompatibleMembers(string batchCommandType) =>
        Format(nameof(BatchCommandTypeHasIncompatibleMembers), batchCommandType);
    /// <summary>
    /// EN: Gets the formatted exception message for TableNotYetDefined.
    /// PT-br: Obtem a mensagem de excecao formatada para TableNotYetDefined.
    /// </summary>
    public static string TableNotYetDefined(string tableName) =>
        Format(nameof(TableNotYetDefined), tableName);
    /// <summary>
    /// EN: Gets the formatted exception message for ColumnAlreadyExistsInTable.
    /// PT-br: Obtem a mensagem de excecao formatada para ColumnAlreadyExistsInTable.
    /// </summary>
    public static string ColumnAlreadyExistsInTable(string columnName, string tableName) =>
        Format(nameof(ColumnAlreadyExistsInTable), columnName, tableName);
    /// <summary>
    /// EN: Gets the formatted exception message for SeedRowHasMoreValuesThanColumns.
    /// PT-br: Obtem a mensagem de excecao formatada para SeedRowHasMoreValuesThanColumns.
    /// </summary>
    public static string SeedRowHasMoreValuesThanColumns(object? valueCount, string tableName, object? columnCount) =>
        Format(nameof(SeedRowHasMoreValuesThanColumns), valueCount, tableName, columnCount);
    /// <summary>
    /// EN: Gets the formatted exception message for ViewAlreadyExists.
    /// PT-br: Obtem a mensagem de excecao formatada para ViewAlreadyExists.
    /// </summary>
    public static string ViewAlreadyExists(string viewName) =>
        Format(nameof(ViewAlreadyExists), viewName);
    /// <summary>
    /// EN: Gets the formatted exception message for ViewDoesNotExist.
    /// PT-br: Obtem a mensagem de excecao formatada para ViewDoesNotExist.
    /// </summary>
    public static string ViewDoesNotExist(string viewName) =>
        Format(nameof(ViewDoesNotExist), viewName);
    /// <summary>
    /// EN: Gets the formatted exception message for MultipleSchemasRequireExplicitName.
    /// PT-br: Obtem a mensagem de excecao formatada para MultipleSchemasRequireExplicitName.
    /// </summary>
    public static string MultipleSchemasRequireExplicitName(object? schemaNames) =>
        Format(nameof(MultipleSchemasRequireExplicitName), schemaNames);
    /// <summary>
    /// EN: Gets the formatted exception message for NoSchemaRegistered.
    /// PT-br: Obtem a mensagem de excecao formatada para NoSchemaRegistered.
    /// </summary>
    public static string NoSchemaRegistered() =>
        Format(nameof(NoSchemaRegistered));
    /// <summary>
    /// EN: Gets the formatted exception message for DuplicatePrimaryKeyColumns.
    /// PT-br: Obtem a mensagem de excecao formatada para DuplicatePrimaryKeyColumns.
    /// </summary>
    public static string DuplicatePrimaryKeyColumns() =>
        Format(nameof(DuplicatePrimaryKeyColumns));
    /// <summary>
    /// EN: Gets the formatted exception message for IndexAlreadyExists.
    /// PT-br: Obtem a mensagem de excecao formatada para IndexAlreadyExists.
    /// </summary>
    public static string IndexAlreadyExists(string indexName) =>
        Format(nameof(IndexAlreadyExists), indexName);
    /// <summary>
    /// EN: Gets the formatted exception message for StringColumnSizeRequired.
    /// PT-br: Obtem a mensagem de excecao formatada para StringColumnSizeRequired.
    /// </summary>
    public static string StringColumnSizeRequired() =>
        Format(nameof(StringColumnSizeRequired));
    /// <summary>
    /// EN: Gets the formatted exception message for DecimalPlacesRequiredForDbType.
    /// PT-br: Obtem a mensagem de excecao formatada para DecimalPlacesRequiredForDbType.
    /// </summary>
    public static string DecimalPlacesRequiredForDbType(object? dbType) =>
        Format(nameof(DecimalPlacesRequiredForDbType), dbType);
    /// <summary>
    /// EN: Gets the formatted exception message for DuplicateEnumItems.
    /// PT-br: Obtem a mensagem de excecao formatada para DuplicateEnumItems.
    /// </summary>
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
