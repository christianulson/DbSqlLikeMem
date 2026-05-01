using IBM.Data.Db2;
using Microsoft.Data.SqlClient;
using MySqlConnector;
using Npgsql;
using Oracle.ManagedDataAccess.Client;
using DbSqlLikeMem.TestTools.Performance;
using DbSqlLikeMem.TestTools.Schema;
using DbSqlLikeMem.TestTools.Query;
using System.Collections.Concurrent;

namespace DbSqlLikeMem.Benchmarks.Core;

/// <summary>
/// EN: Provides the common benchmark workflow shared by provider-specific benchmark sessions.
/// PT-br: Fornece o fluxo de benchmark comum compartilhado pelas sessões de benchmark específicas de cada provedor.
/// </summary>
/// <remarks>
/// EN: Derived types supply the connection factory and, when needed, override individual benchmark routines.
/// PT-br: Tipos derivados fornecem a fábrica de conexões e, quando necessário, sobrescrevem rotinas individuais de benchmark.
/// </remarks>
/// <param name="dialect">EN: The provider-specific SQL dialect used to generate benchmark commands. PT-br: O dialeto SQL específico do provedor usado para gerar os comandos de benchmark.</param>
/// <param name="engine">EN: The benchmark engine that identifies the runtime behind the session. PT-br: O mecanismo de benchmark que identifica o runtime por trás da sessão.</param>
public abstract partial class BenchmarkSessionBase(
    ProviderSqlDialect dialect,
    BenchmarkEngine engine
    ) : IBenchmarkSession
{
    private static int _objectCounter;

    /// <summary>
    /// EN: Gets the SQL dialect abstraction used to generate provider-specific statements for the current session.
    /// PT-br: Obtém a abstração de dialeto SQL usada para gerar comandos específicos do provedor para a sessão atual.
    /// </summary>
    public ProviderSqlDialect Dialect { get; } = dialect;

    /// <summary>
    /// EN: Gets the benchmark engine used by the current session.
    /// PT-br: Obtém o mecanismo de benchmark usado pela sessão atual.
    /// </summary>
    public BenchmarkEngine Engine { get; } = engine;

    /// <summary>
    /// EN: Performs any session initialization required before the benchmarks start.
    /// PT-br: Executa a inicialização necessária da sessão antes do início dos benchmarks.
    /// </summary>
    public virtual void Initialize()
    {
    }

    /// <summary>
    /// EN: Executes one benchmark feature and routes any provider-specific failure to the benchmark logger.
    /// PT-br: Executa um recurso de benchmark e encaminha qualquer falha especifica do provedor para o logger do benchmark.
    /// </summary>
    public virtual void Execute(BenchmarkFeatureId feature)
    {
        try
        {
            RunFeature(feature);
        }
        catch (InvalidOperationException ex)
        {
            LogBenchmarkIssue("NA-IOE", feature, ex);
        }
        catch (NotSupportedException ex)
        {
            LogBenchmarkIssue("NA-NSE", feature, ex);
        }
        catch (DB2Exception ex)
        {
            LogBenchmarkIssue("NA-DB2E", feature, ex);
        }
        catch (SqlException ex)
        {
            LogBenchmarkIssue("NA-SqlE", feature, ex);
        }
        catch (MySqlException ex)
        {
            LogBenchmarkIssue("NA-MSE", feature, ex);
        }
        catch (NpgsqlException ex)
        {
            LogBenchmarkIssue("NA-NE", feature, ex);
        }
        catch (OracleException ex)
        {
            LogBenchmarkIssue("NA-OE", feature, ex);
        }
        catch (Exception ex)
        {
            LogBenchmarkIssue("NA", feature, ex);
        }
    }

    /// <summary>
    /// EN: Dispatches the requested benchmark feature to the corresponding benchmark routine.
    /// PT-br: Encaminha o recurso de benchmark solicitado para a rotina de benchmark correspondente.
    /// </summary>
    /// <param name="feature">EN: The benchmark feature to execute. PT-br: O recurso de benchmark a ser executado.</param>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="ArgumentOutOfRangeException"></exception>
    public void RunFeature(BenchmarkFeatureId feature)
    {
        switch (feature)
        {
            case BenchmarkFeatureId.ConnectionOpen:
                RunConnectionOpen();
                break;
            case BenchmarkFeatureId.CreateSchema:
                RunCreateSchema();
                break;
            case BenchmarkFeatureId.CreateTableWithFK:
                RunCreateTableWithFK();
                break;
            case BenchmarkFeatureId.CreateTableWithFKInsert:
                RunCreateTableWithFKInsert();
                break;
            case BenchmarkFeatureId.InsertInTableWithFK:
                RunInsertInTableWithFK();
                break;
            case BenchmarkFeatureId.DropTable:
                RunDropTable();
                break;
            case BenchmarkFeatureId.InsertSingle:
                RunInsertSingle();
                break;
            case BenchmarkFeatureId.InsertCustomStartId:
                RunInsertCustomStartId();
                break;
            case BenchmarkFeatureId.InsertDefaultColumns:
                RunInsertDefaultColumns();
                break;
            case BenchmarkFeatureId.InsertNullableColumns:
                RunInsertNullableColumns();
                break;
            case BenchmarkFeatureId.InsertNotNullWithoutDefault:
                RunInsertNotNullWithoutDefault();
                break;
            case BenchmarkFeatureId.CheckConstraintsValidInsert:
                RunCheckConstraintsValidInsert();
                break;
            case BenchmarkFeatureId.CheckConstraintsInvalidInsert:
                RunCheckConstraintsInvalidInsert();
                break;
            case BenchmarkFeatureId.CheckConstraintsInvalidUpdate:
                RunCheckConstraintsInvalidUpdate();
                break;
            case BenchmarkFeatureId.InsertBatch10:
                RunInsertBatch10();
                break;
            case BenchmarkFeatureId.InsertBatch100:
                RunInsertBatch100();
                break;
            case BenchmarkFeatureId.InsertBatch100Parallel:
                RunInsertBatch100Parallel();
                break;
            case BenchmarkFeatureId.SelectByPk:
                RunSelectByPk();
                break;
            case BenchmarkFeatureId.SelectJoin:
                RunSelectJoin();
                break;
            case BenchmarkFeatureId.SelectJoinCount:
                RunSelectJoinCount();
                break;
            case BenchmarkFeatureId.RelationalComposite:
                RunRelationalComposite();
                break;
            case BenchmarkFeatureId.SelectApplyProjection:
                RunSelectApplyProjection();
                break;
            case BenchmarkFeatureId.SelectWindowFunctions:
                RunSelectWindowFunctions();
                break;
            case BenchmarkFeatureId.SelectScalarSubqueryCaseMatrix:
                RunSelectScalarSubqueryCaseMatrix();
                break;
            case BenchmarkFeatureId.SelectRangeAndPivot:
                RunSelectRangeAndPivot();
                break;
            case BenchmarkFeatureId.InListPredicate:
                RunInListPredicate();
                break;
            case BenchmarkFeatureId.BetweenPredicate:
                RunBetweenPredicate();
                break;
            case BenchmarkFeatureId.LikePredicate:
                RunLikePredicate();
                break;
            case BenchmarkFeatureId.NotLikePredicate:
                RunNotLikePredicate();
                break;
            case BenchmarkFeatureId.NotEqualPredicate:
                RunNotEqualPredicate();
                break;
            case BenchmarkFeatureId.EqualPredicate:
                RunEqualPredicate();
                break;
            case BenchmarkFeatureId.GreaterThanPredicate:
                RunGreaterThanPredicate();
                break;
            case BenchmarkFeatureId.LessThanPredicate:
                RunLessThanPredicate();
                break;
            case BenchmarkFeatureId.GreaterThanOrEqualPredicate:
                RunGreaterThanOrEqualPredicate();
                break;
            case BenchmarkFeatureId.LessThanOrEqualPredicate:
                RunLessThanOrEqualPredicate();
                break;
            case BenchmarkFeatureId.NotInSubqueryNull:
                RunNotInSubqueryNull();
                break;
            case BenchmarkFeatureId.AllRowsCount:
                RunAllRowsCount();
                break;
            case BenchmarkFeatureId.AllRowsSnapshot:
                RunAllRowsSnapshot();
                break;
            case BenchmarkFeatureId.CteMaterializedHint:
                RunCteMaterializedHint();
                break;
            case BenchmarkFeatureId.DistinctOnProjection:
                RunDistinctOnProjection();
                break;
            case BenchmarkFeatureId.OrderByNameMatrix:
                RunOrderByNameMatrix();
                break;
            case BenchmarkFeatureId.OrderByOrdinalMatrix:
                RunOrderByOrdinalMatrix();
                break;
            case BenchmarkFeatureId.OrderByNameDescendingMatrix:
                RunOrderByNameDescendingMatrix();
                break;
            case BenchmarkFeatureId.NamePaginationMatrix:
                RunNamePaginationMatrix();
                break;
            case BenchmarkFeatureId.GroupByNameInitialMatrix:
                RunGroupByNameInitialMatrix();
                break;
            case BenchmarkFeatureId.GroupByNameHavingMatrix:
                RunGroupByNameHavingMatrix();
                break;
            case BenchmarkFeatureId.GroupByOrdinalMatrix:
                RunGroupByOrdinalMatrix();
                break;
            case BenchmarkFeatureId.DistinctOrderByOrdinalMatrix:
                RunDistinctOrderByOrdinalMatrix();
                break;
            case BenchmarkFeatureId.DistinctLikeOrderByOrdinalMatrix:
                RunDistinctLikeOrderByOrdinalMatrix();
                break;
            case BenchmarkFeatureId.JoinTypedExpressionMatrix:
                RunJoinTypedExpressionMatrix();
                break;
            case BenchmarkFeatureId.JoinNullAggregateMatrix:
                RunJoinNullAggregateMatrix();
                break;
            case BenchmarkFeatureId.JoinCastNullMatrix:
                RunJoinCastNullMatrix();
                break;
            case BenchmarkFeatureId.JoinCastTextComparisonMatrix:
                RunJoinCastTextComparisonMatrix();
                break;
            case BenchmarkFeatureId.JoinHavingCastMatrix:
                RunJoinHavingCastMatrix();
                break;
            case BenchmarkFeatureId.JoinLengthNumericMatrix:
                RunJoinLengthNumericMatrix();
                break;
            case BenchmarkFeatureId.JoinTextCaseLengthMatrix:
                RunJoinTextCaseLengthMatrix();
                break;
            case BenchmarkFeatureId.JoinDistinctCaseMatrix:
                RunJoinDistinctCaseMatrix();
                break;
            case BenchmarkFeatureId.JoinDistinctHavingMatrix:
                RunJoinDistinctHavingMatrix();
                break;
            case BenchmarkFeatureId.StringSplitProjection:
                RunStringSplitProjection();
                break;
            case BenchmarkFeatureId.ForJsonPathProjection:
                RunForJsonPathProjection();
                break;
            case BenchmarkFeatureId.JoinWindowTemporalMatrix:
                RunJoinWindowTemporalMatrix();
                break;
            case BenchmarkFeatureId.JoinTemporalMatrix:
                RunJoinTemporalMatrix();
                break;
            case BenchmarkFeatureId.JoinWindowMatrix:
                RunJoinWindowMatrix();
                break;
            case BenchmarkFeatureId.JoinWindowAggregateTemporalMatrix:
                RunJoinWindowAggregateTemporalMatrix();
                break;
            case BenchmarkFeatureId.ApplyTemporalComposite:
                RunApplyTemporalComposite();
                break;
            case BenchmarkFeatureId.ApplyWindowTemporalComposite:
                RunApplyWindowTemporalComposite();
                break;
            case BenchmarkFeatureId.UpdateByPk:
                RunUpdateByPk();
                break;
            case BenchmarkFeatureId.UpdateDeleteRoundTrip:
                RunUpdateDeleteRoundTrip();
                break;
            case BenchmarkFeatureId.ParameterUpdateDeleteRoundTrip:
                RunParameterUpdateDeleteRoundTrip();
                break;
            case BenchmarkFeatureId.DeleteByPk:
                RunDeleteByPk();
                break;
            case BenchmarkFeatureId.TransactionCommit:
                RunTransactionCommit();
                break;
            case BenchmarkFeatureId.TransactionRollback:
                RunTransactionRollback();
                break;
            case BenchmarkFeatureId.TransactionalUpdateDeleteCommit:
                RunTransactionalUpdateDeleteCommit();
                break;
            case BenchmarkFeatureId.ParameterTransactionCommit:
                RunParameterTransactionCommit();
                break;
            case BenchmarkFeatureId.ParameterTransactionRollback:
                RunParameterTransactionRollback();
                break;
            case BenchmarkFeatureId.SavepointCreate:
                RunSavepointCreate();
                break;
            case BenchmarkFeatureId.RollbackToSavepoint:
                RunRollbackToSavepoint();
                break;
            case BenchmarkFeatureId.ReleaseSavepoint:
                RunReleaseSavepoint();
                break;
            case BenchmarkFeatureId.NestedSavepointFlow:
                RunNestedSavepointFlow();
                break;
            case BenchmarkFeatureId.Upsert:
                RunUpsert();
                break;
            case BenchmarkFeatureId.MergeInsertThenUpdate:
                RunMergeInsertThenUpdate();
                break;
            case BenchmarkFeatureId.UpsertInsertThenUpdate:
                RunUpsertInsertThenUpdate();
                break;
            case BenchmarkFeatureId.ParameterProjection:
                RunParameterProjection();
                break;
            case BenchmarkFeatureId.ParameterInsertSingle:
                RunParameterInsertSingle();
                break;
            case BenchmarkFeatureId.ParameterInsertRoundTrip:
                RunParameterInsertRoundTrip();
                break;
            case BenchmarkFeatureId.ParameterInsertNullRoundTrip:
                RunParameterInsertNullRoundTrip();
                break;
            case BenchmarkFeatureId.ParameterSelectByNameMatrix:
                RunParameterSelectByNameMatrix();
                break;
            case BenchmarkFeatureId.ParameterSelectByIdMatrix:
                RunParameterSelectByIdMatrix();
                break;
            case BenchmarkFeatureId.ParameterRoundTripMatrix:
                RunParameterRoundTripMatrix();
                break;
            case BenchmarkFeatureId.ParameterTypeMatrix:
                RunParameterTypeMatrix();
                break;
            case BenchmarkFeatureId.ParameterDateCurrencyMatrix:
                RunParameterDateCurrencyMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldStorageMatrix:
                RunTypedFieldStorageMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldFunctionMatrix:
                RunTypedFieldFunctionMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldCalculationMatrix:
                RunTypedFieldCalculationMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldAndFunctionBlend:
                RunTypedFieldAndFunctionBlend();
                break;
            case BenchmarkFeatureId.TypedFieldCompoundPredicateMatrix:
                RunTypedFieldCompoundPredicateMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldCastCalculationMatrix:
                RunTypedFieldCastCalculationMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldNullComparisonMatrix:
                RunTypedFieldNullComparisonMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldTextLengthMatrix:
                RunTypedFieldTextLengthMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldTextCaseMatrix:
                RunTypedFieldTextCaseMatrix();
                break;
            case BenchmarkFeatureId.TypedFieldPredicateMatrix:
                RunTypedFieldPredicateMatrix();
                break;
            case BenchmarkFeatureId.StoredProcedureCall:
                RunStoredProcedureCall();
                break;
            case BenchmarkFeatureId.SequenceNextValue:
                RunSequenceNextValue();
                break;
            case BenchmarkFeatureId.SequenceCurrentValue:
                RunSequenceCurrentValue();
                break;
            case BenchmarkFeatureId.SequenceInsertRoundTrip:
                RunSequenceInsertRoundTrip();
                break;
            case BenchmarkFeatureId.SequenceInsertExpression:
                RunSequenceInsertExpression();
                break;
            case BenchmarkFeatureId.SequenceSelectProjection:
                RunSequenceSelectProjection();
                break;
            case BenchmarkFeatureId.SequenceExpressionFilter:
                RunSequenceExpressionFilter();
                break;
            case BenchmarkFeatureId.SequenceCaseWhereMatrix:
                RunSequenceCaseWhereMatrix();
                break;
            case BenchmarkFeatureId.SequenceTemporalMatrix:
                RunSequenceTemporalMatrix();
                break;
            case BenchmarkFeatureId.SequenceJoinAggregate:
                RunSequenceJoinAggregate();
                break;
            case BenchmarkFeatureId.BatchInsert10:
                RunBatchInsert10();
                break;
            case BenchmarkFeatureId.BatchInsert100:
                RunBatchInsert100();
                break;
            case BenchmarkFeatureId.BatchMixedReadWrite:
                RunBatchMixedReadWrite();
                break;
            case BenchmarkFeatureId.BatchScalar:
                RunBatchScalar();
                break;
            case BenchmarkFeatureId.BatchNonQuery:
                RunBatchNonQuery();
                break;
            case BenchmarkFeatureId.StringAggregate:
                RunStringAggregate();
                break;
            case BenchmarkFeatureId.StringAggregateOrdered:
                RunStringAggregateOrdered();
                break;
            case BenchmarkFeatureId.StringAggregateDistinct:
                RunStringAggregateDistinct();
                break;
            case BenchmarkFeatureId.StringAggregateCustomSeparator:
                RunStringAggregateCustomSeparator();
                break;
            case BenchmarkFeatureId.StringAggregateLargeGroup:
                RunStringAggregateLargeGroup();
                break;
            case BenchmarkFeatureId.StringAggregateSummaryMatrix:
                RunStringAggregateSummaryMatrix();
                break;
            case BenchmarkFeatureId.StringAggregateGroupCaseMatrix:
                RunStringAggregateGroupCaseMatrix();
                break;
            case BenchmarkFeatureId.StringAggregationSummaryMatrix:
                RunStringAggregationSummaryMatrix();
                break;
            case BenchmarkFeatureId.StringAggregationGroupCaseMatrix:
                RunStringAggregationGroupCaseMatrix();
                break;
            case BenchmarkFeatureId.StringAggregationVariants:
                RunStringAggregationVariants();
                break;
            case BenchmarkFeatureId.DateScalar:
                RunDateScalar();
                break;
            case BenchmarkFeatureId.MathFunctions:
                RunMathFunctions();
                break;
            case BenchmarkFeatureId.MathLogBaseFunction:
                RunMathLogBaseFunction();
                break;
            case BenchmarkFeatureId.MathLog2Function:
                RunMathLog2Function();
                break;
            case BenchmarkFeatureId.MathPiFunction:
                RunMathPiFunction();
                break;
            case BenchmarkFeatureId.MathRandFunction:
                RunMathRandFunction();
                break;
            case BenchmarkFeatureId.MathRemainderFunction:
                RunMathRemainderFunction();
                break;
            case BenchmarkFeatureId.MathTruncFunction:
                RunMathTruncFunction();
                break;
            case BenchmarkFeatureId.MathCotFunction:
                RunMathCotFunction();
                break;
            case BenchmarkFeatureId.MySqlUtilityMathFunctions:
                RunMySqlUtilityMathFunctions();
                break;
            case BenchmarkFeatureId.GreatestLeastModFunctions:
                RunGreatestLeastModFunctions();
                break;
            case BenchmarkFeatureId.Db2AliasMathFunctions:
                RunDb2AliasMathFunctions();
                break;
            case BenchmarkFeatureId.FirebirdAliasMathFunctions:
                RunFirebirdAliasMathFunctions();
                break;
            case BenchmarkFeatureId.MathTranscendentalFunctions:
                RunMathTranscendentalFunctions();
                break;
            case BenchmarkFeatureId.JsonScalarRead:
                RunJsonScalarRead();
                break;
            case BenchmarkFeatureId.JsonPathRead:
                RunJsonPathRead();
                break;
            case BenchmarkFeatureId.JsonMissingPathRead:
                RunJsonMissingPathRead();
                break;
            case BenchmarkFeatureId.JsonMissingPathReturnsNull:
                RunJsonMissingPathReturnsNull();
                break;
            case BenchmarkFeatureId.JsonQueryRootFragment:
                RunJsonQueryRootFragment();
                break;
            case BenchmarkFeatureId.JsonModifyReplace:
                RunJsonModifyReplace();
                break;
            case BenchmarkFeatureId.JsonTypedFieldMatrix:
                RunJsonTypedFieldMatrix();
                break;
            case BenchmarkFeatureId.JsonEachFromArray:
                RunJsonEachFromArray();
                break;
            case BenchmarkFeatureId.JsonEachFromObject:
                RunJsonEachFromObject();
                break;
            case BenchmarkFeatureId.JsonTreeStructure:
                RunJsonTreeStructure();
                break;
            case BenchmarkFeatureId.OpenJsonArray:
                RunOpenJsonArray();
                break;
            case BenchmarkFeatureId.TemporalCurrentTimestamp:
                RunTemporalCurrentTimestamp();
                break;
            case BenchmarkFeatureId.TemporalDateAdd:
                RunTemporalDateAdd();
                break;
            case BenchmarkFeatureId.TemporalNowWhere:
                RunTemporalNowWhere();
                break;
            case BenchmarkFeatureId.TemporalNowOrderBy:
                RunTemporalNowOrderBy();
                break;
            case BenchmarkFeatureId.ScalarTemporalMatrix:
                RunScalarTemporalMatrix();
                break;
            case BenchmarkFeatureId.TemporalFieldMatrix:
                RunTemporalFieldMatrix();
                break;
            case BenchmarkFeatureId.TemporalComparisonMatrix:
                RunTemporalComparisonMatrix();
                break;
            case BenchmarkFeatureId.TemporalArithmeticMatrix:
                RunTemporalArithmeticMatrix();
                break;
            case BenchmarkFeatureId.TemporalDateTrunc:
                RunTemporalDateTrunc();
                break;
            case BenchmarkFeatureId.TemporalTimeZoneOffset:
                RunTemporalTimeZoneOffset();
                break;
            case BenchmarkFeatureId.TemporalFromParts:
                RunTemporalFromParts();
                break;
            case BenchmarkFeatureId.TemporalEndOfMonth:
                RunTemporalEndOfMonth();
                break;
            case BenchmarkFeatureId.TemporalDateDiffBig:
                RunTemporalDateDiffBig();
                break;
            case BenchmarkFeatureId.SqlServerMetadataFunctions:
                RunSqlServerMetadataFunctions();
                break;
            case BenchmarkFeatureId.ScopeIdentity:
                RunScopeIdentity();
                break;
            case BenchmarkFeatureId.SqlServerSystemFunctions:
                RunSqlServerSystemFunctions();
                break;
            case BenchmarkFeatureId.SqlServerSpecialFunctions:
                RunSqlServerSpecialFunctions();
                break;
            case BenchmarkFeatureId.SqlServerContextFunctions:
                RunSqlServerContextFunctions();
                break;
            case BenchmarkFeatureId.SqlServerTransactionStateFunctions:
                RunSqlServerTransactionStateFunctions();
                break;
            case BenchmarkFeatureId.SqlServerSessionFunctions:
                RunSqlServerSessionFunctions();
                break;
            case BenchmarkFeatureId.StringBasicFunctions:
                RunStringBasicFunctions();
                break;
            case BenchmarkFeatureId.StringUtilityFunctions:
                RunStringUtilityFunctions();
                break;
            case BenchmarkFeatureId.StringMetadataFunctions:
                RunStringMetadataFunctions();
                break;
            case BenchmarkFeatureId.StringEscape:
                RunStringEscape();
                break;
            case BenchmarkFeatureId.Translate:
                RunTranslate();
                break;
            case BenchmarkFeatureId.FormatMessage:
                RunFormatMessage();
                break;
            case BenchmarkFeatureId.IsJson:
                RunIsJson();
                break;
            case BenchmarkFeatureId.Format:
                RunFormat();
                break;
            case BenchmarkFeatureId.ParseFamily:
                RunParseFamily();
                break;
            case BenchmarkFeatureId.Soundex:
                RunSoundex();
                break;
            case BenchmarkFeatureId.Compression:
                RunCompression();
                break;
            case BenchmarkFeatureId.ApproxCountDistinct:
                RunApproxCountDistinct();
                break;
            case BenchmarkFeatureId.PercentileAggregateFunctions:
                RunPercentileAggregateFunctions();
                break;
            case BenchmarkFeatureId.SqlServerAggregateFunctions:
                RunSqlServerAggregateFunctions();
                break;
            case BenchmarkFeatureId.RowCountAfterInsert:
                RunRowCountAfterInsert();
                break;
            case BenchmarkFeatureId.RowCountAfterUpdate:
                RunRowCountAfterUpdate();
                break;
            case BenchmarkFeatureId.RowCountAfterSelect:
                RunRowCountAfterSelect();
                break;
            case BenchmarkFeatureId.CteSimple:
                RunCteSimple();
                break;
            case BenchmarkFeatureId.WindowRowNumber:
                RunWindowRowNumber();
                break;
            case BenchmarkFeatureId.WindowLag:
                RunWindowLag();
                break;
            case BenchmarkFeatureId.WindowLead:
                RunWindowLead();
                break;
            case BenchmarkFeatureId.WindowRankDenseRank:
                RunWindowRankDenseRank();
                break;
            case BenchmarkFeatureId.WindowFirstLastValue:
                RunWindowFirstLastValue();
                break;
            case BenchmarkFeatureId.WindowNtile:
                RunWindowNtile();
                break;
            case BenchmarkFeatureId.WindowPercentRankCumeDist:
                RunWindowPercentRankCumeDist();
                break;
            case BenchmarkFeatureId.WindowNthValue:
                RunWindowNthValue();
                break;
            case BenchmarkFeatureId.BatchReaderMultiResult:
                RunBatchReaderMultiResult();
                break;
            case BenchmarkFeatureId.BatchTransactionControl:
                RunBatchTransactionControl();
                break;
            case BenchmarkFeatureId.ParseSimpleSelect:
                RunParseSimpleSelect();
                break;
            case BenchmarkFeatureId.ParseComplexJoin:
                RunParseComplexJoin();
                break;
            case BenchmarkFeatureId.ParseInsertReturning:
                RunParseInsertReturning();
                break;
            case BenchmarkFeatureId.ParseOnConflictDoUpdate:
                RunParseOnConflictDoUpdate();
                break;
            case BenchmarkFeatureId.ParseJsonExtract:
                RunParseJsonExtract();
                break;
            case BenchmarkFeatureId.ParseStringAggregateWithinGroup:
                RunParseStringAggregateWithinGroup();
                break;
            case BenchmarkFeatureId.ParseAutoDialectTopLimitFetch:
                RunParseAutoDialectTopLimitFetch();
                break;
            case BenchmarkFeatureId.ParseMultiStatementBatch:
                RunParseMultiStatementBatch();
                break;
            case BenchmarkFeatureId.JsonInsertCast:
                RunJsonInsertCast();
                break;
            case BenchmarkFeatureId.JsonInsertCastReturnsNull:
                RunJsonInsertCastReturnsNull();
                break;
            case BenchmarkFeatureId.RowCountInBatch:
                RunRowCountInBatch();
                break;
            case BenchmarkFeatureId.BatchRowCountInBatch:
                RunBatchRowCountInBatch();
                break;
            case BenchmarkFeatureId.PivotCount:
                RunPivotCount();
                break;
            case BenchmarkFeatureId.ReturningInsert:
                RunReturningInsert();
                break;
            case BenchmarkFeatureId.BatchReturningInsert:
                RunBatchReturningInsert();
                break;
            case BenchmarkFeatureId.ReturningUpdate:
                RunReturningUpdate();
                break;
            case BenchmarkFeatureId.MergeBasic:
                RunMergeBasic();
                break;
            case BenchmarkFeatureId.PartitionPruningSelect:
                RunPartitionPruningSelect();
                break;
            case BenchmarkFeatureId.ExecutionPlan:
                RunExecutionPlan();
                break;
            case BenchmarkFeatureId.ExecutionPlanSelect:
                RunExecutionPlanSelect();
                break;
            case BenchmarkFeatureId.ExecutionPlanJoin:
                RunExecutionPlanJoin();
                break;
            case BenchmarkFeatureId.ExecutionPlanDml:
                RunExecutionPlanDml();
                break;
            case BenchmarkFeatureId.DebugTraceSelect:
                RunDebugTraceSelect();
                break;
            case BenchmarkFeatureId.DebugTraceBatch:
                RunDebugTraceBatch();
                break;
            case BenchmarkFeatureId.DebugTraceJson:
                RunDebugTraceJson();
                break;
            case BenchmarkFeatureId.LastExecutionPlansHistory:
                RunLastExecutionPlansHistory();
                break;
            case BenchmarkFeatureId.TempTableCreateAndUse:
                RunTempTableCreateAndUse();
                break;
            case BenchmarkFeatureId.TempTableRollback:
                RunTempTableRollback();
                break;
            case BenchmarkFeatureId.TempTableCrossConnectionIsolation:
                RunTempTableCrossConnectionIsolation();
                break;
            case BenchmarkFeatureId.ResetVolatileData:
                RunResetVolatileData();
                break;
            case BenchmarkFeatureId.ResetAllVolatileData:
                RunResetAllVolatileData();
                break;
            case BenchmarkFeatureId.ConnectionReopenAfterClose:
                RunConnectionReopenAfterClose();
                break;
            case BenchmarkFeatureId.SchemaSnapshotExport:
                RunSchemaSnapshotExport();
                break;
            case BenchmarkFeatureId.SchemaSnapshotToJson:
                RunSchemaSnapshotToJson();
                break;
            case BenchmarkFeatureId.SchemaSnapshotLoadJson:
                RunSchemaSnapshotLoadJson();
                break;
            case BenchmarkFeatureId.SchemaSnapshotApply:
                RunSchemaSnapshotApply();
                break;
            case BenchmarkFeatureId.SchemaSnapshotRoundTrip:
                RunSchemaSnapshotRoundTrip();
                break;
            case BenchmarkFeatureId.SchemaSnapshotCompare:
                RunSchemaSnapshotCompare();
                break;
            case BenchmarkFeatureId.FluentSchemaBuild:
                RunFluentSchemaBuild();
                break;
            case BenchmarkFeatureId.FluentSeed100:
                RunFluentSeed100();
                break;
            case BenchmarkFeatureId.FluentSeed1000:
                RunFluentSeed1000();
                break;
            case BenchmarkFeatureId.FluentScenarioCompose:
                RunFluentScenarioCompose();
                break;
            case BenchmarkFeatureId.MultiJoinAggregate:
                RunMultiJoinAggregate();
                break;
            case BenchmarkFeatureId.UnionAllProjection:
                RunUnionAllProjection();
                break;
            case BenchmarkFeatureId.GroupByHaving:
                RunGroupByHaving();
                break;
            case BenchmarkFeatureId.SelectCorrelatedCount:
                RunSelectCorrelatedCount();
                break;
            case BenchmarkFeatureId.SelectExistsPredicate:
                RunSelectExistsPredicate();
                break;
            case BenchmarkFeatureId.SelectNotExistsPredicate:
                RunSelectNotExistsPredicate();
                break;
            case BenchmarkFeatureId.SelectLeftJoinAntiJoin:
                RunSelectLeftJoinAntiJoin();
                break;
            case BenchmarkFeatureId.SelectScalarSubquery:
                RunSelectScalarSubquery();
                break;
            case BenchmarkFeatureId.SelectScalarCaseMatrix:
                RunSelectScalarCaseMatrix();
                break;
            case BenchmarkFeatureId.DistinctProjection:
                RunDistinctProjection();
                break;
            case BenchmarkFeatureId.UnionDistinctProjection:
                RunUnionDistinctProjection();
                break;
            case BenchmarkFeatureId.SelectInSubquery:
                RunSelectInSubquery();
                break;
            case BenchmarkFeatureId.SelectNotInSubquery:
                RunSelectNotInSubquery();
                break;
            case BenchmarkFeatureId.SelectBetweenLikeOrderByMatrix:
                RunSelectBetweenLikeOrderByMatrix();
                break;
            case BenchmarkFeatureId.OuterApplyProjection:
                RunOuterApplyProjection();
                break;
            case BenchmarkFeatureId.CrossApplyProjection:
                RunCrossApplyProjection();
                break;
            case BenchmarkFeatureId.PagedNameProjection:
                RunPagedNameProjection();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(feature), feature, null);
        }
    }

    private static readonly object _logSync = new();

    private static readonly ConcurrentDictionary<string, int> Errors = [];

    protected virtual void LogBenchmarkIssue(string txt, BenchmarkFeatureId feature, Exception ex)
    {
        var root = ex?.GetBaseException();
        var message = root is NotSupportedException
            ? $"[{txt}-{root.GetType().Name}] {feature}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
            : $"[{txt}-{root?.GetType().Name}] {feature}: {root?.Message} -- {ex?.StackTrace}{Environment.NewLine}{Environment.NewLine}";

        Console.WriteLine(message);

        lock (_logSync)
        {
            var errorKey = $"{GetType().FullName}|{Dialect.DisplayName}|{feature}|{root?.GetType().FullName}|{root?.Message}";
            if (Errors.TryGetValue(errorKey, out int value))
            {
                Errors[errorKey] = value + 1;
                return;
            }
            Errors.GetOrAdd(errorKey, 0);

            var directory = BenchmarkLogPath.GetDirectory();
            Directory.CreateDirectory(directory);

            var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-{Dialect.DisplayName}-errors.log");
            File.AppendAllText(
                file,
                message + Environment.NewLine);
        }
    }

    /// <summary>
    /// EN: Releases any resources allocated by the benchmark session.
    /// PT-br: Libera os recursos alocados pela sessão de benchmark.
    /// </summary>
    public virtual void Dispose()
    {
        DisposePreparedStates();
    }

    /// <summary>
    /// EN: Creates a new provider-specific connection instance for the current benchmark session.
    /// PT-br: Cria uma nova instância de conexão específica do provedor para a sessão de benchmark atual.
    /// </summary>
    /// <returns>EN: A new provider-specific connection instance. PT-br: Uma nova instância de conexão específica do provedor.</returns>
    protected abstract DbConnection CreateConnection();

    /// <summary>
    /// EN: Creates the benchmark users table through the shared CreateTable service and then removes it.
    /// PT-br: Cria a tabela de usuários do benchmark pelo service compartilhado de CreateTable e depois a remove.
    /// </summary>
    protected virtual void RunCreateSchema()
    {
        var state = GetPreparedCreateSchemaState();
        state.RunCreateSchema();
    }

    /// <summary>
    /// EN: Creates the benchmark users and orders tables with a foreign key and removes them after the run.
    /// PT-br: Cria as tabelas de usuarios e pedidos do benchmark com chave estrangeira e as remove apos a execucao.
    /// </summary>
    protected virtual void RunCreateTableWithFK()
    {
        var state = GetPreparedCreateTableWithFkState();
        state.RunCreateTableWithFk();
    }

    /// <summary>
    /// EN: Creates the benchmark foreign-key tables and inserts a referenced row.
    /// PT-br: Cria as tabelas com chave estrangeira do benchmark e insere uma linha referenciada.
    /// </summary>
    protected virtual void RunCreateTableWithFKInsert()
    {
        var state = GetPreparedCreateTableWithFkState();
        var count = state.RunCreateTableWithFkInsert(1, 10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the insert-in-table-with-FK benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de insert na tabela com chave estrangeira e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunInsertInTableWithFK()
        => RunCreateTableWithFKInsert();

    /// <summary>
    /// EN: Creates and drops the benchmark users table through the shared DDL drop workflow.
    /// PT-br: Cria e remove a tabela de usuarios do benchmark pelo fluxo compartilhado de remocao DDL.
    /// </summary>
    protected virtual void RunDropTable()
    {
        var state = GetPreparedDropTableState();
        state.RunDropTable();
    }

    /// <summary>
    /// EN: Inserts a single user row and validates that the row was persisted.
    /// PT-br: Insere uma única linha de usuário e valida que a linha foi persistida.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertSingle()
    {
        var state = GetPreparedInsertUsersState("InsertSingle");
        var count = state.RunSequentialInsert(1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts three rows starting from a custom id and validates the persisted names.
    /// PT-br: Insere tres linhas iniciando em um id customizado e valida os nomes persistidos.
    /// </summary>
    protected virtual void RunInsertCustomStartId()
    {
        var state = GetPreparedInsertUsersState("InsertCustomStartId");
        var result = state.RunInsertCustomStartId();
        GC.KeepAlive(result);
    }

    protected virtual void RunInsertDefaultColumns()
    {
        var state = GetPreparedInsertUsersState("InsertDefaultColumns");
        var result = state.RunInsertDefaultColumns();
        GC.KeepAlive(result);
    }

    protected virtual void RunInsertNullableColumns()
    {
        var state = GetPreparedInsertUsersState("InsertNullableColumns");
        var result = state.RunInsertNullableColumns();
        GC.KeepAlive(result);
    }

    protected virtual void RunInsertNotNullWithoutDefault()
    {
        var state = GetPreparedInsertUsersState("InsertNotNullWithoutDefault");
        var result = state.RunInsertNotNullWithoutDefault();
        GC.KeepAlive(result);
    }

    protected virtual void RunCheckConstraintsValidInsert()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsValidInsert");
        var result = state.RunCheckConstraintsValidInsert();
        GC.KeepAlive(result);
    }

    protected virtual void RunCheckConstraintsInvalidInsert()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsInvalidInsert");
        var result = state.RunCheckConstraintsInvalidInsert();
        GC.KeepAlive(result);
    }

    protected virtual void RunCheckConstraintsInvalidUpdate()
    {
        var state = GetPreparedCheckConstraintsState("CheckConstraintsInvalidUpdate");
        var result = state.RunCheckConstraintsInvalidUpdate();
        GC.KeepAlive(result);
    }

    protected virtual void RunInsertBatch10()
    {
        var state = GetPreparedInsertUsersState("InsertBatch10");
        var count = state.RunSequentialInsert(10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows and validates the final row count.
    /// PT-br: Insere cem linhas de usuário e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100");
        var count = state.RunSequentialInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Inserts one hundred user rows in parallel and validates the final row count.
    /// PT-br: Insere cem linhas de usuário em paralelo e valida a contagem final de linhas.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunInsertBatch100Parallel()
    {
        var state = GetPreparedInsertUsersState("InsertBatch100Parallel");
        var count = state.RunParallelInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Reads a user name by primary key through the shared SelectByPK service and validates the returned value.
    /// PT-br: Lê um nome de usuário pela chave primária pelo service compartilhado SelectByPK e valida o valor retornado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectByPk()
    {
        var state = GetPreparedSelectByPkState();
        var value = state.Service.RunTestAsync(1).GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a join query between users and orders and validates the resulting count.
    /// PT-br: Executa uma consulta com junção entre usuários e pedidos e valida a contagem resultante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunSelectJoin()
    {
        var state = GetPreparedSelectJoinState();
        var value = state.Service.RunTestAsync(1).GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the join-count benchmark by reusing the shared join flow.
    /// PT-br: Executa o benchmark de contagem do join reutilizando o fluxo compartilhado de join.
    /// </summary>
    protected virtual void RunSelectJoinCount()
        => RunSelectJoin();

    /// <summary>
    /// EN: Executes the APPLY projection benchmark by chaining CROSS APPLY and OUTER APPLY projections.
    /// PT-br: Executa o benchmark de projeção APPLY encadeando projecoes CROSS APPLY e OUTER APPLY.
    /// </summary>
    protected virtual void RunSelectApplyProjection()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
    }

    /// <summary>
    /// EN: Executes the window-functions benchmark by chaining row-number, lag, and lead projections.
    /// PT-br: Executa o benchmark de funcoes de janela encadeando row-number, lag e lead.
    /// </summary>
    protected virtual void RunSelectWindowFunctions()
    {
        RunWindowRowNumber();
        RunWindowLag();
        RunWindowLead();
    }

    /// <summary>
    /// EN: Executes the scalar-subquery CASE matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz CASE com subconsulta escalar e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunSelectScalarSubqueryCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "SelectScalarSubqueryCaseMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 1, "o-3"), (4, 2, "o-4")]);
        var value = state.Service.RunSelectScalarSubqueryCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the range-and-pivot benchmark by chaining partition pruning and pivot counting.
    /// PT-br: Executa o benchmark de faixa e pivot encadeando partition pruning e contagem pivot.
    /// </summary>
    protected virtual void RunSelectRangeAndPivot()
    {
        var state = GetPreparedSelectTableQueryState("SelectRangeAndPivot");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            for (var id = 3; id <= 12; id++)
            {
                state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, id, $"User-{id}")).GetAwaiter().GetResult();
            }

            var partitionCount = state.Service.RunPartitionPruningSelectAsync().GetAwaiter().GetResult();
            var pivotCount = state.Service.RunPivotCountAsync().GetAwaiter().GetResult();
            GC.KeepAlive(partitionCount);
            GC.KeepAlive(pivotCount);
        }
        finally
        {
            for (var id = 2; id <= 12; id++)
            {
                state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = {id}").GetAwaiter().GetResult();
            }
        }
    }

    /// <summary>
    /// EN: Executes the IN-list predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado IN com lista e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunInListPredicate()
    {
        var state = GetPreparedUsersQueryState("InListPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        var value = state.Service.RunInListPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the BETWEEN predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado BETWEEN e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunBetweenPredicate()
    {
        var state = GetPreparedUsersQueryState("BetweenPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunBetweenPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the LIKE predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado LIKE e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLikePredicate()
    {
        var state = GetPreparedUsersQueryState("LikePredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLikePredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the NOT LIKE predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado NOT LIKE e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotLikePredicate()
    {
        var state = GetPreparedUsersQueryState("NotLikePredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNotLikePredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the not-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado diferente de e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("NotEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNotEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the equality predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado de igualdade e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("EqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the greater-than predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado maior que e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGreaterThanPredicate()
    {
        var state = GetPreparedUsersQueryState("GreaterThanPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunGreaterThanPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the less-than predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado menor que e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLessThanPredicate()
    {
        var state = GetPreparedUsersQueryState("LessThanPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLessThanPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the greater-than-or-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado maior ou igual e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGreaterThanOrEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("GreaterThanOrEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunGreaterThanOrEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the less-than-or-equal predicate benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de predicado menor ou igual e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunLessThanOrEqualPredicate()
    {
        var state = GetPreparedUsersQueryState("LessThanOrEqualPredicate", (1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunLessThanOrEqualPredicateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the NOT IN subquery with NULL benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de subconsulta NOT IN com NULL e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNotInSubqueryNull()
    {
        var state = GetPreparedUsersQueryState("NotInSubqueryNull", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        using var command = state.Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Id, Name
FROM {state.Context.TbUsersFullName}
WHERE Id NOT IN (
    SELECT 1
    FROM {state.Context.TbUsersFullName} u
    WHERE u.Id = 1
    UNION ALL
    SELECT NULL
    FROM {state.Context.TbUsersFullName} u
    WHERE u.Id = 1
)
ORDER BY Id
""";

        using var reader = command.ExecuteReaderAsync().GetAwaiter().GetResult();
        var snapshot = QueryResultSnapshotReader.Capture(reader);
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the relational composite benchmark by chaining the main relational query flows.
    /// PT-br: Executa o benchmark composto relacional encadeando os principais fluxos relacionais.
    /// </summary>
    protected virtual void RunRelationalComposite()
    {
        RunCteSimple();
        RunSelectExistsPredicate();
        RunSelectNotExistsPredicate();
        RunSelectLeftJoinAntiJoin();
        RunSelectCorrelatedCount();
        RunSelectScalarCaseMatrix();
        RunGroupByHaving();
        RunUnionAllProjection();
        RunUnionDistinctProjection();
        RunDistinctProjection();
        RunMultiJoinAggregate();
        RunSelectScalarSubquery();
        RunSelectInSubquery();
        RunSelectNotInSubquery();
        RunSelectBetweenLikeOrderByMatrix();
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunPivotCount();
    }

    /// <summary>
    /// EN: Executes the all-rows count benchmark and keeps the row-count result alive.
    /// PT-br: Executa o benchmark de contagem de todas as linhas e mantem o resultado da contagem ativo.
    /// </summary>
    protected virtual void RunAllRowsCount()
    {
        var state = GetPreparedSelectTableQueryState("AllRowsCount");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            var count = state.Service.RunRowCountAfterSelectAsync().GetAwaiter().GetResult();
            GC.KeepAlive(count);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the all-rows snapshot benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de snapshot de todas as linhas e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunAllRowsSnapshot()
    {
        var state = GetPreparedSelectTableQueryState("AllRowsSnapshot");
        try
        {
            state.Repo.ExecuteNonQueryAsync(state.Repo.Dialect.InsertUser(state.Context, 2, "Bob")).GetAwaiter().GetResult();
            var value = state.Service.RunAllRowsSnapshotAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the CTE MATERIALIZED benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de CTE MATERIALIZED e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunCteMaterializedHint()
    {
        if (!Dialect.SupportsWithMaterializedHint)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("CteMaterializedHint");
        var value = state.Service.RunCteMaterializedHintAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT ON projection benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark de projecao DISTINCT ON e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctOnProjection()
    {
        if (!Dialect.SupportsDistinctOnProjection)
        {
            return;
        }

        var state = GetPreparedUsersOrdersQueryState(
            "DistinctOnProjection",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunDistinctOnProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY Name matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY Name e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByNameMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByNameMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"));
        var value = state.Service.RunOrderByNameMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByOrdinalMatrix", (1, "Alpha"), (2, "Bravo"), (3, "Charlie"));
        var value = state.Service.RunOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the ORDER BY Name descending matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz ORDER BY Name descendente e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunOrderByNameDescendingMatrix()
    {
        var state = GetPreparedUsersQueryState("OrderByNameDescendingMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"));
        var value = state.Service.RunOrderByNameDescendingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the name pagination matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz de paginacao por nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunNamePaginationMatrix()
    {
        var state = GetPreparedUsersQueryState("NamePaginationMatrix", (1, "Aaron"), (2, "Bravo"), (3, "Charlie"), (4, "Delta"), (5, "Echo"));
        var value = state.Service.RunNamePaginationMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY name initial matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY por inicial do nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByNameInitialMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "GroupByNameInitialMatrix",
            (1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris"));
        var value = state.Service.RunGroupByNameInitialMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY name HAVING matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY com HAVING por nome e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByNameHavingMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "GroupByNameHavingMatrix",
            (1, "Alice"), (2, "Alice"), (3, "Bob"), (4, "Bob"), (5, "Bob"), (6, "Charlie"));
        var value = state.Service.RunGroupByNameHavingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the GROUP BY ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz GROUP BY por ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunGroupByOrdinalMatrix()
    {
        if (!Dialect.SupportsGroupByOrdinal)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "GroupByOrdinalMatrix",
            (1, "Alice"), (2, "Adam"), (3, "Alice"), (4, "Bob"), (5, "Brian"), (6, "Bob"), (7, "Carla"), (8, "Chris"));
        var value = state.Service.RunGroupByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT order-by-ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("DistinctOrderByOrdinalMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var value = state.Service.RunDistinctOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DISTINCT text-filter order-by-ordinal matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com filtro de texto e ORDER BY ordinal e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunDistinctLikeOrderByOrdinalMatrix()
    {
        var state = GetPreparedUsersQueryState("DistinctLikeOrderByOrdinalMatrix", (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var value = state.Service.RunDistinctLikeOrderByOrdinalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined typed-expression matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com expressoes tipadas em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinTypedExpressionMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinTypedExpressionMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinTypedExpressionMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined null-aggregate matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz agregada com null em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinNullAggregateMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinNullAggregateMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinNullAggregateMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined cast-null matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com cast e null em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinCastNullMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinCastNullMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinCastNullMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined cast-text comparison matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com cast e comparacao textual em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinCastTextComparisonMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinCastTextComparisonMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinCastTextComparisonMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined HAVING cast matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz HAVING com cast em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinHavingCastMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinHavingCastMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinHavingCastMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined length-and-numeric matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com comprimento e numericos em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinLengthNumericMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinLengthNumericMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinLengthNumericMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined text-case-length matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz com caixa, texto e comprimento em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinTextCaseLengthMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinTextCaseLengthMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinTextCaseLengthMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined distinct-case matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com CASE em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinDistinctCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinDistinctCaseMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinDistinctCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined distinct-HAVING matrix benchmark and keeps the snapshot alive.
    /// PT-br: Executa o benchmark da matriz DISTINCT com HAVING em join e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunJoinDistinctHavingMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "JoinDistinctHavingMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinDistinctHavingMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the STRING_SPLIT projection benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark de projecao STRING_SPLIT e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunStringSplitProjection()
    {
        if (!Dialect.SupportsApplyClause || !Dialect.SupportsStringSplitFunction)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("StringSplitProjection");
        try
        {
            state.Repo.ExecuteNonQueryAsync($"INSERT INTO {state.Context.TbUsersFullName} (Id, Name, Email) VALUES (3, 'Csv', 'red,blue')").GetAwaiter().GetResult();
            var value = state.Service.RunStringSplitProjectionAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 3").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the FOR JSON PATH projection benchmark and keeps the serialized payload alive.
    /// PT-br: Executa o benchmark de projecao FOR JSON PATH e mantem o payload serializado ativo.
    /// </summary>
    protected virtual void RunForJsonPathProjection()
    {
        if (!Dialect.SupportsForJsonClause)
        {
            return;
        }

        var state = GetPreparedSelectTableQueryState("ForJsonPathProjection");
        try
        {
            state.Repo.ExecuteNonQueryAsync($"INSERT INTO {state.Context.TbUsersFullName} (Id, Name) VALUES (2, 'Bob')").GetAwaiter().GetResult();
            var value = state.Service.RunForJsonPathProjectionAsync().GetAwaiter().GetResult();
            GC.KeepAlive(value);
        }
        finally
        {
            state.Repo.ExecuteNonQueryAsync($"DELETE FROM {state.Context.TbUsersFullName} WHERE Id = 2").GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// EN: Executes the joined window and temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz com janela e temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunJoinWindowTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunJoinTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined window matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz de janela em join e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunJoinWindowMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the joined window, aggregate, and temporal matrix benchmark and keeps the projected snapshot alive.
    /// PT-br: Executa o benchmark da matriz com janela, agregacao e temporal em join e mantem o snapshot projetado ativo.
    /// </summary>
    protected virtual void RunJoinWindowAggregateTemporalMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(10, 1, "A"), (11, 1, "B"), (12, 2, "C")]);
        var value = state.Service.RunJoinWindowAggregateTemporalMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the APPLY and temporal composite benchmark by chaining the shared APPLY and temporal queries.
    /// PT-br: Executa o benchmark composto de APPLY e temporal encadeando as consultas compartilhadas de APPLY e temporal.
    /// </summary>
    protected virtual void RunApplyTemporalComposite()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunJoinTemporalMatrix();
    }

    /// <summary>
    /// EN: Executes the APPLY and window-temporal composite benchmark by chaining the shared APPLY and window queries.
    /// PT-br: Executa o benchmark composto de APPLY e janela-temporal encadeando as consultas compartilhadas de APPLY e janela.
    /// </summary>
    protected virtual void RunApplyWindowTemporalComposite()
    {
        RunCrossApplyProjection();
        RunOuterApplyProjection();
        RunJoinWindowMatrix();
        RunJoinWindowTemporalMatrix();
    }

    /// <summary>
    /// EN: Updates a user row by primary key and validates the stored value.
    /// PT-br: Atualiza uma linha de usuário pela chave primária e valida o valor armazenado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpdateByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpdateByPk(1);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Deletes a user row by primary key and validates the remaining row count.
    /// PT-br: Exclui uma linha de usuário pela chave primária e valida a contagem de linhas restante.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunDeleteByPk()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var count = state.RunDeleteByPk(1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an update/delete round trip and validates the remaining row count.
    /// PT-br: Executa um ciclo de update/delete e valida a contagem de linhas restante.
    /// </summary>
    protected virtual void RunUpdateDeleteRoundTrip()
    {
        var state = GetPreparedCrudUsersState("UpdateDeleteRoundTrip");
        var count = state.RunUpdateDeleteRoundTrip(1, 2);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter update/delete round-trip benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de roundtrip de update/delete com parametros e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunParameterUpdateDeleteRoundTrip()
        => RunUpdateDeleteRoundTrip();

    /// <summary>
    /// EN: Executes an insert inside a transaction, commits it, and validates the committed result.
    /// PT-br: Executa uma inserção dentro de uma transação, confirma a operação e valida o resultado confirmado.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionCommit()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionCommit();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an insert inside a transaction, rolls it back, and validates that no rows remain.
    /// PT-br: Executa uma inserção dentro de uma transação, desfaz a operação e valida que nenhuma linha permaneceu.
    /// </summary>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunTransactionRollback()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunTransactionRollback();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes an update/delete workflow inside a transaction and validates the committed result.
    /// PT-br: Executa um fluxo de update/delete dentro de uma transação e valida o resultado confirmado.
    /// </summary>
    protected virtual void RunTransactionalUpdateDeleteCommit()
    {
        var state = GetPreparedCrudUsersState("TransactionalUpdateDeleteCommit");
        var count = state.RunTransactionalUpdateDeleteCommit(1, 2);
        GC.KeepAlive(count);
    }

    protected virtual void RunSavepointCreate()
    {
        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunSavepointCreate();
    }

    protected virtual void RunRollbackToSavepoint()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunRollbackToSavepoint();
        GC.KeepAlive(count);
    }

    protected virtual void RunReleaseSavepoint()
    {
        if (!Dialect.SupportsReleaseSavepoints)
        {
            return;
        }

        var state = GetPreparedNoopMutationState("NoopMutation");
        state.Service.RunReleaseSavepoint();
    }

    /// <summary>
    /// EN: Executes the provider-specific upsert path and validates the updated value.
    /// PT-br: Executa o caminho de upsert específico do provedor e valida o valor atualizado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    /// <exception cref="InvalidOperationException"></exception>
    protected virtual void RunUpsert()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var value = state.RunUpsert(1);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the merge insert-then-update benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de merge de inserir e depois atualizar e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMergeInsertThenUpdate()
    {
        if (!Dialect.SupportsMerge)
        {
            return;
        }

        var state = GetPreparedMergeUsersState("MergeInsertThenUpdate");
        var value = state.RunMergeInsertThenUpdate();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the upsert insert-then-update benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de upsert de inserir e depois atualizar e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunUpsertInsertThenUpdate()
    {
        if (!Dialect.SupportsUpsert)
        {
            return;
        }

        var state = GetPreparedMergeUsersState("UpsertInsertThenUpdate");
        var value = state.RunUpsertInsertThenUpdate();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameter binding benchmark using a scalar projection query.
    /// PT-br: Executa um benchmark de binding de parametros usando uma consulta de projeção escalar.
    /// </summary>
    protected virtual void RunParameterProjection()
    {
        var state = GetPreparedParameterProjectionState("ParameterProjection");
        var value = state.RunParameterProjection();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameter binding benchmark using a single-row INSERT statement.
    /// PT-br: Executa um benchmark de binding de parametros usando uma instrucao INSERT de uma linha.
    /// </summary>
    protected virtual void RunParameterInsertSingle()
    {
        var state = GetPreparedInsertUsersState("ParameterInsertSingle");
        var count = state.RunParameterInsertSingle();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter insert round-trip benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de roundtrip de insert com parametros e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunParameterInsertRoundTrip()
    {
        var state = GetPreparedParameterInsertUsersState("ParameterInsertRoundTrip");
        var count = state.RunParameterInsertRoundTrip();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the parameter insert round-trip benchmark with null values and keeps the provider result alive.
    /// PT: Executa o benchmark de roundtrip de insert com parametros e valores nulos e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunParameterInsertNullRoundTrip()
    {
        var state = GetPreparedParameterInsertUsersState("ParameterInsertNullRoundTrip");
        var count = state.RunParameterInsertNullRoundTrip();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes a parameterized lookup by name benchmark.
    /// PT: Executa um benchmark de consulta parametrizada por nome.
    /// </summary>
    protected virtual void RunParameterSelectByNameMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "ParameterSelectByNameMatrix",
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Delta"),
            (5, "Echo"));
        var value = state.Service.RunParameterSelectByNameMatrixAsync("Bob").GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a parameterized lookup by id benchmark.
    /// PT: Executa um benchmark de consulta parametrizada por id.
    /// </summary>
    protected virtual void RunParameterSelectByIdMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "ParameterSelectByIdMatrix",
            (1, "Alice"),
            (2, "Bob"),
            (3, "Charlie"),
            (4, "Delta"),
            (5, "Echo"));
        var value = state.Service.RunParameterSelectByIdMatrixAsync(2, "Bob").GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed parameter round-trip benchmark.
    /// PT: Executa um benchmark de roundtrip de parametros tipados.
    /// </summary>
    protected virtual void RunParameterRoundTripMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("ParameterRoundTripMatrix");
        var value = state.RunParameterRoundTripMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed parameter projection benchmark.
    /// PT: Executa um benchmark de projeção de parametros tipados.
    /// </summary>
    protected virtual void RunParameterTypeMatrix()
    {
        var state = GetPreparedParameterMatrixState("ParameterTypeMatrix");
        var value = state.RunParameterTypeMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a typed date and currency parameter benchmark.
    /// PT: Executa um benchmark de data e moeda com parametros tipados.
    /// </summary>
    protected virtual void RunParameterDateCurrencyMatrix()
    {
        var state = GetPreparedParameterMatrixState("ParameterDateCurrencyMatrix");
        var value = state.RunParameterDateCurrencyMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field storage matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de armazenamento tipado e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldStorageMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldStorageMatrix");
        var snapshot = state.RunTypedFieldStorageMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field function matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de funcoes tipadas e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldFunctionMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldFunctionMatrix");
        var snapshot = state.RunTypedFieldFunctionMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field calculation matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de calculo tipado e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldCalculationMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCalculationMatrix");
        var snapshot = state.RunTypedFieldCalculationMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field and function blend benchmark and keeps the validated count alive.
    /// PT-br: Executa o benchmark de mistura de campos tipados e funcoes e mantem a contagem validada ativa.
    /// </summary>
    protected virtual void RunTypedFieldAndFunctionBlend()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldAndFunctionBlend");
        var value = state.RunTypedFieldAndFunctionBlend();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field compound predicate matrix benchmark and keeps the validated count alive.
    /// PT-br: Executa o benchmark da matriz de predicados compostos com campos tipados e mantem a contagem validada ativa.
    /// </summary>
    protected virtual void RunTypedFieldCompoundPredicateMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCompoundPredicateMatrix");
        var value = state.RunTypedFieldCompoundPredicateMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the typed field cast calculation matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de calculo com casts em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldCastCalculationMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldCastCalculationMatrix");
        var snapshot = state.RunCastCalculationMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field null comparison matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de comparacao com null em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldNullComparisonMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldNullComparisonMatrix");
        var snapshot = state.RunNullComparisonMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field text length matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de comprimento de texto em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldTextLengthMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldTextLengthMatrix");
        var snapshot = state.RunTextLengthMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field text case matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de caixa de texto em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldTextCaseMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldTextCaseMatrix");
        var snapshot = state.RunTextCaseMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the typed field predicate matrix benchmark and keeps the validated snapshot alive.
    /// PT: Executa o benchmark da matriz de predicados em campos tipados e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTypedFieldPredicateMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TypedFieldPredicateMatrix");
        var snapshot = state.RunTypedFieldPredicateMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes typed parameter inserts inside a committed transaction and validates the persisted rows.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transação confirmada e valida as linhas persistidas.
    /// </summary>
    protected virtual void RunParameterTransactionCommit()
    {
        var state = GetPreparedParameterTransactionUsersState("ParameterTransactionCommit");
        var count = state.RunParameterTransactionCommit();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes typed parameter inserts inside a rolled-back transaction and validates that no rows remain.
    /// PT-br: Executa inserts tipados com parametros dentro de uma transação revertida e valida que nenhuma linha permaneceu.
    /// </summary>
    protected virtual void RunParameterTransactionRollback()
    {
        var state = GetPreparedParameterTransactionUsersState("ParameterTransactionRollback");
        var count = state.RunParameterTransactionRollback();
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes a stored procedure call benchmark.
    /// PT-br: Executa um benchmark de chamada de procedimento armazenado.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStoredProcedureCall()
    {
        throw new NotSupportedException($"{Dialect.DisplayName} does not support stored procedure benchmarks.");
    }

    /// <summary>
    /// EN: Creates a temporary sequence and reads its next value.
    /// PT-br: Cria uma sequência temporária e lê o seu próximo valor.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunSequenceNextValue()
    {
        var state = GetPreparedSequenceState("SequenceNextValue");
        var value = state.RunSequenceNextValue();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceCurrentValue()
    {
        var state = GetPreparedSequenceState("SequenceCurrentValue");
        var value = state.RunSequenceCurrentValue();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceInsertRoundTrip()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertRoundTrip");
        var value = state.RunSequenceInsertRoundTrip();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceInsertExpression()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertExpression");
        var value = state.RunSequenceInsertExpression();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceSelectProjection()
    {
        var state = GetPreparedSequenceState("SequenceSelectProjection");
        var value = state.RunSequenceSelectProjection();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceExpressionFilter()
    {
        var state = GetPreparedSequenceExpressionFilterState("SequenceExpressionFilter");
        var value = state.RunSequenceExpressionFilter();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceCaseWhereMatrix()
    {
        var state = GetPreparedSequenceState("SequenceCaseWhereMatrix");
        var value = state.RunSequenceCaseWhereMatrix();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceTemporalMatrix()
    {
        var state = GetPreparedSequenceState("SequenceTemporalMatrix");
        var value = state.RunSequenceTemporalMatrix();
        GC.KeepAlive(value);
    }

    protected virtual void RunSequenceJoinAggregate()
    {
        var state = GetPreparedSequenceState("SequenceJoinAggregate");
        var value = state.RunSequenceJoinAggregate();
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchInsert10()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(10);
        GC.KeepAlive(count);
    }

    protected virtual void RunBatchInsert100()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(100);
        GC.KeepAlive(count);
    }

    protected virtual void RunBatchMixedReadWrite()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchMixedReadWrite(1, 2);
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchScalar()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var second = state.RunBatchScalar(1, 2);
        GC.KeepAlive(second);
    }

    protected virtual void RunBatchNonQuery()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchNonQuery(1, 2, 2, 1);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the provider-specific string aggregation query over sample user names.
    /// PT-br: Executa a consulta de agregação de strings específica do provedor sobre nomes de usuários de exemplo.
    /// </summary>
    /// <exception cref="NotSupportedException"></exception>
    protected virtual void RunStringAggregate()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregate",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateOrdered()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateOrdered",
            (1, "Charlie"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateOrderedAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the provider-specific scalar date/time query.
    /// PT-br: Executa a consulta escalar de data/hora específica do provedor.
    /// </summary>
    protected virtual void RunDateScalar()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunDateScalarAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared math functions benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de funcoes matematicas e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathFunctions()
    {
        if (!Dialect.SupportsMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared explicit-base math LOG benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de logaritmo com base explicita e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathLogBaseFunction()
    {
        if (!Dialect.SupportsMathLogBaseFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathLogBaseFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared LOG2 benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de LOG2 e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathLog2Function()
    {
        if (!Dialect.SupportsMathLog2Function)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathLog2FunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared PI benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de PI e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathPiFunction()
    {
        if (!Dialect.SupportsMathPiFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathPiFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared RAND benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de RAND e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathRandFunction()
    {
        if (!Dialect.SupportsMathRandFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathRandFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared remainder benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de resto e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathRemainderFunction()
    {
        if (!Dialect.SupportsMathRemainderFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathRemainderFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared math truncation benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de truncamento numerico e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathTruncFunction()
    {
        if (!Dialect.SupportsMathTruncFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathTruncFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared cotangent benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark compartilhado de cotangente e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathCotFunction()
    {
        if (!Dialect.SupportsMathCotFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunMathCotFunctionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the MySQL utility math benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de utilitarios matematicos da familia MySQL e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMySqlUtilityMathFunctions()
    {
        if (!Dialect.SupportsMySqlUtilityMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("MySqlUtilityMathFunctions");
        var value = state.Service.RunMySqlUtilityMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared greatest/least/mod benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark compartilhado de greatest/least/mod e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunGreatestLeastModFunctions()
    {
        if (!Dialect.SupportsGreatestLeastModFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("GreatestLeastModFunctions");
        var value = state.Service.RunGreatestLeastModFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the DB2 alias math benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de aliases matematicos do DB2 e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunDb2AliasMathFunctions()
    {
        if (!Dialect.SupportsDb2AliasMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("Db2AliasMathFunctions");
        var value = state.Service.RunDb2AliasMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the Firebird alias math benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de aliases matematicos do Firebird e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunFirebirdAliasMathFunctions()
    {
        if (!Dialect.SupportsFirebirdAliasMathFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("FirebirdAliasMathFunctions");
        var value = state.Service.RunFirebirdAliasMathFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the shared transcendental math benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark compartilhado de matematica transcendental e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunMathTranscendentalFunctions()
    {
        if (!Dialect.SupportsMathTranscendentalFunctions)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("MathTranscendentalFunctions");
        var value = state.Service.RunMathTranscendentalFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonScalarRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonScalarReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterInsert()
    {
        var state = GetPreparedInsertUsersState("RowCountAfterInsert");
        var affected = state.RunRowCountAfterInsert();
        GC.KeepAlive(affected);
    }

    protected virtual void RunRowCountAfterUpdate()
    {
        var state = GetPreparedCrudUsersState("CrudUsers");
        var affected = state.RunRowCountAfterUpdate();
        GC.KeepAlive(affected);
    }

    /// <summary>
    /// EN: Tries to drop a table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The table name targeted by the operation. PT-br: O nome da tabela alvo da operação.</param>
    protected void SafeDropTable(DbConnection connection, string tableName)
    {
        SafeExecute(connection, Dialect.DropTable(tableName));
    }

    /// <summary>
    /// EN: Tries to drop a temporary table using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma tabela temporaria usando uma limpeza de melhor esforco.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexao de banco de dados usada para executar a operação.</param>
    /// <param name="tableName">EN: The temporary table name targeted by the operation. PT-br: O nome da tabela temporaria alvo da operação.</param>
    protected void SafeDropTemporaryTable(DbConnection connection, FidelityTestContext context)
    {
        SafeExecute(connection, Dialect.DropTemporaryUsersTable(context));
    }

    /// <summary>
    /// EN: Tries to drop a sequence using best-effort cleanup semantics.
    /// PT-br: Tenta remover uma sequência usando uma limpeza de melhor esforço.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sequenceName">EN: The sequence name targeted by the operation. PT-br: O nome da sequência alvo da operação.</param>
    protected void SafeDropSequence(DbConnection connection, FidelityTestContext context)
    {
        SafeExecute(connection, Dialect.DropSequence(context));
    }

    /// <summary>
    /// EN: Executes a cleanup command while suppressing cleanup failures.
    /// PT-br: Executa um comando de limpeza suprimindo falhas durante a limpeza.
    /// </summary>
    /// <param name="connection">EN: The database connection used to execute the operation. PT-br: A conexão de banco de dados usada para executar a operação.</param>
    /// <param name="sql">EN: The SQL command text to execute. PT-br: O texto do comando SQL a ser executado.</param>
    protected void SafeExecute(DbConnection connection, string sql)
    {
        try
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }
        catch (Exception ex)
        {
            var root = ex.GetBaseException();
            if (IsDb2MissingObjectException(root) || IsOracleMissingObjectException(root))
            {
                return;
            }

            var message = root is NotSupportedException
                ? $"[SAFE-{root.GetType().Name}] {sql}: {root.Message}{Environment.NewLine}{Environment.NewLine}"
                : $"[SAFE-{root.GetType().Name}] {sql}: {root.Message} -- {ex.StackTrace}{Environment.NewLine}{Environment.NewLine}";

            Console.WriteLine(message);

            lock (_logSync)
            {
                var errorKey = $"{GetType().FullName}|{Dialect.DisplayName}|SAFE|{sql}|{root.GetType().FullName}|{root.Message}";
                if (Errors.TryGetValue(errorKey, out int value))
                {
                    Errors[errorKey] = value + 1;
                    return;
                }

                Errors.GetOrAdd(errorKey, 0);

                var file = BenchmarkLogPath.GetFilePath($"{GetType().FullName}-{Dialect.DisplayName}-errors.log");
                Directory.CreateDirectory(BenchmarkLogPath.GetDirectory());
                File.AppendAllText(
                    file,
                    message + Environment.NewLine);
            }
        }
    }

    private static bool IsDb2MissingObjectException(Exception ex)
        => ex is DB2Exception
            && (ex.Message.Contains("SQL0204N", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("42704", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("undefined name", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("not found", StringComparison.OrdinalIgnoreCase));

    private static bool IsOracleMissingObjectException(Exception ex)
        => ex is OracleException
            && (ex.Message.Contains("ORA-00942", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("table or view", StringComparison.OrdinalIgnoreCase)
                || ex.Message.Contains("tabela ou view", StringComparison.OrdinalIgnoreCase));


    protected virtual void RunNestedSavepointFlow()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunNestedSavepointFlow();
        GC.KeepAlive(count);
    }

    protected virtual void RunStringAggregateDistinct()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateDistinct",
            (1, "Bob"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunStringAggregateDistinctAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateCustomSeparator()
    {
        var state = GetPreparedUsersQueryState(
            "StringAggregateCustomSeparator",
            (1, "Bob"), (2, "Alice"));
        var value = state.Service.RunStringAggregateCustomSeparatorAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateLargeGroup()
    {
        var seedRows = new (int id, string name)[50];
        for (var i = 1; i <= 50; i++)
        {
            seedRows[i - 1] = (i, $"User-{i}");
        }

        var state = GetPreparedUsersQueryState("StringAggregateLargeGroup", seedRows);
        var value = state.Service.RunStringAggregateLargeGroupAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunStringAggregateSummaryMatrix()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "StringAggregateSummaryMatrix",
            (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var snapshot = state.Service.RunStringAggregateSummaryMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunStringAggregateGroupCaseMatrix()
    {
        if (!Dialect.SupportsStringAggregate)
        {
            return;
        }

        var state = GetPreparedUsersQueryState(
            "StringAggregateGroupCaseMatrix",
            (1, "Charlie"), (2, "Bob"), (3, "Alice"), (4, "Bob"), (5, "Delta"));
        var snapshot = state.Service.RunStringAggregateGroupCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunStringAggregationSummaryMatrix()
        => RunStringAggregateSummaryMatrix();

    protected virtual void RunStringAggregationGroupCaseMatrix()
        => RunStringAggregateGroupCaseMatrix();

    /// <summary>
    /// EN: Executes the full string aggregation variants benchmark and keeps the provider results alive.
    /// PT: Executa o benchmark completo das variantes de agregacao de strings e mantem os resultados do provedor vivos.
    /// </summary>
    protected virtual void RunStringAggregationVariants()
    {
        RunStringAggregate();
        RunStringAggregateOrdered();
        RunStringAggregateDistinct();
        RunStringAggregateCustomSeparator();
        RunStringAggregateLargeGroup();
    }

    protected virtual void RunTemporalCurrentTimestamp()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalCurrentTimestampAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalDateAdd()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunTemporalDateAddAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowWhere()
    {
        var state = GetPreparedUsersQueryState("TemporalNowWhere", (1, "Alice"));
        var value = state.Service.RunTemporalNowWhereAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalNowOrderBy()
    {
        var state = GetPreparedUsersQueryState("TemporalNowOrderBy", (1, "Bob"), (2, "Alice"));
        var value = state.Service.RunTemporalNowOrderByAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the scalar temporal matrix benchmark and keeps the provider results alive.
    /// PT: Executa a matriz temporal escalar e mantem os resultados do provedor vivos.
    /// </summary>
    protected virtual void RunScalarTemporalMatrix()
    {
        RunDateScalar();
        RunTemporalCurrentTimestamp();
        RunTemporalDateAdd();
        RunTemporalNowWhere();
        RunTemporalNowOrderBy();
    }

    /// <summary>
    /// EN: Executes the temporal field matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de campos temporais e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalFieldMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalFieldMatrix");
        var snapshot = state.RunTemporalFieldMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the temporal comparison matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de comparacao temporal e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalComparisonMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalComparisonMatrix");
        var snapshot = state.RunTemporalComparisonMatrix();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Executes the temporal arithmetic matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de aritmetica temporal e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunTemporalArithmeticMatrix()
    {
        var state = GetPreparedTypedFieldStorageMatrixState("TemporalArithmeticMatrix");
        var snapshot = state.RunTemporalArithmeticMatrix();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunTemporalDateTrunc()
    {
        if (!Dialect.SupportsSqlServerDateFunction("DATETRUNC"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalDateTruncAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalTimeZoneOffset()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("TODATETIMEOFFSET"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalTimeZoneOffsetAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalFromParts()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("DATEFROMPARTS"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalFromPartsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalEndOfMonth()
    {
        if (!Dialect.SupportsSqlServerDateFunction("EOMONTH"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalEndOfMonthAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunTemporalDateDiffBig()
    {
        if (!Dialect.SupportsSqlServerDateFunction("DATEDIFF_BIG"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTemporalDateDiffBigAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerMetadataFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerMetadataFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunScopeIdentity()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunScopeIdentityAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerSystemFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSystemFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerSpecialFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSpecialFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerContextFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerContextFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerTransactionStateFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerTransactionStateFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerSessionFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerSessionFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunStringBasicFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringBasicFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunParseFamily()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunParseFamilyAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSoundex()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSoundexAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunCompression()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunCompressionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunApproxCountDistinct()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunApproxCountDistinctAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunPercentileAggregateFunctions()
    {
        if (!Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_CONT")
            || !Dialect.SupportsSqlServerAggregateFunction("PERCENTILE_DISC"))
        {
            return;
        }

        var state = GetPreparedUsersQueryState("PercentileAggregateFunctions", (1, "Ana"), (2, "Bob"));
        var value = state.Service.RunPercentileAggregatesAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSqlServerAggregateFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunSqlServerAggregateFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonPathReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON missing-path benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de caminho JSON ausente e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonMissingPathRead()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonMissingPathReadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the missing JSON path benchmark and keeps the provider result alive when it is null.
    /// PT: Executa o benchmark de caminho JSON ausente e mantem o resultado do provedor vivo quando ele eh nulo.
    /// </summary>
    protected virtual void RunJsonMissingPathReturnsNull()
        => RunJsonMissingPathRead();

    /// <summary>
    /// EN: Executes the JSON_QUERY root-fragment benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de fragmento raiz JSON_QUERY e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonQueryRootFragment()
    {
        if (!Dialect.SupportsJsonQueryFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonQueryRootFragmentAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON_MODIFY replacement benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de substituicao JSON_MODIFY e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunJsonModifyReplace()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("JSON_MODIFY"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonModifyReplaceAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON typed field matrix benchmark and keeps the validated snapshot alive.
    /// PT-br: Executa o benchmark da matriz de campos tipados com JSON e mantem o snapshot validado ativo.
    /// </summary>
    protected virtual void RunJsonTypedFieldMatrix()
    {
        if (!Dialect.SupportsJsonScalarRead)
        {
            return;
        }

        var state = GetPreparedTypedFieldStorageMatrixState("JsonTypedFieldMatrix");
        var snapshot = state.RunJsonTypedFieldMatrix();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunJsonEachFromArray()
    {
        if (!Dialect.SupportsJsonEachFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonEachFromArrayAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonEachFromObject()
    {
        if (!Dialect.SupportsJsonEachFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonEachFromObjectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunJsonTreeStructure()
    {
        if (!Dialect.SupportsJsonTreeFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunJsonTreeStructureAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunOpenJsonArray()
    {
        if (!Dialect.SupportsOpenJsonFunction)
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunOpenJsonArrayAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountAfterSelect()
    {
        var state = GetPreparedUsersQueryState("RowCountAfterSelect", (1, "Alice"), (2, "Bob"));
        var count = state.Service.RunRowCountAfterSelectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(count);
    }

    protected virtual void RunCteSimple()
    {
        var state = GetPreparedUsersQueryState("CteSimple", (1, "Alice"));
        var value = state.Service.RunCteSimpleAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowRowNumber()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRowNumber",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowRowNumberAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowLag()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLag",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLagAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowLead()
    {
        var state = GetPreparedUsersQueryState(
            "WindowLead",
            (1, "Bob"), (2, "Alice"), (3, "Charlie"));
        var value = state.Service.RunWindowLeadAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowRankDenseRank()
    {
        var state = GetPreparedUsersQueryState(
            "WindowRankDenseRank",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowRankDenseRank("Aaron");
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowFirstLastValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowFirstLastValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowFirstLastValue("Aaron");
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowNtile()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNtile",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNtile("Aaron");
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowPercentRankCumeDist()
    {
        var state = GetPreparedUsersQueryState(
            "WindowPercentRankCumeDist",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowPercentRankCumeDist("Aaron");
        GC.KeepAlive(value);
    }

    protected virtual void RunWindowNthValue()
    {
        var state = GetPreparedUsersQueryState(
            "WindowNthValue",
            (1, "Aaron"), (2, "Bravo"), (3, "Bravo"), (4, "Charlie"));
        var value = state.Service.RunWindowNthValue("Aaron");
        GC.KeepAlive(value);
    }


    protected virtual void RunBatchReaderMultiResult()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchReaderMultiResult(1, 2);
        GC.KeepAlive(value);
    }

    protected virtual void RunBatchTransactionControl()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchTransactionControl(1, 2);
        GC.KeepAlive(value);
    }

    protected virtual void RunParseSimpleSelect()
    {
        var tokens = ParseServiceTest.RunParseSimpleSelect();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseComplexJoin()
    {
        var tokens = ParseServiceTest.RunParseComplexJoin();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseInsertReturning()
    {
        var tokens = ParseServiceTest.RunParseInsertReturning();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseOnConflictDoUpdate()
    {
        var tokens = ParseServiceTest.RunParseOnConflictDoUpdate();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseJsonExtract()
    {
        var tokens = ParseServiceTest.RunParseJsonExtract();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseStringAggregateWithinGroup()
    {
        var tokens = ParseServiceTest.RunParseStringAggregateWithinGroup();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseAutoDialectTopLimitFetch()
    {
        var tokens = ParseServiceTest.RunParseAutoDialectTopLimitFetch();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunParseMultiStatementBatch()
    {
        var tokens = ParseServiceTest.RunParseMultiStatementBatch();
        GC.KeepAlive(tokens);
    }

    protected virtual void RunJsonInsertCast()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var service = state.Service;
        var value = service.RunJsonInsertCastAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the JSON insert cast benchmark and keeps the provider result alive when it is null.
    /// PT: Executa o benchmark de insert e cast de JSON e mantem o resultado do provedor vivo quando ele eh nulo.
    /// </summary>
    protected virtual void RunJsonInsertCastReturnsNull()
        => RunJsonInsertCast();

    /// <summary>
    /// EN: Executes the SQL Server string utility benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de utilitarios de string do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunStringUtilityFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringUtilityFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server string metadata benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark de metadados de string do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunStringMetadataFunctions()
    {
        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringMetadataFunctionsAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server STRING_ESCAPE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark STRING_ESCAPE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunStringEscape()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("STRING_ESCAPE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunStringEscapeAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server TRANSLATE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark TRANSLATE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunTranslate()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("TRANSLATE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunTranslateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMATMESSAGE benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMATMESSAGE do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunFormatMessage()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("FORMATMESSAGE"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunFormatMessageAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server ISJSON benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark ISJSON do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunIsJson()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("ISJSON"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunIsJsonAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the SQL Server FORMAT benchmark and keeps the provider result alive.
    /// PT-br: Executa o benchmark FORMAT do SQL Server e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunFormat()
    {
        if (!Dialect.SupportsSqlServerScalarFunction("FORMAT"))
        {
            return;
        }

        var state = GetPreparedNoopQueryState("NoopQuery");
        var value = state.Service.RunFormatAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunRowCountInBatch()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunRowCountInBatch(1, 2);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the batch row-count benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de contagem de linhas em lote e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunBatchRowCountInBatch()
        => RunRowCountInBatch();

    protected virtual void RunPivotCount()
    {
        var state = GetPreparedUsersQueryState("PivotCount", (1, "Alice"), (2, "Bob"));
        var value = state.Service.RunPivotCountAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunReturningInsert()
    {
        if (Dialect.Provider != ProviderId.MariaDb)
        {
            // Keep the benchmark runnable on providers without RETURNING/OUTPUT support.
            RunInsertSingle();
            return;
        }

        var state = GetPreparedReturningInsertState("ReturningInsert");
        var value = state.RunReturningInsert();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the batch returning insert benchmark and keeps the provider result alive.
    /// PT: Executa o benchmark de batch returning insert e mantem o resultado do provedor vivo.
    /// </summary>
    protected virtual void RunBatchReturningInsert()
        => RunReturningInsert();

    protected virtual void RunReturningUpdate()
    {
        // Keep the benchmark entry as an alias of the plain update/readback flow.
        RunUpdateByPk();
    }

    protected virtual void RunMergeBasic()
    {
        // Keep the benchmark entry as an alias of the upsert flow.
        RunUpsert();
    }

    protected virtual void RunPartitionPruningSelect()
    {
        var seedRows = new (int id, string name)[20];
        for (var i = 1; i <= 20; i++)
        {
            seedRows[i - 1] = (i, $"User{i:00}");
        }

        var state = GetPreparedUsersQueryState("PartitionPruningSelect", seedRows);
        var value = state.Service.RunPartitionPruningSelectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }


    protected virtual void RunSelectExistsPredicate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectExistsPredicateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectNotExistsPredicate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectNotExistsPredicateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes a LEFT JOIN anti-join benchmark and validates the orphan row count.
    /// PT-br: Executa um benchmark de anti-join com LEFT JOIN e valida a contagem de linhas sem correspondencia.
    /// </summary>
    protected virtual void RunSelectLeftJoinAntiJoin()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectLeftJoinAntiJoinAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectCorrelatedCount()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectCorrelatedCountAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectScalarCaseMatrix()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersScalarCaseMatrix",
            [(1, "Alice"), (2, "Bob"), (3, "Carla")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 1, "o-3"), (4, 2, "o-4")]);
        var value = state.Service.RunSelectScalarCaseMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunGroupByHaving()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunGroupByHavingAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunUnionAllProjection()
    {
        var state = GetPreparedUsersQueryState("UnionAllProjection", (1, "Alice"), (2, "Bob"));
        var value = state.Service.RunUnionAllProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunUnionDistinctProjection()
    {
        var state = GetPreparedUsersQueryState("UnionDistinctProjection", (1, "Alice"), (2, "Bob"), (3, "Charlie"));
        var value = state.Service.RunUnionDistinctProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunDistinctProjection()
    {
        var state = GetPreparedUsersQueryState(
            "DistinctProjection",
            (1, "Alice"), (2, "Alice"), (3, "Bob"));
        var value = state.Service.RunDistinctProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunMultiJoinAggregate()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunMultiJoinAggregateAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectScalarSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectScalarSubqueryAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectInSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectInSubqueryAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectNotInSubquery()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunSelectNotInSubqueryAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunSelectBetweenLikeOrderByMatrix()
    {
        var state = GetPreparedUsersQueryState(
            "BetweenLikeOrderByMatrix",
            (1, "Aaron"),
            (2, "Alice"),
            (3, "Bob"),
            (4, "Charlie"),
            (5, "Delta"));
        var value = state.Service.RunBetweenLikeOrderByMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunCrossApplyProjection()
    {
        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunCrossApplyProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunOuterApplyProjection()
    {
        if (!Dialect.SupportsOuterApplyProjection)
        {
            return;
        }

        var state = GetPreparedUsersOrdersQueryState(
            "UsersOrdersThreeRows",
            [(1, "Alice"), (2, "Bob"), (3, "Charlie")],
            [(1, 1, "o-1"), (2, 1, "o-2"), (3, 2, "o-3")]);
        var value = state.Service.RunOuterApplyProjectionAsync().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunPagedNameProjection()
    {
        var state = GetPreparedUsersQueryState(
            "PagedNameProjection",
            (1, "Charlie"),
            (2, "Bravo"),
            (3, "Alice"),
            (4, "Delta"),
            (5, "Echo"));
        var count = state.Service.RunPagedNameProjectionMatrixAsync().GetAwaiter().GetResult();
        GC.KeepAlive(count);
    }

    protected virtual void RunExecutionPlan()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedExecutionPlanState("ExecutionPlan", seedRows);
        var plan = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plan);
    }

    protected virtual void RunExecutionPlanSelect()
    {
        RunExecutionPlan();
    }

    protected virtual void RunExecutionPlanJoin()
    {
        (int id, string name)[] seedUsers = [(1, "Alice")];
        (int id, int userId, string order)[] seedOrders = [(1, 1, "order-1")];
        var state = GetPreparedExecutionPlanJoinState(
            "ExecutionPlanJoin",
            seedUsers,
            seedOrders);
        var plan = state.Service.RunTestAsync(seedUsers[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plan);
    }

    protected virtual void RunExecutionPlanDml()
    {
        var state = GetPreparedExecutionPlanDmlState("ExecutionPlanDml");
        var plan = state.RunExecutionPlanDml();
        GC.KeepAlive(plan);
    }

    protected virtual void RunDebugTraceSelect()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedDebugTraceSelectState("DebugTraceSelect", seedRows);
        var trace = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(trace);
    }

    protected virtual void RunDebugTraceBatch()
    {
        var state = GetPreparedDebugTraceBatchState("DebugTraceBatch");
        var trace = state.Service.RunTestAsync(2, 3).GetAwaiter().GetResult();
        GC.KeepAlive(trace);
    }

    protected virtual void RunDebugTraceJson()
    {
        var json = DebugTraceJsonServiceTest.RunDebugTraceJson(Dialect.DisplayName, Engine.ToString());
        GC.KeepAlive(json);
    }

    protected virtual void RunLastExecutionPlansHistory()
    {
        (int id, string name)[] seedRows = [(1, "Alice")];
        var state = GetPreparedLastExecutionPlansHistoryState("LastExecutionPlansHistory", seedRows);
        var plans = state.Service.RunTestAsync(seedRows[0].id).GetAwaiter().GetResult();
        GC.KeepAlive(plans);
    }

    protected virtual void RunTempTableCreateAndUse()
    {
        var state = GetPreparedTemporaryTableSourceState("TempTableSource");
        var rows = state.Service.RunCreateTemporaryTableAsSelectThenSelectAsync().GetAwaiter().GetResult();
        GC.KeepAlive(rows);
    }

    protected virtual void RunTempTableRollback()
    {
        var state = GetPreparedTemporaryUsersState("TempUsers");
        state.Service.RunTempTableRollback().GetAwaiter().GetResult();
    }

    protected virtual void RunTempTableCrossConnectionIsolation()
    {
        var state = GetPreparedTemporaryUsersState("TempUsersIsolation");
        var value = state.Service.RunTemporaryTableCrossConnectionIsolation().GetAwaiter().GetResult();
        GC.KeepAlive(value);
    }

    protected virtual void RunResetVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleResetVolatileDataServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }

    protected virtual void RunResetAllVolatileData()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleResetAllVolatileDataServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }

    protected virtual void RunConnectionReopenAfterClose()
    {
        using var connection = CreateConnection();
        connection.Open();
        using var repo = new RepoService(() => connection, Dialect);
        var context = new FidelityTestContext();
        var service = new ConnectionLifecycleReopenAfterServiceTest(repo, context);
        var result = service.RunTestAsync().GetAwaiter().GetResult();
        GC.KeepAlive(result);
    }

    protected virtual void RunSchemaSnapshotExport()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var snapshot = state.Service.RunSchemaSnapshotExport();
        GC.KeepAlive(snapshot);
    }

    protected virtual void RunSchemaSnapshotToJson()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var json = state.Service.RunSchemaSnapshotToJson();
        GC.KeepAlive(json);
    }

    protected virtual void RunSchemaSnapshotLoadJson()
    {
        var obj = SchemaSnapshotServiceOpsTest.RunSchemaSnapshotLoadJson(Dialect.DisplayName);
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotApply()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var applied = state.Service.RunSchemaSnapshotApply();
        GC.KeepAlive(applied);
    }

    protected virtual void RunSchemaSnapshotRoundTrip()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var obj = state.Service.RunSchemaSnapshotRoundTrip();
        GC.KeepAlive(obj);
    }

    protected virtual void RunSchemaSnapshotCompare()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var comparison = state.Service.RunSchemaSnapshotCompare();
        GC.KeepAlive(comparison);
    }

    protected virtual void RunFluentSchemaBuild()
    {
        var model = FluentServiceTest.BuildFluentSchemaBuild();
        GC.KeepAlive(model);
    }

    protected virtual void RunFluentSeed100()
    {
        var rows = FluentServiceTest.BuildFluentSeed100();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentSeed1000()
    {
        var rows = FluentServiceTest.BuildFluentSeed1000();
        GC.KeepAlive(rows);
    }

    protected virtual void RunFluentScenarioCompose()
    {
        var scenario = FluentServiceTest.BuildFluentScenarioCompose();
        GC.KeepAlive(scenario);
    }

}
