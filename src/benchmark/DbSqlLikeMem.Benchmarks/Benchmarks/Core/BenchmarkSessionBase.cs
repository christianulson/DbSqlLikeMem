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
            case BenchmarkFeatureId.CreateTable:
                RunCreateTable();
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

    /// <summary>
    /// EN: Reads the current value of a temporary sequence and keeps the result alive.
    /// PT-br: Lê o valor atual de uma sequência temporária e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceCurrentValue()
    {
        var state = GetPreparedSequenceState("SequenceCurrentValue");
        var value = state.RunSequenceCurrentValue();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence insert round-trip benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de round-trip de insert com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceInsertRoundTrip()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertRoundTrip");
        var value = state.RunSequenceInsertRoundTrip();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence insert expression benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de expressao de insert com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceInsertExpression()
    {
        var state = GetPreparedSequenceUsersState("SequenceInsertExpression");
        var value = state.RunSequenceInsertExpression();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence select projection benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de projeção select com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceSelectProjection()
    {
        var state = GetPreparedSequenceState("SequenceSelectProjection");
        var value = state.RunSequenceSelectProjection();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence expression-filter benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de filtro por expressao com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceExpressionFilter()
    {
        var state = GetPreparedSequenceExpressionFilterState("SequenceExpressionFilter");
        var value = state.RunSequenceExpressionFilter();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence CASE/WHERE matrix benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de matriz CASE/WHERE com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceCaseWhereMatrix()
    {
        var state = GetPreparedSequenceState("SequenceCaseWhereMatrix");
        var value = state.RunSequenceCaseWhereMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence temporal matrix benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de matriz temporal com sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceTemporalMatrix()
    {
        var state = GetPreparedSequenceState("SequenceTemporalMatrix");
        var value = state.RunSequenceTemporalMatrix();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the sequence join-aggregate benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de join com agregacao e sequence e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunSequenceJoinAggregate()
    {
        var state = GetPreparedSequenceState("SequenceJoinAggregate");
        var value = state.RunSequenceJoinAggregate();
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the batch insert benchmark for ten rows and keeps the count alive.
    /// PT-br: Executa o benchmark de insert em lote para dez linhas e mantem a contagem viva.
    /// </summary>
    protected virtual void RunBatchInsert10()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(10);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the batch insert benchmark for one hundred rows and keeps the count alive.
    /// PT-br: Executa o benchmark de insert em lote para cem linhas e mantem a contagem viva.
    /// </summary>
    protected virtual void RunBatchInsert100()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchInsert(100);
        GC.KeepAlive(count);
    }

    /// <summary>
    /// EN: Executes the mixed read-write batch benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark em lote de leitura e escrita mistas e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunBatchMixedReadWrite()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var value = state.RunBatchMixedReadWrite(1, 2);
        GC.KeepAlive(value);
    }

    /// <summary>
    /// EN: Executes the scalar batch benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark em lote escalar e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunBatchScalar()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var second = state.RunBatchScalar(1, 2);
        GC.KeepAlive(second);
    }

    /// <summary>
    /// EN: Executes the non-query batch benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark em lote sem query e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunBatchNonQuery()
    {
        var state = GetPreparedBatchUsersState("BatchUsers");
        var count = state.RunBatchNonQuery(1, 2, 2, 1);
        GC.KeepAlive(count);
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
    /// EN: Executes the row-count-after-insert benchmark and keeps the count alive.
    /// PT-br: Executa o benchmark de contagem de linhas apos insert e mantem a contagem viva.
    /// </summary>
    protected virtual void RunRowCountAfterInsert()
    {
        var state = GetPreparedInsertUsersState("RowCountAfterInsert");
        var affected = state.RunRowCountAfterInsert();
        GC.KeepAlive(affected);
    }

    /// <summary>
    /// EN: Executes the row-count-after-update benchmark and keeps the count alive.
    /// PT-br: Executa o benchmark de contagem de linhas apos update e mantem a contagem viva.
    /// </summary>
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


    /// <summary>
    /// EN: Executes the nested savepoint flow benchmark and keeps the result alive.
    /// PT-br: Executa o benchmark de fluxo aninhado de savepoint e mantem o resultado vivo.
    /// </summary>
    protected virtual void RunNestedSavepointFlow()
    {
        var state = GetPreparedTransactionUsersState("TransactionUsers");
        var count = state.RunNestedSavepointFlow();
        GC.KeepAlive(count);
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


}
