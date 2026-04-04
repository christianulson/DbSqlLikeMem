using System.Globalization;

using DbSqlLikeMem.Models;
using static DbSqlLikeMem.AstQueryExecutorBase;

namespace DbSqlLikeMem.Db2;

internal static class Db2ScalarFunctionRegistry
{
    internal static void Register(ISqlDialect dialect, int version)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(dialect, nameof(dialect));

        SqlSharedScalarFunctionRegistry.Register(dialect);
        Db2OracleDb2ScalarFunctionRegistry.Register(dialect);

        RegisterConversionFunctions(dialect);
        RegisterTemporalFunctions(dialect);
        RegisterLegacyNumericFunctions(dialect);
        RegisterAnalyticsFunctions(dialect);
        RegisterStringFunctions(dialect, version);
        RegisterRowCountFunctions(dialect);
    }

    private static void RegisterConversionFunctions(ISqlDialect dialect)
    {
        static bool TryEvalDb2CastFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            if (fn.Args.Count < 2)
            {
                result = null;
                return false;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            var type = fn.Args[1] is RawSqlExpr rawType
                ? rawType.Sql
                : evalArg(1)?.ToString() ?? string.Empty;
            type = type.Trim();

            try
            {
                if (IsBinaryCastTypeName(type))
                {
                    result = value switch
                    {
                        byte[] bytes => bytes,
                        ReadOnlyMemory<byte> memory => memory.ToArray(),
                        Memory<byte> memory => memory.ToArray(),
                        _ => value
                    };
                    return true;
                }

                if (IsTextCastTypeName(type))
                {
                    result = Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty;
                    return true;
                }

                var dialect = context.Dialect ?? throw new InvalidOperationException("Dialeto SQL não disponível para CAST.");
                if (dialect.IsIntegerCastTypeName(type))
                {
                    if (value is long longValue)
                    {
                        result = (int)longValue;
                        return true;
                    }

                    if (value is int intValue)
                    {
                        result = intValue;
                        return true;
                    }

                    if (value is decimal decimalValue)
                    {
                        result = (int)decimalValue;
                        return true;
                    }

                    var text = value?.ToString()?.Trim() ?? string.Empty;
                    if (int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedInt))
                    {
                        result = parsedInt;
                        return true;
                    }

                    if (long.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsedLong))
                    {
                        result = (int)parsedLong;
                        return true;
                    }

                    if (decimal.TryParse(text, NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
                    {
                        result = (int)parsedDecimal;
                        return true;
                    }

                    result = 0;
                    return true;
                }

                if (type.StartsWith("DECIMAL", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("NUMERIC", StringComparison.OrdinalIgnoreCase))
                {
                    if (value is decimal decimalResult)
                    {
                        result = decimalResult;
                        return true;
                    }

                    if (decimal.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDecimal))
                    {
                        result = parsedDecimal;
                        return true;
                    }

                    result = 0m;
                    return true;
                }

                if (type.StartsWith("FLOAT", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("REAL", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("DOUBLE", StringComparison.OrdinalIgnoreCase))
                {
                    if (value is double doubleResult)
                    {
                        result = doubleResult;
                        return true;
                    }

                    if (value is float floatResult)
                    {
                        result = (double)floatResult;
                        return true;
                    }

                    if (value is decimal decimalResult)
                    {
                        result = (double)decimalResult;
                        return true;
                    }

                    if (double.TryParse(value?.ToString(), NumberStyles.Any, CultureInfo.InvariantCulture, out var parsedDouble))
                    {
                        result = parsedDouble;
                        return true;
                    }

                    result = 0d;
                    return true;
                }

                if (type.StartsWith("DATE", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("DATETIME", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("SMALLDATETIME", StringComparison.OrdinalIgnoreCase)
                    || type.StartsWith("TIMESTAMP", StringComparison.OrdinalIgnoreCase))
                {
                    result = AstQueryExecutionRuntimeHelper.TryCoerceDateTime(value, out var dateTime)
                        ? dateTime
                        : null;
                    return true;
                }

                result = Convert.ToString(value, CultureInfo.InvariantCulture);
                return true;
            }
#pragma warning disable CA1031
            catch (Exception e)
            {
                AstQueryExecutionRuntimeHelper.LogFunctionEvaluationFailure(e);
                result = null;
                return true;
            }
#pragma warning restore CA1031
        }

        static bool IsTextCastTypeName(string typeName)
        {
            if (string.IsNullOrWhiteSpace(typeName))
                return false;

            return typeName.StartsWith("CHAR", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("VARCHAR", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("NCHAR", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("NVARCHAR", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("TEXT", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("CLOB", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("LONGTEXT", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("MEDIUMTEXT", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("TINYTEXT", StringComparison.OrdinalIgnoreCase);
        }

        static bool IsBinaryCastTypeName(string typeName)
        {
            if (string.IsNullOrWhiteSpace(typeName))
                return false;

            return typeName.IndexOf("FOR BIT DATA", StringComparison.OrdinalIgnoreCase) >= 0
                || typeName.StartsWith("BINARY", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("VARBINARY", StringComparison.OrdinalIgnoreCase)
                || typeName.StartsWith("BLOB", StringComparison.OrdinalIgnoreCase);
        }

        static bool TryEvalDb2CharFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var value = fn.Args.Count == 0 ? null : evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (value is DateTime dateTime)
            {
                result = dateTime.ToString(CultureInfo.InvariantCulture);
                return true;
            }

            if (value is DateTimeOffset dateTimeOffset)
            {
                result = dateTimeOffset.ToString(CultureInfo.InvariantCulture);
                return true;
            }

            result = Convert.ToString(value, CultureInfo.InvariantCulture);
            return true;
        }

        dialect.AddScalarFunctions("VARCHAR", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate, "BPCHAR", "DBCLOB", "GRAPHIC", "VARGRAPHIC");
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("CAST", "VARCHAR") with
        {
            AstExecutor = TryEvalDb2CastFunction,
            InvocationStyle = DbInvocationStyle.Call
        });
        dialect.AddScalarFunctions("VARCHAR", TryEvalDb2CharFunction, DbInvocationStyle.Call | DbInvocationStyle.Identifier, "CHAR", "NCHAR", "VARCHAR");
        dialect.AddScalarFunctions("BIGINT", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvalDialectSpecificCastFunction, DbInvocationStyle.Call, "BIGINT");
        dialect.AddScalarFunctions("INT", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvalDialectSpecificCastFunction, DbInvocationStyle.Call, "INT", "INTEGER", "SMALLINT");
        dialect.AddScalarFunctions("DECIMAL", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvalDialectSpecificCastFunction, DbInvocationStyle.Call, "DEC", "DECIMAL");
        dialect.AddScalarFunctions("DOUBLE", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvalDialectSpecificCastFunction, DbInvocationStyle.Call, "DOUBLE", "DOUBLE_PRECISION", "FLOAT", "FLOAT4", "FLOAT8", "REAL");
        dialect.AddScalarFunctions("DOUBLE", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate, "DOUBLE_PRECISION", "FLOAT4", "FLOAT8");
        dialect.AddScalarFunctions("VARCHAR", AstQueryOracleDb2ConversionFunctionEvaluator.TryEvaluate, "TO_CLOB", "TO_NCHAR", "TO_NCLOB");
    }

    private static void RegisterTemporalFunctions(ISqlDialect dialect)
    {
        static bool TryEvalDb2DateAliasFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            return AstQueryDb2DateFunctionEvaluator.TryEvaluateDb2DateFunction(
                context,
                fn,
                evalArg,
                AstQueryExecutionRuntimeHelper.ResolveTemporalUnit,
                out result);
        }

        static bool TryEvalDb2MonthNameFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = dateTime.ToString("MMMM", CultureInfo.InvariantCulture);
            return true;
        }

        static bool TryEvalDb2QuarterFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            result = ((dateTime.Month - 1) / 3) + 1;
            return true;
        }

        static bool TryEvalDb2ExtractFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count < 2)
            {
                result = null;
                return false;
            }

            var unitText = fn.Args[0] switch
            {
                RawSqlExpr raw => raw.Sql,
                IdentifierExpr id => id.Name,
                LiteralExpr lit => lit.Value?.ToString(),
                _ => fn.Args[0].ToString()
            };

            var unit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unitText ?? string.Empty);
            if (unit == TemporalUnit.Unknown)
            {
                result = null;
                return true;
            }

            var value = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            if (AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = unit switch
                {
                    TemporalUnit.Day => dateTime.Day,
                    TemporalUnit.Month => dateTime.Month,
                    TemporalUnit.Year => dateTime.Year,
                    TemporalUnit.Hour => dateTime.Hour,
                    TemporalUnit.Minute => dateTime.Minute,
                    TemporalUnit.Second => dateTime.Second,
                    _ => null
                };
                return true;
            }

            if (AstQueryExecutorBase.TryConvertNumericToDouble(value, out var numeric))
            {
                result = unit == TemporalUnit.Day
                    ? (int)Math.Truncate(numeric)
                    : null;
                return true;
            }

            result = null;
            return true;
        }

        static bool TryEvalDb2LastDayFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value) || !AstQueryExecutorBase.TryCoerceDateTime(value, out var dateTime))
            {
                result = null;
                return true;
            }

            var lastDay = DateTime.DaysInMonth(dateTime.Year, dateTime.Month);
            result = new DateTime(dateTime.Year, dateTime.Month, lastDay, dateTime.Hour, dateTime.Minute, dateTime.Second, dateTime.Kind);
            return true;
        }

        static bool TryEvalDb2DaysFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count != 1)
            {
                result = null;
                return false;
            }

            var baseValue = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(baseValue) || !AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            result = (int)(dateTime.Date - DateTime.MinValue.Date).TotalDays + 1;
            return true;
        }

        static bool TryEvalDb2TimestampAddAndDiffFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            // SqlExpressionParser stores leading temporal units as RawSqlExpr for TIMESTAMPADD/TIMESTAMPDIFF.
            // The legacy evaluator expects the unit through evalArg(0), so we shim it here for DB2.
            object? unitValue;
            if (fn.Args.Count > 0 && fn.Args[0] is RawSqlExpr raw)
            {
                var text = raw.Sql.Trim();
                unitValue = int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var intervalCode)
                    ? intervalCode
                    : text;
            }
            else
            {
                unitValue = evalArg(0);
            }

            object? EvalArgShim(int i) => i == 0 ? unitValue : evalArg(i);
            return AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate(context, fn, EvalArgShim, out result);
        }

        static bool TryEvalDb2DateAddAliasFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (fn.Args.Count < 2)
            {
                result = null;
                return false;
            }

            var baseValue = evalArg(0);
            var amountValue = evalArg(1);
            if (AstQueryExecutorBase.IsNullish(baseValue) || AstQueryExecutorBase.IsNullish(amountValue))
            {
                result = null;
                return true;
            }

            if (!AstQueryExecutorBase.TryCoerceDateTime(baseValue, out var dateTime))
            {
                result = null;
                return true;
            }

            var name = fn.Name.ToUpperInvariant();
            var unit = name["ADD_".Length..];
            var amount = Convert.ToInt32(Convert.ToDecimal(amountValue, CultureInfo.InvariantCulture));
            if (name == "ADD_DAYS" || name == "ADD_HOURS" || name == "ADD_MINUTES" || name == "ADD_SECONDS" || name == "ADD_MONTHS" || name == "ADD_YEARS")
            {
                var temporalUnit = AstQueryExecutionRuntimeHelper.ResolveTemporalUnit(unit);
                result = AstQueryExecutorBase.ApplyDateDelta(dateTime, temporalUnit, amount);
                return true;
            }

            result = null;
            return true;
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE", "DATE") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunctions(
            "INT",
            TryEvalDb2DateAliasFunction,
            DbInvocationStyle.Call,
            "DAY",
            "DAYOFMONTH",
            "DAYOFWEEK",
            "DAYOFWEEK_ISO",
            "DAYOFYEAR",
            "HOUR",
            "MINUTE",
            "MONTH",
            "SECOND",
            "WEEK",
            "WEEK_ISO",
            "YEAR");
        dialect.AddScalarFunctions(
            "VARCHAR",
            TryEvalDb2MonthNameFunction,
            DbInvocationStyle.Call,
            "MONTHNAME");
        dialect.AddScalarFunctions(
            "INT",
            TryEvalDb2QuarterFunction,
            DbInvocationStyle.Call,
            "QUARTER");
        dialect.AddScalarFunctions(
            "VARCHAR",
            TryEvalDb2DateAliasFunction,
            DbInvocationStyle.Call,
            "DAYNAME");
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("EXTRACT", "INT") with
            {
                AstExecutor = TryEvalDb2ExtractFunction
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("LAST_DAY", "DATE") with
            {
                AstExecutor = TryEvalDb2LastDayFunction
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATETIME", "DATETIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIME", "TIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMP", "DATETIME") with { AstExecutor = AstQueryGeneralDateFunctionEvaluator.TryEvaluate });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DAYS", "INT") with
            {
                AstExecutor = TryEvalDb2DaysFunction
            });

        dialect.AddScalarFunction(
            "SESSION_USER",
            "VARCHAR",
            static (QueryExecutionContext ctx, FunctionCallExpr fn, Func<int, object?> evalArg, out object? result) =>
            {
                _ = ctx;
                _ = fn;
                _ = evalArg;
                result = "dbo";
                return true;
            },
            DbInvocationStyle.Call | DbInvocationStyle.Identifier,
            null);

        dialect.AddScalarFunction("CURDATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Call | DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT DATE", "DATE", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Date);
        dialect.AddScalarFunction("CURRENT_TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT TIME", "TIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.Time);
        dialect.AddScalarFunction("CURRENT_TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("CURRENT TIMESTAMP", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);
        dialect.AddScalarFunction("SYSTEMDATE", "DATETIME", SqlDialectScalarFunctionRegistryExtensions.TryEvalZeroArgTemporalFunction, DbInvocationStyle.Identifier, SqlTemporalFunctionKind.DateTime);

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("NEXT_DAY", "DATE") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            "TRUNCATE",
            "DECIMAL",
            QueryOracleDb2ScalarFunctionHelper.TryEvalCoreFunctions);
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_DAYS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_HOURS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_MINUTES", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_SECONDS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_MONTHS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ADD_YEARS", "DATE") with { AstExecutor = TryEvalDb2DateAddAliasFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE_ADD", "DATETIME") with
            {
                AstExecutor = AstQueryGeneralDateArithmeticFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DATE_TRUNC", "DATE") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPADD", "DATETIME") with { AstExecutor = TryEvalDb2TimestampAddAndDiffFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("TIMESTAMPDIFF", "INT") with { AstExecutor = TryEvalDb2TimestampAddAndDiffFunction });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("MIDNIGHT_SECONDS", "INT") with
            {
                AstExecutor = AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate
            });
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("EOMONTH", "DATE") with
            {
                AstExecutor = TryEvalDb2EomonthFunction
            });
        dialect.AddScalarFunctions(
            DbFunctionDef.CreateScalar("NEXT_VALUE_FOR", "BIGINT") with
            {
                AstExecutor = TryEvalDb2SequenceFunction
            },
            "NEXT_VALUE_FOR",
            "PREVIOUS_VALUE_FOR");
    }

    private static bool TryEvalDb2EomonthFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;
        return AstQuerySqlServerCompatibilityFunctionEvaluator.TryEvalEomonthFunction(fn, evalArg, out result);
    }

    private static bool TryEvalDb2SequenceFunction(
        QueryExecutionContext context,
        FunctionCallExpr fn,
        Func<int, object?> evalArg,
        out object? result)
    {
        _ = context;

        return SqlSequenceEvaluator.TryEvaluateCall(
            context.Connection,
            fn.Name,
            fn.Args,
            expr => ResolveSequenceArgValue(fn.Args, expr, evalArg),
            out result);
    }

    private static object? ResolveSequenceArgValue(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        return expr switch
        {
            LiteralExpr lit => lit.Value,
            RawSqlExpr raw => raw.Sql,
            IdentifierExpr id => id.Name,
            ColumnExpr col => string.IsNullOrWhiteSpace(col.Qualifier) ? col.Name : col.Qualifier + "." + col.Name,
            _ => ResolveSequenceArgValueByReference(args, expr, evalArg)
        };
    }

    private static object? ResolveSequenceArgValueByReference(
        IReadOnlyList<SqlExpr> args,
        SqlExpr expr,
        Func<int, object?> evalArg)
    {
        for (var i = 0; i < args.Count; i++)
        {
            if (ReferenceEquals(args[i], expr))
                return evalArg(i);
        }

        return null;
    }

    private static void RegisterLegacyNumericFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("DIV", "DECIMAL", AstQueryOracleDb2LegacyFunctionEvaluator.TryEvaluate));
    }

    private static void RegisterAnalyticsFunctions(ISqlDialect dialect)
    {
        dialect.AddScalarFunction(DbFunctionDef.CreateScalar("RATIO_TO_REPORT", "DOUBLE"));
    }

    private static void RegisterStringFunctions(ISqlDialect dialect, int version)
    {
        static bool TryEvalDb2UpperAliasFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = context;

            if (!string.Equals(fn.Name, "UCASE", StringComparison.OrdinalIgnoreCase))
            {
                result = null;
                return false;
            }

            var value = evalArg(0);
            if (AstQueryExecutorBase.IsNullish(value))
            {
                result = null;
                return true;
            }

            result = value?.ToString()?.ToUpperInvariant();
            return true;
        }

        dialect.AddScalarFunctions(
            DbFunctionDef.CreateCallOrIdentifier(Db2Const.VALUE, "VARCHAR") with
            {
                AstExecutor = QueryConditionalNullFunctionHelper.TryEvalConditionalAndNullFunctions
            },
            Db2Const.VALUE,
            "IFNULL");

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar(SqlConst.LISTAGG, "VARCHAR") with
            {
                IsStringAggregate = true
            });

        dialect.AddScalarFunctions(
            "VARCHAR",
            TryEvalDb2UpperAliasFunction,
            DbInvocationStyle.Call,
            "UCASE");

        if (version >= Db2Dialect.JsonFunctionsMinVersion)
        {
            dialect.AddScalarFunctions(
                DbFunctionDef.CreateScalar("JSON_QUERY", "VARCHAR") with
                {
                    AstExecutor = AstQueryJsonExtractionFunctionEvaluator.TryEvalJsonExtractionFunction
                },
                "JSON_QUERY",
                "JSON_VALUE");
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("GROUPING", "INT") with
            {
                AstExecutor = AstQueryGroupingFunctionEvaluator.TryEvaluate
            });

    }

    private static void RegisterRowCountFunctions(ISqlDialect dialect)
    {
        static bool TryEvalLastFoundRowsFunction(
            QueryExecutionContext context,
            FunctionCallExpr fn,
            Func<int, object?> evalArg,
            out object? result)
        {
            _ = evalArg;
            context.EnsureOracleDb2FunctionSupported(fn);

            if (fn.Args.Count != 0)
            {
                result = null;
                return false;
            }

            result = context.Connection.GetLastFoundRows();
            return true;
        }

        dialect.AddScalarFunction(
            DbFunctionDef.CreateScalar("ROW_COUNT", "BIGINT") with
            {
                AstExecutor = TryEvalLastFoundRowsFunction
            });
    }
}
