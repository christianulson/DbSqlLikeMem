namespace DbSqlLikeMem;

internal static class AggregateFunctionCatalog
{
    private static readonly HashSet<string> Names = new(StringComparer.OrdinalIgnoreCase)
    {
        SqlConst.COUNT,SqlConst.COUNT_BIG,SqlConst.SUM,SqlConst.MIN,SqlConst.MAX,SqlConst.AVG,SqlConst.GROUP_CONCAT,SqlConst.STRING_AGG,SqlConst.LISTAGG,SqlConst.LIST,SqlConst.ANY_VALUE,SqlConst.BIT_AND,SqlConst.BIT_OR,SqlConst.BIT_XOR,SqlConst.JSON_ARRAYAGG,
        SqlConst.JSON_GROUP_OBJECT,SqlConst.TOTAL,"MEDIAN","PERCENTILE","PERCENTILE_CONT","PERCENTILE_DISC",SqlConst.VAR_POP,SqlConst.VAR_SAMP,SqlConst.VARIANCE,SqlConst.VARIANCE_SAMP,SqlConst.VAR,SqlConst.VARP,
        SqlConst.COLLECT,"CORR","CORR_K","CORR_S","CORRELATION","COVAR_POP","COVAR_SAMP","COVARIANCE","COVARIANCE_SAMP",SqlConst.CV,SqlConst.JSON_OBJECTAGG,"GROUP_ID",
        SqlConst.CHECKSUM_AGG,SqlConst.STDEV,SqlConst.STDEVP,
        "APPROX_COUNT_DISTINCT","APPROX_COUNT_DISTINCT_AGG","APPROX_COUNT_DISTINCT_DETAIL","APPROX_MEDIAN","APPROX_PERCENTILE","APPROX_PERCENTILE_AGG","APPROX_PERCENTILE_DETAIL",
        "REGR_AVGX","REGR_AVGY","REGR_COUNT","REGR_INTERCEPT","REGR_ICPT","REGR_R2","REGR_SLOPE","REGR_SXX","REGR_SXY","REGR_SYY",
        "STD","STDDEV","STDDEV_POP","STDDEV_SAMP","STATS_BINOMIAL_TEST","STATS_CROSSTAB","STATS_F_TEST","STATS_KS_TEST","STATS_MODE","STATS_MW_TEST","STATS_ONE_WAY_ANOVA",
        "STATS_T_TEST_INDEP","STATS_T_TEST_INDEPU","STATS_T_TEST_ONE","STATS_T_TEST_PAIRED","STATS_WSR_TEST","XMLAGG","RATIO_TO_REPORT",
        SqlConst.ARRAY_AGG,SqlConst.BOOL_AND,SqlConst.BOOL_OR,SqlConst.EVERY,SqlConst.JSON_AGG,SqlConst.JSONB_AGG,
        SqlConst.JSON_OBJECT_AGG,SqlConst.JSON_OBJECT_AGG_STRICT,SqlConst.JSON_OBJECT_AGG_UNIQUE,SqlConst.JSON_OBJECT_AGG_UNIQUE_STRICT,
        SqlConst.JSONB_OBJECT_AGG,SqlConst.JSONB_OBJECT_AGG_STRICT,SqlConst.JSONB_OBJECT_AGG_UNIQUE,SqlConst.JSONB_OBJECT_AGG_UNIQUE_STRICT
    };

    internal static bool Contains(string name)
        => !string.IsNullOrWhiteSpace(name) && Names.Contains(name);

    internal static string GetRegexAlternation()
        => string.Join("|", Names.OrderByDescending(static name => name.Length).Select(Regex.Escape));
}
