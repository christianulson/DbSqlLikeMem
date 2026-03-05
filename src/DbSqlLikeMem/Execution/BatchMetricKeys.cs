namespace DbSqlLikeMem;

internal static class BatchMetricKeys
{
    public static class Phases
    {
        public const string Materialization = "materialization";
        public const string Reader = "reader";
        public const string FallbackNonQuery = "fallback-nonquery";
        public const string NonQuery = "nonquery";
        public const string Scalar = "scalar";
    }

    public static class TypePrefixes
    {
        public const string Materialize = "materialize:";
        public const string Reader = "reader:";
        public const string FallbackNonQuery = "fallback-nonquery:";
        public const string NonQuery = "nonquery:";
        public const string Scalar = "scalar:";
    }
}
