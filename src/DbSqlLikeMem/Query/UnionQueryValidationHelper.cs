namespace DbSqlLikeMem;

internal static class UnionQueryValidationHelper
{
    internal static void ValidateUnionColumnTypes(
        IList<TableResultColMock> expected,
        IList<TableResultColMock> current,
        int currentIndex,
        string? sqlContextForErrors,
        ISqlDialect dialect)
    {
        for (var i = 0; i < expected.Count; i++)
        {
            if (dialect.AreUnionColumnTypesCompatible(expected[i].DbType, current[i].DbType))
                continue;

            var message =
                "UNION: tipo de coluna incompatível. "
                + $"Coluna[{i}] Primeiro={expected[i].DbType}, SELECT[{currentIndex}]={current[i].DbType}.";

            if (!string.IsNullOrWhiteSpace(sqlContextForErrors))
                message += "\nSQL: " + sqlContextForErrors;

            throw new InvalidOperationException(message);
        }
    }
}
