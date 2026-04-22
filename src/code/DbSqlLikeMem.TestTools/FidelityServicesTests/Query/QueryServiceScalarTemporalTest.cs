using System.Text;

namespace DbSqlLikeMem.TestTools.Query;

public partial class QueryServiceTest
{
    /// <summary>
    /// EN: Executes a scalar date query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de data e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<object?> RunDateScalarAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.DateScalar());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes the JSON scalar benchmark when the provider supports it.
    /// PT: Executa o benchmark escalar de JSON quando o provedor suporta isso.
    /// </summary>
    public async Task<object?> RunJsonScalarReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON scalar benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonScalarRead("{\"name\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the nested JSON path benchmark when the provider supports it.
    /// PT: Executa o benchmark de caminho JSON aninhado quando o provedor suporta isso.
    /// </summary>
    public async Task<object?> RunJsonPathReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonPathRead("{\"user\":{\"name\":\"Alice\"}}"));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the JSON path benchmark with a missing path and keeps the provider result alive.
    /// PT: Executa o benchmark de caminho JSON com caminho ausente e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<object?> RunJsonMissingPathReadAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON path benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonPathRead("{\"user\":{}}"));
        GC.KeepAlive(value);
        return value is DBNull ? null : value;
    }

    /// <summary>
    /// EN: Executes the JSON insert and cast benchmark when the provider supports JSON reads.
    /// PT: Executa o benchmark de insert e cast de JSON quando o provedor suporta leituras JSON.
    /// </summary>
    public async Task<object?> RunJsonInsertCastAsync()
    {
        if (!Repo.Dialect.SupportsJsonScalarRead)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the JSON insert/cast benchmark.");
        }

        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.JsonScalarRead("{\"value\":42,\"text\":\"Alice\"}"));
        GC.KeepAlive(value);
        return value is DBNull ? null : value;
    }

    /// <summary>
    /// EN: Executes json_each over JSON array and returns all rows for fidelity comparison.
    /// PT: Executa json_each sobre array JSON e retorna todas as linhas para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonEachFromArrayAsync()
    {
        if (!Repo.Dialect.SupportsJsonEachFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_each.");
        }

        var json = "{\"items\":[{\"name\":\"Alice\"},{\"name\":\"Bob\"}]}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonEachFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"])
            });
        }

        return results;
    }

    /// <summary>
    /// EN: Executes json_each over JSON object and returns all rows for fidelity comparison.
    /// PT: Executa json_each sobre objeto JSON e retorna todas as linhas para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonEachFromObjectAsync()
    {
        if (!Repo.Dialect.SupportsJsonEachFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_each.");
        }

        var json = "{\"name\":\"Alice\",\"age\":30}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonEachFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"])
            });
        }

        return results;
    }

    /// <summary>
    /// EN: Executes json_tree over JSON and returns full tree structure for fidelity comparison.
    /// PT: Executa json_tree sobre JSON e retorna estrutura completa para comparação de fidelidade.
    /// </summary>
    public async Task<List<Dictionary<string, object?>>> RunJsonTreeStructureAsync()
    {
        if (!Repo.Dialect.SupportsJsonTreeFunction)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support json_tree.");
        }

        var json = "{\"user\":{\"name\":\"Alice\"}}";
        using var cmd = Repo.Cnn.CreateCommand();
        cmd.CommandText = Repo.Dialect.JsonTreeFunction($"'{json}'");
        using var reader = await cmd.ExecuteReaderAsync();
        var results = new List<Dictionary<string, object?>>();
        while (await reader.ReadAsync())
        {
            results.Add(new Dictionary<string, object?>
            {
                ["key"] = Repo.Dialect.NormalizeJsonTableValue(reader["key"]),
                ["value"] = Repo.Dialect.NormalizeJsonTableValue(reader["value"]),
                ["type"] = Repo.Dialect.NormalizeJsonTableValue(reader["type"]),
                ["path"] = Repo.Dialect.NormalizeJsonTableValue(reader["path"])
            });
        }

        NormalizeJsonTreeIdentifiers(results);
        return results;
    }

    private static void NormalizeJsonTreeIdentifiers(List<Dictionary<string, object?>> rows)
    {
        var fullKeyToId = new Dictionary<string, long>(StringComparer.Ordinal);

        for (var i = 0; i < rows.Count; i++)
        {
            var row = rows[i];
            var fullKey = BuildJsonTreeFullKey(row);
            var parentFullKey = Convert.ToString(row["path"], CultureInfo.InvariantCulture);

            row["id"] = (long)i;
            row["parent"] = parentFullKey is null
                || string.Equals(parentFullKey, fullKey, StringComparison.Ordinal)
                    ? null
                : fullKeyToId.TryGetValue(parentFullKey, out var parentId)
                    ? parentId
                    : null;

            fullKeyToId[fullKey] = (long)i;
        }
    }

    private static string BuildJsonTreeFullKey(Dictionary<string, object?> row)
    {
        var path = Convert.ToString(row["path"], CultureInfo.InvariantCulture) ?? "$";
        var key = row["key"];

        if (key is null)
        {
            return path;
        }

        return key switch
        {
            string text => path == "$" ? $"$.{text}" : $"{path}.{text}",
            long index => $"{path}[{index}]",
            int index => $"{path}[{index}]",
            short index => $"{path}[{index}]",
            byte index => $"{path}[{index}]",
            sbyte index => $"{path}[{index}]",
            _ => path == "$" ? $"$.{Convert.ToString(key, CultureInfo.InvariantCulture)}" : $"{path}.{Convert.ToString(key, CultureInfo.InvariantCulture)}"
        };
    }

    /// <summary>
/// EN: Executes a current timestamp scalar query and keeps the provider result alive.
    /// PT: Executa uma consulta escalar de timestamp atual e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<object?> RunTemporalCurrentTimestampAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalCurrentTimestamp());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes a temporal date-add query and keeps the provider result alive.
    /// PT: Executa uma consulta temporal de soma de data e mantém o resultado do provedor vivo.
    /// </summary>
    public async Task<object?> RunTemporalDateAddAsync()
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalDateAdd());
        var normalized = NormalizeTemporalValue(value);
        GC.KeepAlive(normalized);
        return normalized;
    }

    /// <summary>
    /// EN: Executes the provider string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings do provedor sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregate(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    private static DateTime NormalizeTemporalValue(object? value)
    {
        var normalized = value switch
        {
            DateTime dateTime => dateTime,
            DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
            string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
        };

        return new DateTime(
            normalized.Year,
            normalized.Month,
            normalized.Day,
            normalized.Hour,
            normalized.Minute,
            normalized.Second,
            normalized.Millisecond,
            normalized.Kind);
    }

    /// <summary>
    /// EN: Executes the ordered string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao ordenada de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateOrderedAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateOrdered(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the distinct string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao distinta de strings sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateDistinctAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateDistinct(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the custom-separator string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao com separador customizado sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateCustomSeparatorAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateCustomSeparator(Context, ";")), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes the large-group string aggregation benchmark over sample user names.
    /// PT: Executa o benchmark de agregacao de strings em grupo grande sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateLargeGroupAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var value = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateLargeGroup(Context)), CultureInfo.InvariantCulture);
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Executes a string-aggregation summary query with total, distinct, and repeated-name counts over sample user names.
    /// PT: Executa uma consulta resumo de agregacao de strings com contagens total, distinta e de nomes repetidos sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateSummaryMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var ordered = Convert.ToString(await Repo.ExecuteScalarAsync(Repo.Dialect.StringAggregateOrdered(Context)), CultureInfo.InvariantCulture);
        var totalCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var distinctCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(DISTINCT Name) FROM {Context.TbUsersFullName}"), CultureInfo.InvariantCulture);
        var bobCount = Convert.ToInt32(await Repo.ExecuteScalarAsync($"SELECT COUNT(*) FROM {Context.TbUsersFullName} WHERE Name = 'Bob'"), CultureInfo.InvariantCulture);

        GC.KeepAlive(ordered);
        GC.KeepAlive(totalCount);
        GC.KeepAlive(distinctCount);
        GC.KeepAlive(bobCount);
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Ordered", "TotalCount", "DistinctCount", "BobCount"]),
            Rows =
            [
                new QueryResultRowSnapshot
                {
                    Values = [ordered, totalCount, distinctCount, bobCount],
                },
            ],
        };
    }

    /// <summary>
    /// EN: Executes a grouped string report with CASE and COALESCE over sample user names.
    /// PT: Executa um relatorio agrupado de strings com CASE e COALESCE sobre nomes de usuarios de exemplo.
    /// </summary>
    public async Task<object?> RunStringAggregateGroupCaseMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsStringAggregate)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support the string aggregate benchmark.");
        }

        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END AS NameGroup,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName
FROM {Context.TbUsersFullName}
GROUP BY CASE WHEN Name = 'Bob' THEN 'B' ELSE 'Other' END
ORDER BY NameGroup
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateStringAggregateGroupCaseRow(reader, "B", 2, 1, "Bob", "Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateStringAggregateGroupCaseRow(reader, "Other", 3, 3, "Alice", "Delta");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameGroup", "TotalCount", "DistinctCount", "FirstName", "LastName"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped name-initial report with distinct counts and HAVING filtering over the configured users table.
    /// PT: Executa um relatorio agrupado por inicial do nome com contagens distintas e filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunGroupByNameInitialMatrixAsync(params object[] pars)
    {
        var initialExpr = $"UPPER({Repo.Dialect.StringPrefixExpression("Name", 1)})";
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount,
    COUNT(DISTINCT Name) AS DistinctCount,
    SUM(CASE WHEN Name = 'Alice' THEN 1 ELSE 0 END) AS AliceCount,
    SUM(CASE WHEN Name = 'Bob' THEN 1 ELSE 0 END) AS BobCount,
    COALESCE(MIN(Name), 'none') AS FirstName,
    COALESCE(MAX(Name), 'none') AS LastName,
    CASE WHEN COUNT(*) >= 2 THEN 1 ELSE 0 END AS HasAtLeastTwo
FROM {Context.TbUsersFullName}
GROUP BY {initialExpr}
HAVING COUNT(*) >= 2
ORDER BY {initialExpr}
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "A", 3, 2, 2, 0, "Adam", "Alice", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "B", 3, 2, 0, 2, "Bob", "Brian", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateGroupByNameInitialRow(reader, "C", 2, 2, 0, 0, "Carla", "Chris", 1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameInitial", "TotalCount", "DistinctCount", "AliceCount", "BobCount", "FirstName", "LastName", "HasAtLeastTwo"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a grouped name report with HAVING filtering over the configured users table.
    /// PT: Executa um relatorio agrupado por nome com filtro HAVING na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunGroupByNameHavingMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    COUNT(*) AS TotalCount
FROM {Context.TbUsersFullName}
GROUP BY Name
HAVING COUNT(*) >= 2
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "TotalCount"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a GROUP BY ordinal query over the configured users table and validates grouped counts.
    /// PT: Executa uma consulta GROUP BY ordinal na tabela de usuarios configurada e valida as contagens agrupadas.
    /// </summary>
    public async Task<object?> RunGroupByOrdinalMatrixAsync(params object[] pars)
    {
        if (!Repo.Dialect.SupportsGroupByOrdinal)
        {
            throw new NotSupportedException($"{Repo.Dialect.DisplayName} does not support GROUP BY ordinal benchmarks.");
        }

        var initialExpr = $"UPPER({Repo.Dialect.StringPrefixExpression("Name", 1)})";
        var rows = new List<QueryResultRowSnapshot>(3);

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    {initialExpr} AS NameInitial,
    COUNT(*) AS TotalCount
FROM {Context.TbUsersFullName}
GROUP BY 1
HAVING COUNT(*) >= 2
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("A");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("B");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("C");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["NameInitial", "TotalCount"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an ORDER BY ordinal query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY ordinal na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<object?> RunOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT
    Name,
    Id
FROM {Context.TbUsersFullName}
ORDER BY 2 DESC
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(3);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bravo");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(2);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alpha");
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(1);
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name", "Id"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a DISTINCT query ordered by ordinal and validates the projected names.
    /// PT: Executa uma consulta DISTINCT ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public async Task<object?> RunDistinctOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(4);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    Name
FROM {Context.TbUsersFullName}
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Delta");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a DISTINCT query with a text filter ordered by ordinal and validates the projected names.
    /// PT: Executa uma consulta DISTINCT com filtro de texto ordenada por ordinal e valida os nomes projetados.
    /// </summary>
    public async Task<object?> RunDistinctLikeOrderByOrdinalMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT DISTINCT
    UPPER(Name)
FROM {Context.TbUsersFullName}
WHERE UPPER(Name) LIKE '%A%'
ORDER BY 1
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("ALICE");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("CHARLIE");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("DELTA");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["UPPER(Name)"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an IN-list predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado IN com lista na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunInListPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name IN ('Alice', 'Charlie')
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a BETWEEN predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado BETWEEN na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunBetweenPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id BETWEEN 2 AND 4
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a LIKE predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado LIKE na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunLikePredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name LIKE 'A%'
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a combined BETWEEN, LIKE, and ORDER BY query over the configured users table and returns the matching rowset.
    /// PT: Executa uma consulta combinada com BETWEEN, LIKE e ORDER BY na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunBetweenLikeOrderByMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Id BETWEEN 1 AND 4
  AND Name LIKE 'A%'
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Aaron");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a NOT LIKE predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado NOT LIKE na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunNotLikePredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name NOT LIKE 'A%'
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a not-equal predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado diferente de na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunNotEqualPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name <> 'Bob'
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes an equality predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado de igualdade na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunEqualPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Name = 'Bob'
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a parameterized name lookup over the configured users table and returns the matched name.
    /// PT: Executa uma consulta parametrizada por nome na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public async Task<object?> RunParameterSelectByNameMatrixAsync(params object[] pars)
    {
        var name = (string)pars[0];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Name = {Repo.Dialect.Parameter("name")}
""";

        Repo.Dialect.AddParameter(command, "name", DbType.String, name);

        var result = Convert.ToString(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(name);
        GC.KeepAlive(result);
        GC.KeepAlive(name);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameterized id lookup over the configured users table and returns the matched name.
    /// PT: Executa uma consulta parametrizada por id na tabela de usuarios configurada e retorna o nome correspondente.
    /// </summary>
    public async Task<object?> RunParameterSelectByIdMatrixAsync(params object[] pars)
    {
        var id = Convert.ToInt32(pars[0], CultureInfo.InvariantCulture);
        var expectedName = (string)pars[1];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""";

        Repo.Dialect.AddParameter(command, "id", DbType.Int32, id);

        var result = Convert.ToString(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
        result.Should().Be(expectedName);
        GC.KeepAlive(result);
        GC.KeepAlive(id);
        GC.KeepAlive(expectedName);
        return result;
    }

    /// <summary>
    /// EN: Executes a parameter roundtrip over typed user columns and validates string, numeric, boolean, date, and null parameters.
    /// PT: Executa um roundtrip de parametros sobre colunas tipadas de usuarios e valida parametros de texto, numericos, booleanos, data e nulos.
    /// </summary>
    public async Task<object?> RunParameterRoundTripMatrixAsync(params object[] pars)
    {
        var id = (int)pars[0];
        var name = (string)pars[1];
        var email = pars[2] is DBNull ? null : (string?)pars[2];
        var isActive = (bool)pars[3];
        var age = (short)pars[4];
        var balance = (decimal)pars[5];
        var createdAt = (DateTime)pars[6];
        var updatedAt = pars[7] is DBNull ? (DateTime?)null : (DateTime)pars[7];
        var profileJson = pars[8] is DBNull ? null : (string?)pars[8];

        using var insertCommand = Repo.Cnn.CreateCommand();
        insertCommand.CommandText = $"""
INSERT INTO {Context.TbUsersFullName} (
    Id,
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
) VALUES (
    {Repo.Dialect.Parameter("id")},
    {Repo.Dialect.Parameter("name")},
    {Repo.Dialect.Parameter("email")},
    {Repo.Dialect.Parameter("isActive")},
    {Repo.Dialect.Parameter("age")},
    {Repo.Dialect.Parameter("balance")},
    {Repo.Dialect.Parameter("createdAt")},
    {Repo.Dialect.Parameter("updatedAt")},
    {Repo.Dialect.JsonParameter("profileJson")}
)
""";

        AddParameter(insertCommand, "id", DbType.Int32, id);
        AddParameter(insertCommand, "name", DbType.String, name);
        AddParameter(insertCommand, "email", DbType.String, email is null ? DBNull.Value : email);
        AddParameter(insertCommand, "isActive", DbType.Boolean, isActive);
        AddParameter(insertCommand, "age", DbType.Int16, age);
        AddParameter(insertCommand, "balance", DbType.Decimal, balance);
        var createdAtParameter = NormalizeNpgsqlDateTimeInput(createdAt);
        object? updatedAtParameter = updatedAt is null ? DBNull.Value : NormalizeNpgsqlDateTimeInput(updatedAt.Value);

        AddParameter(insertCommand, "createdAt", DbType.DateTime, createdAtParameter);
        AddParameter(insertCommand, "updatedAt", DbType.DateTime, updatedAtParameter);
        AddParameter(insertCommand, "profileJson", DbType.String, profileJson is null ? DBNull.Value : profileJson);

        (await insertCommand.ExecuteNonQueryAsync()).Should().Be(1);

        using var selectCommand = Repo.Cnn.CreateCommand();
        selectCommand.CommandText = $"""
SELECT
    Name,
    Email,
    IsActive,
    Age,
    Balance,
    CreatedAt,
    UpdatedAt,
    ProfileJson
FROM {Context.TbUsersFullName}
WHERE Id = {Repo.Dialect.Parameter("id")}
""";

        AddParameter(selectCommand, "id", DbType.Int32, id);

        using var reader = await selectCommand.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(name);
        (await reader.IsDBNullAsync(1) ? null : Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture)).Should().Be(email);
        Convert.ToBoolean(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(isActive);
        Convert.ToInt16(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(age);
        Convert.ToDecimal(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(balance);
        NormalizeDateTimeValue(reader.GetValue(5)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(createdAt.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));

        var updatedAtText = await reader.IsDBNullAsync(6)
            ? null
            : NormalizeDateTimeValue(reader.GetValue(6)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        (updatedAt is null ? null : updatedAt.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)).Should().Be(updatedAtText);
        DbSqlLikeMem.TestTools.Json.JsonTextAssertions.ShouldMatchJsonText(
            await reader.IsDBNullAsync(7) ? null : Convert.ToString(reader.GetValue(7), CultureInfo.InvariantCulture),
            profileJson);

        (await reader.ReadAsync()).Should().BeFalse();

        GC.KeepAlive(id);
        GC.KeepAlive(name);
        GC.KeepAlive(email);
        GC.KeepAlive(isActive);
        GC.KeepAlive(age);
        GC.KeepAlive(balance);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(updatedAt);
        GC.KeepAlive(profileJson);
        return 1;
    }

    /// <summary>
    /// EN: Executes a typed parameter projection and validates ANSI text, fixed-length text, numeric, temporal, GUID, and binary values returned by provider-specific parameter objects.
    /// PT: Executa uma projeção de parametros tipados e valida valores de texto ANSI, texto de comprimento fixo, numericos, temporais, GUID e binario retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public async Task<object?> RunParameterTypeMatrixAsync(params object[] pars)
    {
        var text = (string)pars[0];
        var ansiText = (string)pars[1];
        var ansiFixedText = (string)pars[2];
        var fixedText = (string)pars[3];
        var int16Value = (short)pars[4];
        var int32Value = (int)pars[5];
        var int64Value = (long)pars[6];
        var boolValue = (bool)pars[7];
        var decimalValue = (decimal)pars[8];
        var doubleValue = (double)pars[9];
        var timeSpanValue = (TimeSpan)pars[10];
        var dateTimeOffsetValue = (DateTimeOffset)pars[11];
        var dateTimeValue = (DateTime)pars[12];
        var guidValue = (Guid)pars[13];
        var binaryValue = (byte[])pars[14];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("text")} AS VARCHAR(100)) AS TextValue,
    CAST({Repo.Dialect.Parameter("ansiText")} AS VARCHAR(100)) AS AnsiTextValue,
    CAST({Repo.Dialect.Parameter("ansiFixedText")} AS CHAR(20)) AS AnsiFixedTextValue,
    CAST({Repo.Dialect.Parameter("fixedText")} AS CHAR(20)) AS FixedTextValue,
    CAST({Repo.Dialect.Parameter("int16Value")} AS SMALLINT) AS Int16Value,
    CAST({Repo.Dialect.Parameter("int32Value")} AS INTEGER) AS Int32Value,
    CAST({Repo.Dialect.Parameter("int64Value")} AS BIGINT) AS Int64Value,
    CAST({Repo.Dialect.Parameter("boolValue")} AS BOOLEAN) AS BoolValue,
    CAST({Repo.Dialect.Parameter("decimalValue")} AS DECIMAL(19,4)) AS DecimalValue,
    CAST({Repo.Dialect.Parameter("doubleValue")} AS DOUBLE) AS DoubleValue,
    CAST({Repo.Dialect.Parameter("timeSpanValue")} AS VARCHAR(32)) AS TimeSpanValue,
    CAST({Repo.Dialect.Parameter("dateTimeOffsetValue")} AS VARCHAR(40)) AS DateTimeOffsetValue,
    CAST({Repo.Dialect.Parameter("dateTimeValue")} AS TIMESTAMP) AS DateTimeValue,
    CAST({Repo.Dialect.Parameter("guidValue")} AS VARCHAR(36)) AS GuidValue,
    CAST({Repo.Dialect.Parameter("binaryValue")} AS VARCHAR(4) FOR BIT DATA) AS BinaryValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("text")} AS TextValue,
    {Repo.Dialect.Parameter("ansiText")} AS AnsiTextValue,
    {Repo.Dialect.Parameter("ansiFixedText")} AS AnsiFixedTextValue,
    {Repo.Dialect.Parameter("fixedText")} AS FixedTextValue,
    {Repo.Dialect.Parameter("int16Value")} AS Int16Value,
    {Repo.Dialect.Parameter("int32Value")} AS Int32Value,
    {Repo.Dialect.Parameter("int64Value")} AS Int64Value,
    {Repo.Dialect.Parameter("boolValue")} AS BoolValue,
    {Repo.Dialect.Parameter("decimalValue")} AS DecimalValue,
    {Repo.Dialect.Parameter("doubleValue")} AS DoubleValue,
    {Repo.Dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {Repo.Dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {Repo.Dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {Repo.Dialect.Parameter("guidValue")} AS GuidValue,
    {Repo.Dialect.Parameter("binaryValue")} AS BinaryValue
""");

        AddParameter(command, "text", DbType.String, text);
        AddParameter(command, "ansiText", DbType.AnsiString, ansiText);
        AddParameter(command, "ansiFixedText", DbType.AnsiStringFixedLength, ansiFixedText);
        AddParameter(command, "fixedText", DbType.StringFixedLength, fixedText);
        AddParameter(command, "int16Value", DbType.Int16, int16Value);
        AddParameter(command, "int32Value", DbType.Int32, int32Value);
        AddParameter(command, "int64Value", DbType.Int64, int64Value);
        AddParameter(command, "boolValue", DbType.Boolean, boolValue);
        AddParameter(command, "decimalValue", DbType.Decimal, decimalValue);
        AddParameter(command, "doubleValue", DbType.Double, doubleValue);
        AddParameter(command, "timeSpanValue", DbType.Time, timeSpanValue);
        AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, dateTimeOffsetValue);
        AddParameter(command, "dateTimeValue", DbType.DateTime, NormalizeNpgsqlDateTimeInput(dateTimeValue));
        AddParameter(command, "guidValue", DbType.Guid, guidValue);
        AddParameter(command, "binaryValue", DbType.Binary, binaryValue);

        using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(text);
        Convert.ToString(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(ansiText);
        Convert.ToString(reader.GetValue(2), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(ansiFixedText);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(fixedText);
        Convert.ToInt16(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(int16Value);
        Convert.ToInt32(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(int32Value);
        Convert.ToInt64(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(int64Value);
        Convert.ToBoolean(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(boolValue);
        Convert.ToDecimal(reader.GetValue(8), CultureInfo.InvariantCulture).Should().Be(decimalValue);
        Convert.ToDouble(reader.GetValue(9), CultureInfo.InvariantCulture).Should().Be(doubleValue);
        NormalizeTimeSpanValue(reader.GetValue(10)).Should().Be(timeSpanValue);
        NormalizeDateTimeOffsetValue(reader.GetValue(11), Repo.Dialect.Provider).Should().Be(Repo.Dialect.Provider == ProviderId.Oracle ? new DateTimeOffset(dateTimeOffsetValue.DateTime, TimeSpan.Zero) : dateTimeOffsetValue);
        NormalizeDateTimeValue(reader.GetValue(12)).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)
            .Should().Be(dateTimeValue.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture));
        NormalizeGuidValue(reader.GetValue(13)).Should().Be(guidValue);
        NormalizeBinaryValue(reader.GetValue(14), Repo.Dialect.Provider).Should().Equal(binaryValue);

        (await reader.ReadAsync()).Should().BeFalse();

        GC.KeepAlive(text);
        GC.KeepAlive(ansiText);
        GC.KeepAlive(ansiFixedText);
        GC.KeepAlive(fixedText);
        GC.KeepAlive(int16Value);
        GC.KeepAlive(int32Value);
        GC.KeepAlive(int64Value);
        GC.KeepAlive(boolValue);
        GC.KeepAlive(decimalValue);
        GC.KeepAlive(doubleValue);
        GC.KeepAlive(timeSpanValue);
        GC.KeepAlive(dateTimeOffsetValue);
        GC.KeepAlive(dateTimeValue);
        GC.KeepAlive(guidValue);
        GC.KeepAlive(binaryValue);
        return 1;
    }

    /// <summary>
    /// EN: Executes a compact typed parameter projection for date and currency values returned by provider-specific parameter objects.
    /// PT: Executa uma projeção compacta de parametros tipados para valores de data e moeda retornados pelos objetos de parametro especificos do provedor.
    /// </summary>
    public async Task<object?> RunParameterDateCurrencyMatrixAsync(params object[] pars)
    {
        var dateValue = (DateTime)pars[0];
        var currencyValue = (decimal)pars[1];

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("dateValue")} AS DATE) AS DateValue,
    CAST({Repo.Dialect.Parameter("currencyValue")} AS DECIMAL(19,2)) AS CurrencyValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("dateValue")} AS DateValue,
    {Repo.Dialect.Parameter("currencyValue")} AS CurrencyValue
""");

        AddParameter(command, "dateValue", DbType.Date, dateValue);
        AddParameter(command, "currencyValue", DbType.Currency, currencyValue);

        using var reader = await command.ExecuteReaderAsync();
        (await reader.ReadAsync()).Should().BeTrue();

        NormalizeDateTimeValue(reader.GetValue(0)).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)
            .Should().Be(dateValue.Date.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture));
        Convert.ToDecimal(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(currencyValue);

        (await reader.ReadAsync()).Should().BeFalse();
        GC.KeepAlive(dateValue);
        GC.KeepAlive(currencyValue);
        return 1;
    }

    /// <summary>
    /// EN: Executes the broad parameter projection benchmark and returns the first projected value.
    /// PT: Executa o benchmark amplo de projeção de parametros e retorna o primeiro valor projetado.
    /// </summary>
    public string? RunParameterProjection()
    {
        var createdAt = Repo.Dialect.Provider == ProviderId.Npgsql
            ? new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc)
            : new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Unspecified);
        var dateValue = createdAt.Date;
        var currencyValue = 123.45m;

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.Provider == ProviderId.Db2
            ? $"""
SELECT
    CAST({Repo.Dialect.Parameter("textValue")} AS VARCHAR(100)) AS TextValue,
    CAST({Repo.Dialect.Parameter("ansiTextValue")} AS VARCHAR(100)) AS AnsiTextValue,
    CAST({Repo.Dialect.Parameter("ansiFixedTextValue")} AS CHAR(20)) AS AnsiFixedTextValue,
    CAST({Repo.Dialect.Parameter("fixedTextValue")} AS CHAR(20)) AS FixedTextValue,
    CAST({Repo.Dialect.Parameter("int16Value")} AS SMALLINT) AS Int16Value,
    CAST({Repo.Dialect.Parameter("int32Value")} AS INTEGER) AS Int32Value,
    CAST({Repo.Dialect.Parameter("int64Value")} AS BIGINT) AS Int64Value,
    CAST({Repo.Dialect.Parameter("boolValue")} AS BOOLEAN) AS BoolValue,
    CAST({Repo.Dialect.Parameter("decimalValue")} AS DECIMAL(19,4)) AS DecimalValue,
    CAST({Repo.Dialect.Parameter("doubleValue")} AS DOUBLE) AS DoubleValue,
    CAST({Repo.Dialect.Parameter("timeSpanValue")} AS VARCHAR(32)) AS TimeSpanValue,
    CAST({Repo.Dialect.Parameter("dateTimeOffsetValue")} AS VARCHAR(40)) AS DateTimeOffsetValue,
    CAST({Repo.Dialect.Parameter("dateTimeValue")} AS TIMESTAMP) AS DateTimeValue,
    CAST({Repo.Dialect.Parameter("guidValue")} AS VARCHAR(36)) AS GuidValue,
    CAST({Repo.Dialect.Parameter("binaryValue")} AS VARCHAR(4) FOR BIT DATA) AS BinaryValue,
    CAST({Repo.Dialect.Parameter("dateValue")} AS DATE) AS DateValue,
    CAST({Repo.Dialect.Parameter("currencyValue")} AS DECIMAL(19,2)) AS CurrencyValue
FROM SYSIBM.SYSDUMMY1
"""
            : Repo.Dialect.SelectParameterProjection($"""
    {Repo.Dialect.Parameter("textValue")} AS TextValue,
    {Repo.Dialect.Parameter("ansiTextValue")} AS AnsiTextValue,
    {Repo.Dialect.Parameter("ansiFixedTextValue")} AS AnsiFixedTextValue,
    {Repo.Dialect.Parameter("fixedTextValue")} AS FixedTextValue,
    {Repo.Dialect.Parameter("int16Value")} AS Int16Value,
    {Repo.Dialect.Parameter("int32Value")} AS Int32Value,
    {Repo.Dialect.Parameter("int64Value")} AS Int64Value,
    {Repo.Dialect.Parameter("boolValue")} AS BoolValue,
    {Repo.Dialect.Parameter("decimalValue")} AS DecimalValue,
    {Repo.Dialect.Parameter("doubleValue")} AS DoubleValue,
    {Repo.Dialect.Parameter("timeSpanValue")} AS TimeSpanValue,
    {Repo.Dialect.Parameter("dateTimeOffsetValue")} AS DateTimeOffsetValue,
    {Repo.Dialect.Parameter("dateTimeValue")} AS DateTimeValue,
    {Repo.Dialect.Parameter("guidValue")} AS GuidValue,
    {Repo.Dialect.Parameter("binaryValue")} AS BinaryValue
""");

        AddParameter(command, "textValue", DbType.String, "benchmark");
        AddParameter(command, "ansiTextValue", DbType.AnsiString, "ansi");
        AddParameter(command, "ansiFixedTextValue", DbType.AnsiStringFixedLength, "fixed-ansi");
        AddParameter(command, "fixedTextValue", DbType.StringFixedLength, "fixed-text");
        AddParameter(command, "int16Value", DbType.Int16, (short)16);
        AddParameter(command, "int32Value", DbType.Int32, 32);
        AddParameter(command, "int64Value", DbType.Int64, 64L);
        AddParameter(command, "boolValue", DbType.Boolean, true);
        AddParameter(command, "decimalValue", DbType.Decimal, 12.34m);
        AddParameter(command, "doubleValue", DbType.Double, 56.78d);
        AddParameter(command, "timeSpanValue", DbType.Time, TimeSpan.FromHours(1.5));
        AddParameter(command, "dateTimeOffsetValue", DbType.DateTimeOffset, new DateTimeOffset(2024, 1, 2, 3, 4, 5, TimeSpan.Zero));
        AddParameter(command, "dateTimeValue", DbType.DateTime, NormalizeNpgsqlDateTimeInput(createdAt));
        AddParameter(command, "guidValue", DbType.Guid, Guid.Parse("11111111-2222-3333-4444-555555555555"));
        AddParameter(command, "binaryValue", DbType.Binary, new byte[] { 1, 2, 3, 4 });
        if (Repo.Dialect.Provider == ProviderId.Db2)
        {
            AddParameter(command, "dateValue", DbType.Date, dateValue);
            AddParameter(command, "currencyValue", DbType.Currency, currencyValue);
        }

        var value = Convert.ToString(command.ExecuteScalar(), CultureInfo.InvariantCulture);
        value.Should().Be("benchmark");
        GC.KeepAlive(value);
        GC.KeepAlive(createdAt);
        GC.KeepAlive(dateValue);
        GC.KeepAlive(currencyValue);
        return value;
    }

    private static DateTime NormalizeDateTimeValue(object? value)
    {
        return value switch
        {
            DateTime dateTime => dateTime,
            DateTimeOffset dateTimeOffset => dateTimeOffset.DateTime,
            string text => DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTime parameter returned a null value."),
            _ when TryNormalizeDateOnlyValue(value, out var dateOnly) => dateOnly,
            _ => Convert.ToDateTime(value, CultureInfo.InvariantCulture)
        };
    }

    private static bool TryNormalizeDateOnlyValue(object? value, out DateTime dateTime)
    {
        dateTime = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.DateOnly", StringComparison.Ordinal))
            return false;

        if (type.GetProperty("Year")?.GetValue(value) is not int year
            || type.GetProperty("Month")?.GetValue(value) is not int month
            || type.GetProperty("Day")?.GetValue(value) is not int day)
        {
            return false;
        }

        dateTime = new DateTime(year, month, day);
        return true;
    }

    private static bool TryNormalizeTimeOnlyValue(object? value, out TimeSpan timeSpan)
    {
        timeSpan = default;

        if (value is null)
            return false;

        var type = value.GetType();
        if (!string.Equals(type.FullName, "System.TimeOnly", StringComparison.Ordinal))
            return false;

        if (type.GetMethod("ToTimeSpan", Type.EmptyTypes)?.Invoke(value, null) is not TimeSpan normalized)
            return false;

        timeSpan = normalized;
        return true;
    }

    private DateTime NormalizeNpgsqlDateTimeInput(DateTime value)
    {
        if (Repo.Dialect.Provider == ProviderId.Npgsql && value.Kind == DateTimeKind.Unspecified)
            return DateTime.SpecifyKind(value, DateTimeKind.Utc);

        return value;
    }

    private static Guid NormalizeGuidValue(object? value)
    {
        return value switch
        {
            Guid guid => guid,
            byte[] bytes when bytes.Length == 16 => new Guid(bytes),
            string text => Guid.Parse(text),
            null => throw new InvalidOperationException("GUID parameter returned a null value."),
            _ => Guid.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("GUID parameter returned an unconvertible value."))
        };
    }

    private static DateTimeOffset NormalizeDateTimeOffsetValue(object? value, ProviderId provider)
    {
        return value switch
        {
            DateTimeOffset dateTimeOffset => provider == ProviderId.Oracle ? new DateTimeOffset(dateTimeOffset.DateTime, TimeSpan.Zero) : dateTimeOffset,
            DateTime dateTime => provider == ProviderId.Oracle ? new DateTimeOffset(dateTime, TimeSpan.Zero) : new DateTimeOffset(dateTime, TimeSpan.Zero),
            string text => DateTimeOffset.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind),
            null => throw new InvalidOperationException("DateTimeOffset parameter returned a null value."),
            _ => DateTimeOffset.Parse(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("DateTimeOffset parameter returned an unconvertible value."), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind)
        };
    }

    private static TimeSpan NormalizeTimeSpanValue(object? value)
    {
        return value switch
        {
            TimeSpan timeSpan => timeSpan,
            _ when TryNormalizeTimeOnlyValue(value, out var timeOnly) => timeOnly,
            DateTime dateTime => dateTime.TimeOfDay,
            string text => ParseTimeSpanText(text),
            null => throw new InvalidOperationException("TimeSpan parameter returned a null value."),
            _ => ParseTimeSpanText(Convert.ToString(value, CultureInfo.InvariantCulture) ?? throw new InvalidOperationException("TimeSpan parameter returned an unconvertible value."))
        };
    }

    private static TimeSpan ParseTimeSpanText(string text)
    {
        if (TimeSpan.TryParse(text, CultureInfo.InvariantCulture, out var timeSpan))
        {
            return timeSpan;
        }

        var trimmed = text.Trim();
        var separatorIndex = trimmed.IndexOf(' ');
        if (separatorIndex > 0
            && int.TryParse(trimmed[..separatorIndex], NumberStyles.Integer, CultureInfo.InvariantCulture, out var days)
            && TimeSpan.TryParse(trimmed[(separatorIndex + 1)..], CultureInfo.InvariantCulture, out var remainder))
        {
            return TimeSpan.FromDays(days) + remainder;
        }

        return TimeSpan.Parse(text, CultureInfo.InvariantCulture);
    }

    private static byte[] NormalizeBinaryValue(object? value, ProviderId provider)
    {
        return value switch
        {
            byte[] bytes => bytes,
            ReadOnlyMemory<byte> memory => memory.ToArray(),
            Memory<byte> memory => memory.ToArray(),
            string text => ParseBinaryText(text),
            DBNull when provider == ProviderId.Oracle => Array.Empty<byte>(),
            null => throw new InvalidOperationException("Binary parameter returned a null value."),
            _ => throw new InvalidOperationException($"Unsupported binary parameter type: {value.GetType().FullName}.")
        };
    }

    private static byte[] ParseBinaryText(string text)
    {
        var trimmed = text.Trim();
        if (trimmed.Length == 0)
            return Array.Empty<byte>();

        var hexCandidate = trimmed;
        if (trimmed.StartsWith("X'", StringComparison.OrdinalIgnoreCase) && trimmed.EndsWith("'", StringComparison.OrdinalIgnoreCase))
        {
            hexCandidate = trimmed[2..^1];
        }
        else if (trimmed.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
        {
            hexCandidate = trimmed[2..];
        }

        hexCandidate = hexCandidate.Replace(" ", string.Empty);

        if (hexCandidate.Length == 0)
            return Array.Empty<byte>();

        if (IsHexString(hexCandidate))
        {
            if (hexCandidate.Length % 2 != 0)
                throw new InvalidOperationException($"Binary parameter returned an odd-length hex string: {text}");

            return ParseHexBytes(hexCandidate);
        }

        return Encoding.GetEncoding("ISO-8859-1").GetBytes(trimmed);
    }

    private static bool IsHexString(string value)
    {
        foreach (var ch in value)
        {
            if ((ch >= '0' && ch <= '9')
                || (ch >= 'A' && ch <= 'F')
                || (ch >= 'a' && ch <= 'f'))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private static byte[] ParseHexBytes(string hexCandidate)
    {
        var bytes = new byte[hexCandidate.Length / 2];
        for (var i = 0; i < hexCandidate.Length; i += 2)
        {
            if (!byte.TryParse(hexCandidate.Substring(i, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var part))
                throw new InvalidOperationException($"Binary parameter returned an invalid hex string: {hexCandidate}");

            bytes[i / 2] = part;
        }

        return bytes;
    }

    /// <summary>
    /// EN: Executes a greater-than predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado maior que na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunGreaterThanPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id > 3
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a less-than predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado menor que na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunLessThanPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id < 3
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a greater-than-or-equal predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado maior ou igual na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunGreaterThanOrEqualPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id >= 3
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes a less-than-or-equal predicate over the configured users table and returns the matching rowset.
    /// PT: Executa um predicado menor ou igual na tabela de usuarios configurada e retorna o conjunto de linhas correspondente.
    /// </summary>
    public async Task<object?> RunLessThanOrEqualPredicateMatrixAsync(params object[] pars)
    {
        return await CaptureSnapshotAsync($"""
SELECT Id, Name
FROM {Context.TbUsersFullName}
WHERE Id <= 3
ORDER BY Id
""");
    }

    /// <summary>
    /// EN: Executes an ORDER BY Name query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY Name na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<object?> RunOrderByNameMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
ORDER BY Name
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes an ORDER BY Name descending query over the configured users table and validates the output order.
    /// PT: Executa uma consulta ORDER BY Name descendente na tabela de usuarios configurada e valida a ordem da saida.
    /// </summary>
    public async Task<object?> RunOrderByNameDescendingMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM {Context.TbUsersFullName}
ORDER BY Name DESC
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Bob");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be("Alice");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a paged name query using ROW_NUMBER and validates the selected page rows.
    /// PT: Executa uma consulta paginada por nome usando ROW_NUMBER e valida as linhas da pagina selecionada.
    /// </summary>
    public async Task<object?> RunNamePaginationMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(3);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = $"""
SELECT Name
FROM (
    SELECT Name, ROW_NUMBER() OVER (ORDER BY Name) AS rn
    FROM {Context.TbUsersFullName}
) q
WHERE rn BETWEEN 2 AND 4
ORDER BY rn
""";

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Delta");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Executes a native paged name query and validates the selected page rows for the configured users table.
    /// PT: Executa uma consulta nativa paginada por nome e valida as linhas da pagina selecionada na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunPagedNameProjectionMatrixAsync(params object[] pars)
    {
        var rows = new List<QueryResultRowSnapshot>(2);
        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = Repo.Dialect.PagedNameProjection(Context.TbUsersFullName, 1, 2);

        using var reader = await command.ExecuteReaderAsync();

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Bravo");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeTrue();
        ValidateNamePaginationRow(reader, "Charlie");
        rows.Add(QueryResultSnapshotReader.CaptureRow(reader));

        (await reader.ReadAsync()).Should().BeFalse();
        return new QueryResultSnapshot
        {
            ColumnNames = NormalizeSnapshotColumnNames(["Name"]),
            Rows = rows,
        };
    }

    /// <summary>
    /// EN: Reads a current-time predicate query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com predicado de tempo atual na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunTemporalNowWhereAsync(params object[] pars)
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalNowWhere(Context));
        GC.KeepAlive(value);
        return value;
    }

    /// <summary>
    /// EN: Reads a current-time ordering query result from the configured users table.
    /// PT: Lê o resultado de uma consulta com ordenação por tempo atual na tabela de usuarios configurada.
    /// </summary>
    public async Task<object?> RunTemporalNowOrderByAsync(params object[] pars)
    {
        var value = await Repo.ExecuteScalarAsync(Repo.Dialect.TemporalNowOrderBy(Context));
        GC.KeepAlive(value);
        return value;
    }

    private string[] NormalizeSnapshotColumnNames(string[] columnNames)
    {
        if (Repo.Dialect.Provider == ProviderId.Npgsql)
        {
            var normalized = new string[columnNames.Length];
            for (var i = 0; i < columnNames.Length; i++)
            {
                normalized[i] = columnNames[i] switch
                {
                    "Name" => "name",
                    "Id" => "id",
                    _ => columnNames[i]
                };
            }

            return normalized;
        }

        if (Repo.Dialect.Provider is not ProviderId.Oracle and not ProviderId.Db2)
            return columnNames;

        var normalized2 = new string[columnNames.Length];
        for (var i = 0; i < columnNames.Length; i++)
            normalized2[i] = columnNames[i].ToUpperInvariant();

        return normalized2;
    }

    private static void ValidateStringAggregateGroupCaseRow(
        DbDataReader reader,
        string expectedNameGroup,
        int expectedTotalCount,
        int expectedDistinctCount,
        string expectedFirstName,
        string expectedLastName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture)?.TrimEnd().Should().Be(expectedNameGroup);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedTotalCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDistinctCount);
        Convert.ToString(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedFirstName);
        Convert.ToString(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedLastName);
    }

    private static void ValidateGroupByNameInitialRow(
        DbDataReader reader,
        string expectedNameInitial,
        int expectedTotalCount,
        int expectedDistinctCount,
        int expectedAliceCount,
        int expectedBobCount,
        string expectedFirstName,
        string expectedLastName,
        int expectedHasAtLeastTwo)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedNameInitial);
        Convert.ToInt32(reader.GetValue(1), CultureInfo.InvariantCulture).Should().Be(expectedTotalCount);
        Convert.ToInt32(reader.GetValue(2), CultureInfo.InvariantCulture).Should().Be(expectedDistinctCount);
        Convert.ToInt32(reader.GetValue(3), CultureInfo.InvariantCulture).Should().Be(expectedAliceCount);
        Convert.ToInt32(reader.GetValue(4), CultureInfo.InvariantCulture).Should().Be(expectedBobCount);
        Convert.ToString(reader.GetValue(5), CultureInfo.InvariantCulture).Should().Be(expectedFirstName);
        Convert.ToString(reader.GetValue(6), CultureInfo.InvariantCulture).Should().Be(expectedLastName);
        Convert.ToInt32(reader.GetValue(7), CultureInfo.InvariantCulture).Should().Be(expectedHasAtLeastTwo);
    }

    private static void ValidateNamePaginationRow(DbDataReader reader, string expectedName)
    {
        Convert.ToString(reader.GetValue(0), CultureInfo.InvariantCulture).Should().Be(expectedName);
    }
}
