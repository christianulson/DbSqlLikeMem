namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Executes the MySQL recursive thought chain query and captures its ordered rowset.
/// PT-br: Executa a consulta MySQL recursiva da cadeia de pensamentos e captura seu conjunto de linhas ordenado.
/// </summary>
public sealed class MySqlThoughtChainRecursiveCteServiceTest(
    RepoService repo,
    FidelityTestContext context
    ) : BaseServiceTest(repo, context)
{
    /// <summary>
    /// EN: Runs the recursive CTE that walks thought edges from the requested node to its ancestors.
    /// PT-br: Executa a CTE recursiva que percorre as arestas de pensamento do no solicitado ate seus ancestrais.
    /// </summary>
    public async Task<QueryResultSnapshot> RunThoughtChainAsync(params object[] pars)
    {
        if (Repo.Cnn.State != System.Data.ConnectionState.Open)
            await Repo.Cnn.OpenAsync();

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = """
WITH RECURSIVE chain AS (
    SELECT node_id, 0 AS depth FROM thought_nodes WHERE node_id = @NodeId
    UNION ALL
    SELECT e.source_id, c.depth + 1
    FROM thought_edges e
    INNER JOIN chain c ON e.target_id = c.node_id
)
SELECT n.node_id AS NodeId, n.cycle_id AS CycleId, n.step_kind AS StepKind, n.type AS Type,
       n.moment_category AS MomentCategory, n.content_hash AS ContentHash,
       n.classification_json AS ClassificationJson, n.created_at AS CreatedAt, n.summary AS Summary
FROM thought_nodes n
INNER JOIN chain c ON n.node_id = c.node_id
ORDER BY c.depth
""";
        AddParameter(command, "NodeId", DbType.String, "leaf");

        using var reader = await command.ExecuteReaderAsync();
        return QueryResultSnapshotReader.Capture(reader);
    }
}
