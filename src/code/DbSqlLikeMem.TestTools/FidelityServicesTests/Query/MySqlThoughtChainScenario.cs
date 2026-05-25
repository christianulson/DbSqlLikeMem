namespace DbSqlLikeMem.TestTools.Query;

/// <summary>
/// EN: Creates the MySQL thought graph tables used by the recursive chain fidelity scenario.
/// PT-br: Cria as tabelas MySQL do grafo de pensamentos usadas pelo cenario de fidelidade de cadeia recursiva.
/// </summary>
public sealed class MySqlThoughtChainScenario(
    RepoService repo,
    FidelityTestContext context
    ) : BaseScenario(repo, context),
        ITestScenario
{
    /// <inheritdoc />
    public async Task CreateScenarioAsync()
    {
        await ExecuteNonQueryAsync("""
CREATE TABLE thought_nodes (
    node_id VARCHAR(40) NOT NULL PRIMARY KEY,
    cycle_id VARCHAR(40) NOT NULL,
    step_kind VARCHAR(40) NOT NULL,
    type VARCHAR(40) NOT NULL,
    moment_category VARCHAR(40) NOT NULL,
    content_hash VARCHAR(80) NOT NULL,
    classification_json JSON NULL,
    created_at DATETIME NOT NULL,
    summary VARCHAR(200) NULL
)
""");
        await ExecuteNonQueryAsync("""
CREATE TABLE thought_edges (
    source_id VARCHAR(40) NOT NULL,
    target_id VARCHAR(40) NOT NULL,
    PRIMARY KEY (source_id, target_id)
)
""");
        await ExecuteNonQueryAsync("""
INSERT INTO thought_nodes
    (node_id, cycle_id, step_kind, type, moment_category, content_hash, classification_json, created_at, summary)
VALUES
    ('root', 'cycle-1', 'Plan', 'Thought', 'Past', 'hash-root', '{"rank":1}', '2024-01-02 03:04:05', 'Root summary'),
    ('middle', 'cycle-1', 'Reason', 'Thought', 'Past', 'hash-middle', '{"rank":2}', '2024-01-02 03:04:05', 'Middle summary'),
    ('leaf', 'cycle-1', 'Answer', 'Thought', 'Current', 'hash-leaf', '{"rank":3}', '2024-01-02 03:04:05', 'Leaf summary'),
    ('side', 'cycle-1', 'Side', 'Thought', 'Past', 'hash-side', '{"rank":4}', '2024-01-02 03:04:05', 'Side summary')
""");
        await ExecuteNonQueryAsync("""
INSERT INTO thought_edges (source_id, target_id)
VALUES
    ('root', 'middle'),
    ('middle', 'leaf'),
    ('root', 'side')
""");
    }

    /// <inheritdoc />
    public async Task DropScenarioAsync()
    {
        await ExecuteNonQueryAsync("DROP TABLE IF EXISTS thought_edges");
        await ExecuteNonQueryAsync("DROP TABLE IF EXISTS thought_nodes");
    }

    private async Task ExecuteNonQueryAsync(string sql)
    {
        if (Repo.Cnn.State != System.Data.ConnectionState.Open)
            await Repo.Cnn.OpenAsync();

        using var command = Repo.Cnn.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }
}
