namespace DbSqlLikeMem.Benchmarks.Core;

public abstract partial class BenchmarkSessionBase
{
    /// <summary>
    /// EN: Exports the prepared schema snapshot through the shared service and keeps the snapshot alive.
    /// PT: Exporta o schema snapshot preparado pelo service compartilhado e mantem o snapshot ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotExport()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var snapshot = state.Service.RunSchemaSnapshotExport();
        GC.KeepAlive(snapshot);
    }

    /// <summary>
    /// EN: Serializes the prepared schema snapshot to JSON and keeps the result alive.
    /// PT: Serializa o schema snapshot preparado para JSON e mantem o resultado ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotToJson()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var json = state.Service.RunSchemaSnapshotToJson();
        GC.KeepAlive(json);
    }

    /// <summary>
    /// EN: Loads a schema snapshot from JSON through the shared service and keeps the result alive.
    /// PT: Carrega um schema snapshot a partir de JSON pelo service compartilhado e mantem o resultado ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotLoadJson()
    {
        var obj = SchemaSnapshotServiceOpsTest.RunSchemaSnapshotLoadJson(Dialect.DisplayName);
        GC.KeepAlive(obj);
    }

    /// <summary>
    /// EN: Applies the prepared schema snapshot through the shared service and keeps the result alive.
    /// PT: Aplica o schema snapshot preparado pelo service compartilhado e mantem o resultado ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotApply()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var applied = state.Service.RunSchemaSnapshotApply();
        GC.KeepAlive(applied);
    }

    /// <summary>
    /// EN: Performs the schema snapshot round trip through the shared service and keeps the result alive.
    /// PT: Executa o round trip do schema snapshot pelo service compartilhado e mantem o resultado ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotRoundTrip()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var obj = state.Service.RunSchemaSnapshotRoundTrip();
        GC.KeepAlive(obj);
    }

    /// <summary>
    /// EN: Compares schema snapshots through the shared service and keeps the comparison result alive.
    /// PT: Compara schema snapshots pelo service compartilhado e mantem o resultado da comparacao ativo.
    /// </summary>
    protected virtual void RunSchemaSnapshotCompare()
    {
        var state = GetPreparedSchemaSnapshotState("SchemaSnapshot");
        var comparison = state.Service.RunSchemaSnapshotCompare();
        GC.KeepAlive(comparison);
    }
}
