namespace DbSqlLikeMem;

internal sealed class DbConnectionSchemaSnapshotBridge(DbConnectionMockBase connection)
{
    public SchemaSnapshot ExportSchemaSnapshot()
        => SchemaSnapshot.Export(connection);

    public SchemaSnapshotSupportProfile GetSchemaSnapshotSupportProfile()
        => SchemaSnapshot.GetSupportProfile(connection);

    public void ImportSchemaSnapshotCore(
        SchemaSnapshot snapshot,
        bool ensureCompatibility)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(snapshot, nameof(snapshot));
        if (ensureCompatibility)
            snapshot.EnsureCompatibleWith(connection.Db);

        snapshot.ApplyTo(connection.Db);
        AlignCurrentDatabaseToImportedSchemas(snapshot);
    }

    private void AlignCurrentDatabaseToImportedSchemas(SchemaSnapshot snapshot)
    {
        if (connection.Db.TryGetValue(connection.Database, out _))
            return;

        string? nextDatabase = null;
        foreach (var schema in snapshot.Schemas)
        {
            if (schema.Tables.Count > 0
                || schema.Views.Count > 0
                || schema.Functions.Count > 0
                || schema.Procedures.Count > 0
                || schema.Sequences.Count > 0)
            {
                nextDatabase = schema.Name;
                break;
            }
        }

        if (nextDatabase is null)
        {
            foreach (var schema in snapshot.Schemas)
            {
                if (schema.Name.Equals("DefaultSchema", StringComparison.OrdinalIgnoreCase))
                {
                    nextDatabase = schema.Name;
                    break;
                }
            }
        }

        if (nextDatabase is null)
        {
            foreach (var schema in snapshot.Schemas)
            {
                nextDatabase = schema.Name;
                break;
            }
        }

        nextDatabase ??= "DefaultSchema";

        connection.ChangeDatabase(nextDatabase);
    }
}
