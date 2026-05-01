namespace DbSqlLikeMem;

/// <summary>
/// EN: Bridges schema snapshot export, import, and support-profile queries for a connection.
/// PT-br: Faz a ponte entre exportacao, importacao e consulta de perfil de suporte de schema snapshot para uma conexao.
/// </summary>
internal sealed class DbConnectionSchemaSnapshotBridge(DbConnectionMockBase connection)
{
    /// <summary>
    /// EN: Exports the current database schema into a snapshot.
    /// PT-br: Exporta o schema atual do banco para um snapshot.
    /// </summary>
    public SchemaSnapshot ExportSchemaSnapshot()
        => SchemaSnapshot.Export(connection);

    /// <summary>
    /// EN: Gets the schema snapshot support profile for the current connection.
    /// PT-br: Obtém o perfil de suporte do schema snapshot para a conexao atual.
    /// </summary>
    public SchemaSnapshotSupportProfile GetSchemaSnapshotSupportProfile()
        => SchemaSnapshot.GetSupportProfile(connection);

    /// <summary>
    /// EN: Imports a schema snapshot into the current database connection.
    /// PT-br: Importa um schema snapshot para a conexao de banco atual.
    /// </summary>
    /// <param name="snapshot">EN: Snapshot to import. PT-br: Snapshot a importar.</param>
    /// <param name="ensureCompatibility">EN: Whether to validate compatibility before importing. PT-br: Indica se a compatibilidade deve ser validada antes da importacao.</param>
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
