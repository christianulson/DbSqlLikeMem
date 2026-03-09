using DbSqlLikeMem.Models;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Linq;

namespace DbSqlLikeMem;

/// <summary>
/// EN: Captures and reapplies the structural schema state of a mock database.
/// PT: Captura e reaplica o estado estrutural de schema de um banco simulado.
/// </summary>
public sealed record SchemaSnapshot
{
    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();
    private static readonly IReadOnlyList<string> SupportedObjectKinds = Array.AsReadOnly([
        "schemas",
        "tables",
        "columns",
        "primary-keys",
        "indexes",
        "foreign-keys",
        "views",
        "sequences",
        "procedure-signatures",
        "file-roundtrip",
        "compatibility-gate",
        "drift-fingerprint",
        "drift-comparison"
    ]);
    private static readonly IReadOnlyList<string> UnsupportedObjectKinds = Array.AsReadOnly([
        "check-constraints",
        "computed-default-expressions",
        "computed-column-generators",
        "trigger-bodies",
        "procedure-bodies",
        "global-temporary-table-definitions"
    ]);

    /// <summary>
    /// EN: Serialized dialect name associated with the exported schema.
    /// PT: Nome do dialeto serializado associado ao schema exportado.
    /// </summary>
    public required string DialectName { get; init; }

    /// <summary>
    /// EN: Simulated provider version associated with the exported schema.
    /// PT: Versao simulada do provider associada ao schema exportado.
    /// </summary>
    public required int Version { get; init; }

    /// <summary>
    /// EN: Schemas captured in the snapshot.
    /// PT: Schemas capturados no snapshot.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotSchema> Schemas { get; init; }

    /// <summary>
    /// EN: Returns the current supported subset profile for this snapshot contract.
    /// PT: Retorna o perfil do subset atualmente suportado por este contrato de snapshot.
    /// </summary>
    /// <returns>EN: Supported subset profile. PT: Perfil do subset suportado.</returns>
    public SchemaSnapshotSupportProfile GetSupportProfile()
        => CreateSupportProfile(DialectName, Version);

    /// <summary>
    /// EN: Exports the current structural schema from a mock connection.
    /// PT: Exporta o schema estrutural atual de uma conexao simulada.
    /// </summary>
    /// <param name="connection">EN: Connection whose schema will be exported. PT: Conexao cujo schema sera exportado.</param>
    /// <returns>EN: Captured schema snapshot. PT: Snapshot de schema capturado.</returns>
    public static SchemaSnapshot Export(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return Export(connection.Db);
    }

    /// <summary>
    /// EN: Exports the current structural schema from a mock database.
    /// PT: Exporta o schema estrutural atual de um banco simulado.
    /// </summary>
    /// <param name="db">EN: Database whose schema will be exported. PT: Banco cujo schema sera exportado.</param>
    /// <returns>EN: Captured schema snapshot. PT: Snapshot de schema capturado.</returns>
    public static SchemaSnapshot Export(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));

        return new SchemaSnapshot
        {
            DialectName = db.Dialect.Name,
            Version = db.Version,
            Schemas = db.Values
                .OrderBy(static schema => schema.SchemaName, StringComparer.OrdinalIgnoreCase)
                .Select(static schema => SchemaSnapshotSchema.FromSchema(schema))
                .ToArray()
        };
    }

    /// <summary>
    /// EN: Returns the current supported subset profile for a target mock database.
    /// PT: Retorna o perfil do subset atualmente suportado para um banco simulado de destino.
    /// </summary>
    /// <param name="db">EN: Target database. PT: Banco de destino.</param>
    /// <returns>EN: Supported subset profile. PT: Perfil do subset suportado.</returns>
    public static SchemaSnapshotSupportProfile GetSupportProfile(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        return CreateSupportProfile(db.Dialect.Name, db.Version);
    }

    /// <summary>
    /// EN: Returns the current supported subset profile for a target mock connection.
    /// PT: Retorna o perfil do subset atualmente suportado para uma conexao simulada de destino.
    /// </summary>
    /// <param name="connection">EN: Target connection. PT: Conexao de destino.</param>
    /// <returns>EN: Supported subset profile. PT: Perfil do subset suportado.</returns>
    public static SchemaSnapshotSupportProfile GetSupportProfile(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return CreateSupportProfile(connection.Db.Dialect.Name, connection.Db.Version);
    }

    /// <summary>
    /// EN: Loads a snapshot from JSON text.
    /// PT: Carrega um snapshot a partir de texto JSON.
    /// </summary>
    /// <param name="json">EN: Serialized schema snapshot. PT: Snapshot de schema serializado.</param>
    /// <returns>EN: Deserialized schema snapshot. PT: Snapshot de schema desserializado.</returns>
    public static SchemaSnapshot Load(string json)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(json, nameof(json));

        var snapshot = JsonSerializer.Deserialize<SchemaSnapshot>(json, JsonOptions);
        if (snapshot is null)
            throw new InvalidOperationException("Schema snapshot JSON could not be deserialized.");

        return snapshot;
    }

    /// <summary>
    /// EN: Loads a snapshot from JSON text and applies it to the target database.
    /// PT: Carrega um snapshot a partir de texto JSON e o aplica ao banco de destino.
    /// </summary>
    /// <param name="json">EN: Serialized schema snapshot. PT: Snapshot de schema serializado.</param>
    /// <param name="db">EN: Target database to rebuild. PT: Banco de destino a reconstruir.</param>
    public static void Load(string json, DbMock db)
        => Load(json, db, ensureCompatibility: false);

    /// <summary>
    /// EN: Loads a snapshot from JSON text and applies it to the target database with optional compatibility validation.
    /// PT: Carrega um snapshot a partir de texto JSON e o aplica ao banco de destino com validacao opcional de compatibilidade.
    /// </summary>
    /// <param name="json">EN: Serialized schema snapshot. PT: Snapshot de schema serializado.</param>
    /// <param name="db">EN: Target database to rebuild. PT: Banco de destino a reconstruir.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public static void Load(
        string json,
        DbMock db,
        bool ensureCompatibility)
        => Load(json).ApplyTo(db, ensureCompatibility);

    /// <summary>
    /// EN: Loads a snapshot from JSON text and applies it to the target connection database.
    /// PT: Carrega um snapshot a partir de texto JSON e o aplica ao banco da conexao de destino.
    /// </summary>
    /// <param name="json">EN: Serialized schema snapshot. PT: Snapshot de schema serializado.</param>
    /// <param name="connection">EN: Target connection whose database will be rebuilt. PT: Conexao de destino cujo banco sera reconstruido.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public static void Load(
        string json,
        DbConnectionMockBase connection,
        bool ensureCompatibility = false)
        => Load(json).ApplyTo(connection, ensureCompatibility);

    /// <summary>
    /// EN: Loads a snapshot from a JSON file path.
    /// PT: Carrega um snapshot a partir do caminho de um arquivo JSON.
    /// </summary>
    /// <param name="path">EN: JSON file path. PT: Caminho do arquivo JSON.</param>
    /// <returns>EN: Deserialized schema snapshot. PT: Snapshot de schema desserializado.</returns>
    public static SchemaSnapshot LoadFromFile(string path)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(path, nameof(path));
        return Load(File.ReadAllText(path));
    }

    /// <summary>
    /// EN: Loads a snapshot from a JSON file path and applies it to the target database.
    /// PT: Carrega um snapshot a partir do caminho de um arquivo JSON e o aplica ao banco de destino.
    /// </summary>
    /// <param name="path">EN: JSON file path. PT: Caminho do arquivo JSON.</param>
    /// <param name="db">EN: Target database to rebuild. PT: Banco de destino a reconstruir.</param>
    public static void LoadFromFile(string path, DbMock db)
        => LoadFromFile(path, db, ensureCompatibility: false);

    /// <summary>
    /// EN: Loads a snapshot from a JSON file path and applies it to the target database with optional compatibility validation.
    /// PT: Carrega um snapshot a partir do caminho de um arquivo JSON e o aplica ao banco de destino com validacao opcional de compatibilidade.
    /// </summary>
    /// <param name="path">EN: JSON file path. PT: Caminho do arquivo JSON.</param>
    /// <param name="db">EN: Target database to rebuild. PT: Banco de destino a reconstruir.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public static void LoadFromFile(
        string path,
        DbMock db,
        bool ensureCompatibility)
        => LoadFromFile(path).ApplyTo(db, ensureCompatibility);

    /// <summary>
    /// EN: Loads a snapshot from a JSON file path and applies it to the target connection database.
    /// PT: Carrega um snapshot a partir do caminho de um arquivo JSON e o aplica ao banco da conexao de destino.
    /// </summary>
    /// <param name="path">EN: JSON file path. PT: Caminho do arquivo JSON.</param>
    /// <param name="connection">EN: Target connection whose database will be rebuilt. PT: Conexao de destino cujo banco sera reconstruido.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public static void LoadFromFile(
        string path,
        DbConnectionMockBase connection,
        bool ensureCompatibility = false)
        => LoadFromFile(path).ApplyTo(connection, ensureCompatibility);

    /// <summary>
    /// EN: Checks whether the snapshot is compatible with the target mock database provider and version.
    /// PT: Verifica se o snapshot e compativel com o provider e a versao do banco simulado de destino.
    /// </summary>
    /// <param name="db">EN: Target database to validate. PT: Banco de destino a validar.</param>
    /// <returns>EN: True when the target can consume the snapshot under the explicit compatibility gate. PT: True quando o destino pode consumir o snapshot sob o gate explicito de compatibilidade.</returns>
    public bool IsCompatibleWith(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));

        return string.Equals(DialectName, db.Dialect.Name, StringComparison.OrdinalIgnoreCase)
            && db.Version >= Version;
    }

    /// <summary>
    /// EN: Checks whether the snapshot is compatible with the target mock connection provider and version.
    /// PT: Verifica se o snapshot e compativel com o provider e a versao da conexao simulada de destino.
    /// </summary>
    /// <param name="connection">EN: Target connection to validate. PT: Conexao de destino a validar.</param>
    /// <returns>EN: True when the target can consume the snapshot under the explicit compatibility gate. PT: True quando o destino pode consumir o snapshot sob o gate explicito de compatibilidade.</returns>
    public bool IsCompatibleWith(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return IsCompatibleWith(connection.Db);
    }

    /// <summary>
    /// EN: Throws when the snapshot is not compatible with the target mock database provider and version.
    /// PT: Lanca erro quando o snapshot nao e compativel com o provider e a versao do banco simulado de destino.
    /// </summary>
    /// <param name="db">EN: Target database to validate. PT: Banco de destino a validar.</param>
    public void EnsureCompatibleWith(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));

        if (!string.Equals(DialectName, db.Dialect.Name, StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException(
                $"Schema snapshot dialect '{DialectName}' is not compatible with target dialect '{db.Dialect.Name}'.");
        }

        if (db.Version < Version)
        {
            throw new InvalidOperationException(
                $"Schema snapshot version {Version} requires target version >= {Version}, but target version is {db.Version}.");
        }
    }

    /// <summary>
    /// EN: Throws when the snapshot is not compatible with the target mock connection provider and version.
    /// PT: Lanca erro quando o snapshot nao e compativel com o provider e a versao da conexao simulada de destino.
    /// </summary>
    /// <param name="connection">EN: Target connection to validate. PT: Conexao de destino a validar.</param>
    public void EnsureCompatibleWith(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        EnsureCompatibleWith(connection.Db);
    }

    /// <summary>
    /// EN: Serializes the snapshot to JSON.
    /// PT: Serializa o snapshot para JSON.
    /// </summary>
    /// <returns>EN: JSON representation of the snapshot. PT: Representacao JSON do snapshot.</returns>
    public string ToJson()
        => JsonSerializer.Serialize(this, JsonOptions);

    /// <summary>
    /// EN: Computes a stable fingerprint for the current snapshot content.
    /// PT: Calcula uma assinatura estavel para o conteudo atual do snapshot.
    /// </summary>
    /// <returns>EN: Stable fingerprint string for the snapshot. PT: String de assinatura estavel do snapshot.</returns>
    public string GetFingerprint()
    {
        var utf8 = Encoding.UTF8.GetBytes(ToJson());
        using var sha = SHA256.Create();
        var hash = sha.ComputeHash(utf8);
        var sb = new StringBuilder(hash.Length * 2);
        foreach (var b in hash)
            sb.Append(b.ToString("x2"));
        return sb.ToString();
    }

    /// <summary>
    /// EN: Checks whether another snapshot has the same structural content.
    /// PT: Verifica se outro snapshot possui o mesmo conteudo estrutural.
    /// </summary>
    /// <param name="other">EN: Snapshot to compare. PT: Snapshot a comparar.</param>
    /// <returns>EN: True when both snapshots have the same structural content. PT: True quando os dois snapshots possuem o mesmo conteudo estrutural.</returns>
    public bool Matches(SchemaSnapshot other)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(other, nameof(other));
        return string.Equals(GetFingerprint(), other.GetFingerprint(), StringComparison.Ordinal);
    }

    /// <summary>
    /// EN: Checks whether the target mock database matches the current snapshot content.
    /// PT: Verifica se o banco simulado de destino corresponde ao conteudo atual do snapshot.
    /// </summary>
    /// <param name="db">EN: Target database to compare. PT: Banco de destino a comparar.</param>
    /// <returns>EN: True when the target database exports to the same structural content. PT: True quando o banco de destino exporta para o mesmo conteudo estrutural.</returns>
    public bool Matches(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        return Matches(Export(db));
    }

    /// <summary>
    /// EN: Checks whether the target mock connection database matches the current snapshot content.
    /// PT: Verifica se o banco da conexao simulada de destino corresponde ao conteudo atual do snapshot.
    /// </summary>
    /// <param name="connection">EN: Target connection to compare. PT: Conexao de destino a comparar.</param>
    /// <returns>EN: True when the target connection database exports to the same structural content. PT: True quando o banco da conexao de destino exporta para o mesmo conteudo estrutural.</returns>
    public bool Matches(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return Matches(connection.Db);
    }

    /// <summary>
    /// EN: Compares the current snapshot against another snapshot and returns a structured drift report.
    /// PT: Compara o snapshot atual com outro snapshot e retorna um relatorio estruturado de drift.
    /// </summary>
    /// <param name="other">EN: Snapshot to compare. PT: Snapshot a comparar.</param>
    /// <returns>EN: Structured comparison result. PT: Resultado estruturado da comparacao.</returns>
    public SchemaSnapshotComparison CompareTo(SchemaSnapshot other)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(other, nameof(other));

        var differences = new List<string>();
        CompareScalar(nameof(DialectName), DialectName, other.DialectName, differences);
        CompareScalar(nameof(Version), Version, other.Version, differences);

        CompareNamedCollection(
            "Schema",
            Schemas,
            other.Schemas,
            static schema => schema.Name,
            static (left, right) => CompareSchema(left, right),
            differences);

        return new SchemaSnapshotComparison(
            IsMatch: differences.Count == 0,
            SourceFingerprint: GetFingerprint(),
            TargetFingerprint: other.GetFingerprint(),
            Differences: differences);
    }

    /// <summary>
    /// EN: Compares the current snapshot against a target mock database and returns a structured drift report.
    /// PT: Compara o snapshot atual com um banco simulado de destino e retorna um relatorio estruturado de drift.
    /// </summary>
    /// <param name="db">EN: Target database to compare. PT: Banco de destino a comparar.</param>
    /// <returns>EN: Structured comparison result. PT: Resultado estruturado da comparacao.</returns>
    public SchemaSnapshotComparison CompareTo(DbMock db)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        return CompareTo(Export(db));
    }

    /// <summary>
    /// EN: Compares the current snapshot against a target mock connection database and returns a structured drift report.
    /// PT: Compara o snapshot atual com o banco de uma conexao simulada de destino e retorna um relatorio estruturado de drift.
    /// </summary>
    /// <param name="connection">EN: Target connection to compare. PT: Conexao de destino a comparar.</param>
    /// <returns>EN: Structured comparison result. PT: Resultado estruturado da comparacao.</returns>
    public SchemaSnapshotComparison CompareTo(DbConnectionMockBase connection)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        return CompareTo(connection.Db);
    }

    /// <summary>
    /// EN: Writes the snapshot JSON representation to a file path.
    /// PT: Escreve a representacao JSON do snapshot em um caminho de arquivo.
    /// </summary>
    /// <param name="path">EN: Target JSON file path. PT: Caminho do arquivo JSON de destino.</param>
    public void SaveToFile(string path)
    {
        ArgumentExceptionCompatible.ThrowIfNullOrWhiteSpace(path, nameof(path));
        File.WriteAllText(path, ToJson());
    }

    /// <summary>
    /// EN: Rebuilds the structural schema of the target database from this snapshot.
    /// PT: Reconstroi o schema estrutural do banco de destino a partir deste snapshot.
    /// </summary>
    /// <param name="db">EN: Target database to rebuild. PT: Banco de destino a reconstruir.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public void ApplyTo(
        DbMock db,
        bool ensureCompatibility = false)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(db, nameof(db));
        if (ensureCompatibility)
            EnsureCompatibleWith(db);

        db.Clear();
        db.ClearGlobalTemporaryTables();

        foreach (var schemaSnapshot in Schemas)
        {
            var schema = db.CreateSchema(schemaSnapshot.Name);
            var schemaMock = (SchemaMock)schema;
            foreach (var tableSnapshot in schemaSnapshot.Tables)
            {
                var created = schemaMock.CreateTable(
                    tableSnapshot.Name,
                    tableSnapshot.Columns.Select(static column => column.ToCol()));
                created.NextIdentity = tableSnapshot.NextIdentity;
            }

            foreach (var tableSnapshot in schemaSnapshot.Tables)
            {
                var table = db.GetTable(tableSnapshot.Name, schemaSnapshot.Name);
                if (tableSnapshot.PrimaryKeyColumns.Count > 0)
                    table.AddPrimaryKeyIndexes([.. tableSnapshot.PrimaryKeyColumns]);

                foreach (var indexSnapshot in tableSnapshot.Indexes)
                    table.CreateIndex(indexSnapshot.Name, indexSnapshot.KeyColumns, [.. indexSnapshot.IncludeColumns], indexSnapshot.Unique);
            }

            foreach (var tableSnapshot in schemaSnapshot.Tables)
            {
                var table = db.GetTable(tableSnapshot.Name, schemaSnapshot.Name);
                foreach (var foreignKeySnapshot in tableSnapshot.ForeignKeys)
                {
                    var referencedTable = string.IsNullOrWhiteSpace(foreignKeySnapshot.RefSchemaName)
                        ? foreignKeySnapshot.RefTableName
                        : $"{foreignKeySnapshot.RefSchemaName}.{foreignKeySnapshot.RefTableName}";
                    table.CreateForeignKey(
                        foreignKeySnapshot.Name,
                        referencedTable,
                        [.. foreignKeySnapshot.References.Select(static reference => (reference.ColumnName, reference.RefColumnName))]);
                }
            }

            foreach (var viewSnapshot in schemaSnapshot.Views)
            {
                var parsed = SqlQueryParser.Parse(viewSnapshot.SelectSql, db.Dialect);
                schemaMock.Views[viewSnapshot.Name] = parsed as SqlSelectQuery
                    ?? throw new InvalidOperationException($"View '{viewSnapshot.Name}' did not deserialize to SELECT.");
            }

            foreach (var sequenceSnapshot in schemaSnapshot.Sequences)
            {
                db.AddSequence(
                    sequenceSnapshot.Name,
                    startValue: sequenceSnapshot.StartValue,
                    incrementBy: sequenceSnapshot.IncrementBy,
                    currentValue: sequenceSnapshot.CurrentValue,
                    schemaName: schemaSnapshot.Name);
            }

            foreach (var procedureSnapshot in schemaSnapshot.Procedures)
                db.AddProdecure(procedureSnapshot.Name, procedureSnapshot.ToProcedureDef(), schemaSnapshot.Name);
        }

        if (db.Count == 0)
            db.CreateSchema("DefaultSchema");
    }

    /// <summary>
    /// EN: Rebuilds the structural schema of the target connection database from this snapshot.
    /// PT: Reconstroi o schema estrutural do banco da conexao de destino a partir deste snapshot.
    /// </summary>
    /// <param name="connection">EN: Target connection whose database will be rebuilt. PT: Conexao de destino cujo banco sera reconstruido.</param>
    /// <param name="ensureCompatibility">EN: True to enforce dialect/version compatibility before replay. PT: True para exigir compatibilidade de dialeto/versao antes do replay.</param>
    public void ApplyTo(
        DbConnectionMockBase connection,
        bool ensureCompatibility = false)
    {
        ArgumentNullExceptionCompatible.ThrowIfNull(connection, nameof(connection));
        connection.ImportSchemaSnapshotCore(this, ensureCompatibility);
    }

    private static void CompareScalar<T>(
        string label,
        T left,
        T right,
        List<string> differences)
    {
        if (EqualityComparer<T>.Default.Equals(left, right))
            return;

        differences.Add($"{label} differs: '{left}' vs '{right}'.");
    }

    private static void CompareNamedCollection<T>(
        string entityLabel,
        IReadOnlyList<T> left,
        IReadOnlyList<T> right,
        Func<T, string> keySelector,
        Func<T, T, IEnumerable<string>> itemComparer,
        List<string> differences)
    {
        var leftMap = left.ToDictionary(keySelector, StringComparer.OrdinalIgnoreCase);
        var rightMap = right.ToDictionary(keySelector, StringComparer.OrdinalIgnoreCase);

        foreach (var missing in leftMap.Keys.Except(rightMap.Keys, StringComparer.OrdinalIgnoreCase).OrderBy(static key => key, StringComparer.OrdinalIgnoreCase))
            differences.Add($"{entityLabel} missing in target: '{missing}'.");

        foreach (var extra in rightMap.Keys.Except(leftMap.Keys, StringComparer.OrdinalIgnoreCase).OrderBy(static key => key, StringComparer.OrdinalIgnoreCase))
            differences.Add($"{entityLabel} only in target: '{extra}'.");

        foreach (var shared in leftMap.Keys.Intersect(rightMap.Keys, StringComparer.OrdinalIgnoreCase).OrderBy(static key => key, StringComparer.OrdinalIgnoreCase))
            differences.AddRange(itemComparer(leftMap[shared], rightMap[shared]));
    }

    private static IEnumerable<string> CompareSchema(
        SchemaSnapshotSchema left,
        SchemaSnapshotSchema right)
    {
        var differences = new List<string>();
        var schemaName = left.Name;

        CompareNamedCollection(
            $"Table in schema '{schemaName}'",
            left.Tables,
            right.Tables,
            static table => table.Name,
            (tableLeft, tableRight) => CompareSerializedObject(
                $"Table '{tableLeft.Name}' in schema '{schemaName}'",
                tableLeft,
                tableRight),
            differences);

        CompareNamedCollection(
            $"View in schema '{schemaName}'",
            left.Views,
            right.Views,
            static view => view.Name,
            (viewLeft, viewRight) => CompareSerializedObject(
                $"View '{viewLeft.Name}' in schema '{schemaName}'",
                viewLeft,
                viewRight),
            differences);

        CompareNamedCollection(
            $"Procedure in schema '{schemaName}'",
            left.Procedures,
            right.Procedures,
            static procedure => procedure.Name,
            (procedureLeft, procedureRight) => CompareSerializedObject(
                $"Procedure '{procedureLeft.Name}' in schema '{schemaName}'",
                procedureLeft,
                procedureRight),
            differences);

        CompareNamedCollection(
            $"Sequence in schema '{schemaName}'",
            left.Sequences,
            right.Sequences,
            static sequence => sequence.Name,
            (sequenceLeft, sequenceRight) => CompareSerializedObject(
                $"Sequence '{sequenceLeft.Name}' in schema '{schemaName}'",
                sequenceLeft,
                sequenceRight),
            differences);

        return differences;
    }

    private static IEnumerable<string> CompareSerializedObject<T>(
        string label,
        T left,
        T right)
    {
        var leftJson = JsonSerializer.Serialize(left, JsonOptions);
        var rightJson = JsonSerializer.Serialize(right, JsonOptions);
        if (string.Equals(leftJson, rightJson, StringComparison.Ordinal))
            return [];

        return [$"{label} differs."];
    }

    private static SchemaSnapshotSupportProfile CreateSupportProfile(
        string dialectName,
        int version)
        => new(
            DialectName: dialectName,
            Version: version,
            SupportedObjects: SupportedObjectKinds,
            UnsupportedObjects: UnsupportedObjectKinds);

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions(JsonSerializerDefaults.Web)
        {
            WriteIndented = true
        };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }

    internal static JsonElement? SerializeDefaultValue(object? value)
    {
        if (value is null)
            return null;

        return JsonSerializer.SerializeToElement(value, value.GetType(), JsonOptions);
    }

    internal static object? DeserializeDefaultValue(JsonElement? value)
    {
        if (!value.HasValue)
            return null;

        return value.Value.ValueKind switch
        {
            JsonValueKind.Null => null,
            JsonValueKind.String => value.Value.GetString(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Number when value.Value.TryGetInt32(out var i) => i,
            JsonValueKind.Number when value.Value.TryGetInt64(out var l) => l,
            JsonValueKind.Number when value.Value.TryGetDecimal(out var d) => d,
            JsonValueKind.Number => value.Value.GetDouble(),
            _ => value.Value.GetRawText()
        };
    }
}

/// <summary>
/// EN: Captures a schema and its tables inside a schema snapshot.
/// PT: Captura um schema e suas tabelas dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotSchema
{
    /// <summary>
    /// EN: Schema name.
    /// PT: Nome do schema.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Tables captured for the schema.
    /// PT: Tabelas capturadas para o schema.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotTable> Tables { get; init; }

    /// <summary>
    /// EN: Views captured for the schema.
    /// PT: Views capturadas para o schema.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotView> Views { get; init; }

    /// <summary>
    /// EN: Stored procedures captured for the schema.
    /// PT: Procedures capturadas para o schema.
    /// </summary>
    public IReadOnlyList<SchemaSnapshotProcedure> Procedures { get; init; } = [];

    /// <summary>
    /// EN: Sequences captured for the schema.
    /// PT: Sequences capturadas para o schema.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotSequence> Sequences { get; init; }

    internal static SchemaSnapshotSchema FromSchema(SchemaMock schema)
        => new()
        {
            Name = schema.SchemaName,
            Tables = schema.Tables
                .OrderBy(static table => table.Key, StringComparer.OrdinalIgnoreCase)
                .Select(static table => SchemaSnapshotTable.FromTable(table.Value))
                .ToArray(),
            Views = schema.Views
                .OrderBy(static view => view.Key, StringComparer.OrdinalIgnoreCase)
                .Select(static view => SchemaSnapshotView.FromView(view.Key, view.Value))
                .ToArray(),
            Procedures = schema.Procedures
                .OrderBy(static procedure => procedure.Key, StringComparer.OrdinalIgnoreCase)
                .Select(static procedure => SchemaSnapshotProcedure.FromProcedure(procedure.Key, procedure.Value))
                .ToArray(),
            Sequences = schema.Sequences
                .OrderBy(static sequence => sequence.Key, StringComparer.OrdinalIgnoreCase)
                .Select(static sequence => SchemaSnapshotSequence.FromSequence(sequence.Value))
                .ToArray()
        };
}

/// <summary>
/// EN: Captures the structural metadata of a table inside a schema snapshot.
/// PT: Captura os metadados estruturais de uma tabela dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotTable
{
    /// <summary>
    /// EN: Table name.
    /// PT: Nome da tabela.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Next identity value stored for the table.
    /// PT: Proximo valor de identidade armazenado para a tabela.
    /// </summary>
    public required int NextIdentity { get; init; }

    /// <summary>
    /// EN: Columns captured for the table.
    /// PT: Colunas capturadas para a tabela.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotColumn> Columns { get; init; }

    /// <summary>
    /// EN: Primary key columns captured for the table.
    /// PT: Colunas da chave primaria capturadas para a tabela.
    /// </summary>
    public required IReadOnlyList<string> PrimaryKeyColumns { get; init; }

    /// <summary>
    /// EN: Secondary indexes captured for the table.
    /// PT: Indices secundarios capturados para a tabela.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotIndex> Indexes { get; init; }

    /// <summary>
    /// EN: Foreign keys captured for the table.
    /// PT: Chaves estrangeiras capturadas para a tabela.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotForeignKey> ForeignKeys { get; init; }

    internal static SchemaSnapshotTable FromTable(ITableMock table)
        => new()
        {
            Name = table.TableName,
            NextIdentity = table.NextIdentity,
            Columns = table.Columns.Values
                .OrderBy(static column => column.Index)
                .Select(static column => SchemaSnapshotColumn.FromColumn(column))
                .ToArray(),
            PrimaryKeyColumns = table.PrimaryKeyIndexes
                .OrderBy(static index => index)
                .Select(index => table.Columns.Values.Single(column => column.Index == index).Name)
                .ToArray(),
            Indexes = table.Indexes.Values
                .OrderBy(static index => index.Name, StringComparer.OrdinalIgnoreCase)
                .Select(static index => SchemaSnapshotIndex.FromIndex(index))
                .ToArray(),
            ForeignKeys = table.ForeignKeys.Values
                .OrderBy(static foreignKey => foreignKey.Name, StringComparer.OrdinalIgnoreCase)
                .Select(static foreignKey => SchemaSnapshotForeignKey.FromForeignKey(foreignKey))
                .ToArray()
        };
}

/// <summary>
/// EN: Captures the structural metadata of a single column inside a schema snapshot.
/// PT: Captura os metadados estruturais de uma unica coluna dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotColumn
{
    /// <summary>
    /// EN: Column name.
    /// PT: Nome da coluna.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Serialized database type of the column.
    /// PT: Tipo de banco serializado da coluna.
    /// </summary>
    public required DbType DbType { get; init; }

    /// <summary>
    /// EN: Indicates whether the column accepts null values.
    /// PT: Indica se a coluna aceita valores nulos.
    /// </summary>
    public required bool Nullable { get; init; }

    /// <summary>
    /// EN: Optional size metadata for the column.
    /// PT: Metadado opcional de tamanho da coluna.
    /// </summary>
    public int? Size { get; init; }

    /// <summary>
    /// EN: Optional decimal places metadata for the column.
    /// PT: Metadado opcional de casas decimais da coluna.
    /// </summary>
    public int? DecimalPlaces { get; init; }

    /// <summary>
    /// EN: Indicates whether the column is identity/auto increment.
    /// PT: Indica se a coluna e identity/auto incremento.
    /// </summary>
    public required bool Identity { get; init; }

    /// <summary>
    /// EN: Serialized default value when present.
    /// PT: Valor padrao serializado quando presente.
    /// </summary>
    public JsonElement? DefaultValue { get; init; }

    /// <summary>
    /// EN: Optional enum value set configured for the column.
    /// PT: Conjunto opcional de valores enum configurado para a coluna.
    /// </summary>
    public IReadOnlyList<string>? EnumValues { get; init; }

    internal static SchemaSnapshotColumn FromColumn(ColumnDef column)
        => new()
        {
            Name = column.Name,
            DbType = column.DbType,
            Nullable = column.Nullable,
            Size = column.Size,
            DecimalPlaces = column.DecimalPlaces,
            Identity = column.Identity,
            DefaultValue = SchemaSnapshot.SerializeDefaultValue(column.DefaultValue),
            EnumValues = column.EnumValues.OrderBy(static value => value, StringComparer.OrdinalIgnoreCase).ToArray()
        };

    internal Col ToCol()
        => new(
            name: Name,
            dbType: DbType,
            nullable: Nullable,
            size: Size,
            decimalPlaces: DecimalPlaces,
            identity: Identity,
            defaultValue: SchemaSnapshot.DeserializeDefaultValue(DefaultValue),
            enumValues: EnumValues is null ? null : [.. EnumValues]);
}

/// <summary>
/// EN: Captures a non-materialized view definition inside a schema snapshot.
/// PT: Captura a definicao de uma view nao materializada dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotView
{
    /// <summary>
    /// EN: View name.
    /// PT: Nome da view.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Raw SELECT SQL used to rebuild the view definition.
    /// PT: SQL bruto do SELECT usado para reconstruir a definicao da view.
    /// </summary>
    public required string SelectSql { get; init; }

    internal static SchemaSnapshotView FromView(string name, SqlSelectQuery query)
        => new()
        {
            Name = name,
            SelectSql = query.RawSql
        };
}

/// <summary>
/// EN: Captures a secondary index definition inside a schema snapshot.
/// PT: Captura a definicao de um indice secundario dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotIndex
{
    /// <summary>
    /// EN: Index name.
    /// PT: Nome do indice.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Key columns that compose the index.
    /// PT: Colunas chave que compoem o indice.
    /// </summary>
    public required IReadOnlyList<string> KeyColumns { get; init; }

    /// <summary>
    /// EN: Included non-key columns.
    /// PT: Colunas incluídas fora da chave.
    /// </summary>
    public required IReadOnlyList<string> IncludeColumns { get; init; }

    /// <summary>
    /// EN: Indicates whether the index is unique.
    /// PT: Indica se o indice e unico.
    /// </summary>
    public required bool Unique { get; init; }

    internal static SchemaSnapshotIndex FromIndex(IndexDef index)
        => new()
        {
            Name = index.Name,
            KeyColumns = [.. index.KeyCols],
            IncludeColumns = [.. index.Include],
            Unique = index.Unique
        };
}

/// <summary>
/// EN: Captures a foreign key definition inside a schema snapshot.
/// PT: Captura a definicao de uma chave estrangeira dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotForeignKey
{
    /// <summary>
    /// EN: Foreign key name.
    /// PT: Nome da chave estrangeira.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Referenced table name.
    /// PT: Nome da tabela referenciada.
    /// </summary>
    public required string RefTableName { get; init; }

    /// <summary>
    /// EN: Referenced schema name when the foreign key points outside the local schema.
    /// PT: Nome do schema referenciado quando a chave estrangeira aponta para fora do schema local.
    /// </summary>
    public string? RefSchemaName { get; init; }

    /// <summary>
    /// EN: Column mappings captured for the foreign key.
    /// PT: Mapeamentos de colunas capturados para a chave estrangeira.
    /// </summary>
    public required IReadOnlyList<SchemaSnapshotForeignKeyReference> References { get; init; }

    internal static SchemaSnapshotForeignKey FromForeignKey(ForeignDef foreignKey)
        => new()
        {
            Name = foreignKey.Name,
            RefTableName = foreignKey.RefTable.TableName,
            RefSchemaName = string.Equals(
                foreignKey.Table.Schema.SchemaName,
                foreignKey.RefTable.Schema.SchemaName,
                StringComparison.OrdinalIgnoreCase)
                ? null
                : foreignKey.RefTable.Schema.SchemaName,
            References = foreignKey.References
                .OrderBy(static reference => reference.col.Index)
                .Select(static reference => new SchemaSnapshotForeignKeyReference
                {
                    ColumnName = reference.col.Name,
                    RefColumnName = reference.refCol.Name
                })
                .ToArray()
        };
}

/// <summary>
/// EN: Captures a single local-to-referenced column mapping of a foreign key.
/// PT: Captura um unico mapeamento de coluna local-para-referenciada de uma chave estrangeira.
/// </summary>
public sealed record SchemaSnapshotForeignKeyReference
{
    /// <summary>
    /// EN: Local column name.
    /// PT: Nome da coluna local.
    /// </summary>
    public required string ColumnName { get; init; }

    /// <summary>
    /// EN: Referenced column name.
    /// PT: Nome da coluna referenciada.
    /// </summary>
    public required string RefColumnName { get; init; }
}

/// <summary>
/// EN: Captures a sequence definition and current state inside a schema snapshot.
/// PT: Captura uma definicao de sequence e seu estado atual dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotSequence
{
    /// <summary>
    /// EN: Sequence name.
    /// PT: Nome da sequence.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: First value produced by the sequence.
    /// PT: Primeiro valor produzido pela sequence.
    /// </summary>
    public required long StartValue { get; init; }

    /// <summary>
    /// EN: Increment step between generated values.
    /// PT: Passo de incremento entre os valores gerados.
    /// </summary>
    public required long IncrementBy { get; init; }

    /// <summary>
    /// EN: Current sequence value when already initialized.
    /// PT: Valor atual da sequence quando ja inicializada.
    /// </summary>
    public long? CurrentValue { get; init; }

    internal static SchemaSnapshotSequence FromSequence(SequenceDef sequence)
        => new()
        {
            Name = sequence.Name,
            StartValue = sequence.StartValue,
            IncrementBy = sequence.IncrementBy,
            CurrentValue = sequence.CurrentValue
        };
}

/// <summary>
/// EN: Captures a stored procedure signature inside a schema snapshot.
/// PT: Captura a assinatura de uma procedure dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotProcedure
{
    /// <summary>
    /// EN: Procedure name.
    /// PT: Nome da procedure.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Required input parameters of the procedure.
    /// PT: Parametros de entrada obrigatorios da procedure.
    /// </summary>
    public IReadOnlyList<SchemaSnapshotProcParam> RequiredIn { get; init; } = [];

    /// <summary>
    /// EN: Optional input parameters of the procedure.
    /// PT: Parametros de entrada opcionais da procedure.
    /// </summary>
    public IReadOnlyList<SchemaSnapshotProcParam> OptionalIn { get; init; } = [];

    /// <summary>
    /// EN: Output parameters of the procedure.
    /// PT: Parametros de saida da procedure.
    /// </summary>
    public IReadOnlyList<SchemaSnapshotProcParam> OutParams { get; init; } = [];

    /// <summary>
    /// EN: Optional return parameter of the procedure.
    /// PT: Parametro de retorno opcional da procedure.
    /// </summary>
    public SchemaSnapshotProcParam? ReturnParam { get; init; }

    internal static SchemaSnapshotProcedure FromProcedure(string name, ProcedureDef procedure)
        => new()
        {
            Name = name,
            RequiredIn = procedure.RequiredIn
                .Select(static parameter => SchemaSnapshotProcParam.FromParameter(parameter))
                .ToArray(),
            OptionalIn = procedure.OptionalIn
                .Select(static parameter => SchemaSnapshotProcParam.FromParameter(parameter))
                .ToArray(),
            OutParams = procedure.OutParams
                .Select(static parameter => SchemaSnapshotProcParam.FromParameter(parameter))
                .ToArray(),
            ReturnParam = procedure.ReturnParam is null
                ? null
                : SchemaSnapshotProcParam.FromParameter(procedure.ReturnParam)
        };

    internal ProcedureDef ToProcedureDef()
        => new(
            RequiredIn.Select(static parameter => parameter.ToParameter()).ToArray(),
            OptionalIn.Select(static parameter => parameter.ToParameter()).ToArray(),
            OutParams.Select(static parameter => parameter.ToParameter()).ToArray(),
            ReturnParam?.ToParameter());
}

/// <summary>
/// EN: Captures a stored procedure parameter inside a schema snapshot.
/// PT: Captura um parametro de procedure dentro de um snapshot de schema.
/// </summary>
public sealed record SchemaSnapshotProcParam
{
    /// <summary>
    /// EN: Parameter name.
    /// PT: Nome do parametro.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    /// EN: Serialized database type of the parameter.
    /// PT: Tipo de banco serializado do parametro.
    /// </summary>
    public required DbType DbType { get; init; }

    /// <summary>
    /// EN: Indicates whether the parameter is required.
    /// PT: Indica se o parametro e obrigatorio.
    /// </summary>
    public required bool Required { get; init; }

    /// <summary>
    /// EN: Serialized parameter default/current value when present.
    /// PT: Valor padrao/atual serializado do parametro quando presente.
    /// </summary>
    public JsonElement? Value { get; init; }

    internal static SchemaSnapshotProcParam FromParameter(ProcParam parameter)
        => new()
        {
            Name = parameter.Name,
            DbType = parameter.DbType,
            Required = parameter.Required,
            Value = SchemaSnapshot.SerializeDefaultValue(parameter.Value)
        };

    internal ProcParam ToParameter()
        => new(
            Name,
            DbType,
            Required,
            SchemaSnapshot.DeserializeDefaultValue(Value));
}

/// <summary>
/// EN: Reports structural drift detected while comparing two schema snapshots.
/// PT: Reporta o drift estrutural detectado ao comparar dois schema snapshots.
/// </summary>
/// <param name="IsMatch">EN: Indicates whether both sides are structurally equivalent. PT: Indica se os dois lados sao estruturalmente equivalentes.</param>
/// <param name="SourceFingerprint">EN: Fingerprint of the source snapshot. PT: Fingerprint do snapshot de origem.</param>
/// <param name="TargetFingerprint">EN: Fingerprint of the snapshot or database compared against the source. PT: Fingerprint do snapshot ou banco comparado com a origem.</param>
/// <param name="Differences">EN: Human-readable drift entries detected during comparison. PT: Entradas legiveis de drift detectadas durante a comparacao.</param>
public sealed record SchemaSnapshotComparison(
    bool IsMatch,
    string SourceFingerprint,
    string TargetFingerprint,
    IReadOnlyList<string> Differences)
{
    /// <summary>
    /// EN: Formats the comparison as multi-line text for logs and diagnostics.
    /// PT: Formata a comparacao como texto multilinha para logs e diagnosticos.
    /// </summary>
    /// <returns>EN: Multi-line comparison text. PT: Texto multilinha da comparacao.</returns>
    public string ToText()
    {
        var lines = new List<string>
        {
            $"IsMatch: {IsMatch}",
            $"SourceFingerprint: {SourceFingerprint}",
            $"TargetFingerprint: {TargetFingerprint}"
        };

        if (Differences.Count == 0)
        {
            lines.Add("Differences: none");
            return string.Join(Environment.NewLine, lines);
        }

        lines.Add("Differences:");
        lines.AddRange(Differences.Select(static difference => $"- {difference}"));
        return string.Join(Environment.NewLine, lines);
    }
}

/// <summary>
/// EN: Describes the currently supported schema snapshot subset for a dialect/version pair.
/// PT: Descreve o subset atualmente suportado de schema snapshot para um par de dialeto/versao.
/// </summary>
/// <param name="DialectName">EN: Dialect name of the profiled target. PT: Nome do dialeto do alvo perfilado.</param>
/// <param name="Version">EN: Simulated provider version of the profiled target. PT: Versao simulada do provider do alvo perfilado.</param>
/// <param name="SupportedObjects">EN: Structural object families explicitly covered by the snapshot contract. PT: Familias de objetos estruturais explicitamente cobertas pelo contrato de snapshot.</param>
/// <param name="UnsupportedObjects">EN: Metadata or executable objects intentionally outside the current snapshot contract. PT: Metadados ou objetos executaveis intencionalmente fora do contrato atual de snapshot.</param>
public sealed record SchemaSnapshotSupportProfile(
    string DialectName,
    int Version,
    IReadOnlyList<string> SupportedObjects,
    IReadOnlyList<string> UnsupportedObjects)
{
    /// <summary>
    /// EN: Formats the support profile as multi-line text for diagnostics and documentation.
    /// PT: Formata o perfil de suporte como texto multilinha para diagnosticos e documentacao.
    /// </summary>
    /// <returns>EN: Multi-line support profile text. PT: Texto multilinha do perfil de suporte.</returns>
    public string ToText()
    {
        var lines = new List<string>
        {
            $"DialectName: {DialectName}",
            $"Version: {Version}",
            "SupportedObjects:"
        };

        lines.AddRange(SupportedObjects.Select(static item => $"- {item}"));
        lines.Add("UnsupportedObjects:");
        lines.AddRange(UnsupportedObjects.Select(static item => $"- {item}"));
        return string.Join(Environment.NewLine, lines);
    }
}
