using DbSqlLikeMem.VisualStudioExtension.Core.Generation;
using DbSqlLikeMem.VisualStudioExtension.Core.Models;

namespace DbSqlLikeMem.VisualStudioExtension.Core.Validation;

/// <summary>
/// Represents this public API type.
/// Representa este tipo público da API.
/// </summary>
public sealed class ObjectConsistencyChecker
{
    /// <summary>
    /// EN: Returns the missing artifact kinds required by the generation flow in deterministic order.
    /// PT: Retorna os tipos de artefato ausentes exigidos pelo fluxo de geracao em ordem deterministica.
    /// </summary>
    /// <param name="hasPrimaryClass">EN: Indicates whether the main generated class exists. PT: Indica se a classe gerada principal existe.</param>
    /// <param name="hasModel">EN: Indicates whether the companion model file exists. PT: Indica se o arquivo complementar de modelo existe.</param>
    /// <param name="hasRepository">EN: Indicates whether the companion repository file exists. PT: Indica se o arquivo complementar de repositorio existe.</param>
    public IReadOnlyCollection<string> GetMissingArtifactKinds(
        bool hasPrimaryClass,
        bool hasModel,
        bool hasRepository)
    {
        var missingArtifacts = new List<string>(3);
        if (!hasPrimaryClass)
        {
            missingArtifacts.Add("class");
        }

        if (!hasModel)
        {
            missingArtifacts.Add("model");
        }

        if (!hasRepository)
        {
            missingArtifacts.Add("repository");
        }

        return missingArtifacts;
    }

    /// <summary>
    /// EN: Returns the generated artifact kinds whose embedded snapshot points to a different database object.
    /// PT: Retorna os tipos de artefato gerado cujo snapshot embutido aponta para um objeto de banco diferente.
    /// </summary>
    /// <param name="databaseObject">EN: Database object expected by the current consistency flow. PT: Objeto de banco esperado pelo fluxo atual de consistencia.</param>
    /// <param name="primarySnapshot">EN: Snapshot for the main generated class. PT: Snapshot da classe gerada principal.</param>
    /// <param name="modelSnapshot">EN: Snapshot for the generated model artifact. PT: Snapshot do artefato de modelo gerado.</param>
    /// <param name="repositorySnapshot">EN: Snapshot for the generated repository artifact. PT: Snapshot do artefato de repositorio gerado.</param>
    public IReadOnlyCollection<string> GetDriftedArtifactKinds(
        DatabaseObjectReference databaseObject,
        LocalObjectSnapshot? primarySnapshot,
        LocalObjectSnapshot? modelSnapshot,
        LocalObjectSnapshot? repositorySnapshot)
    {
        var driftedArtifacts = new List<string>(3);
        if (primarySnapshot is not null && !IsSnapshotAligned(databaseObject, primarySnapshot))
        {
            driftedArtifacts.Add("class");
        }

        if (modelSnapshot is not null && !IsSnapshotAligned(databaseObject, modelSnapshot))
        {
            driftedArtifacts.Add("model");
        }
        else if (primarySnapshot is not null && modelSnapshot is not null
            && !HaveSameProperties(primarySnapshot.Properties ?? new Dictionary<string, string>(), modelSnapshot.Properties ?? new Dictionary<string, string>()))
        {
            driftedArtifacts.Add("model");
        }

        if (repositorySnapshot is not null && !IsSnapshotAligned(databaseObject, repositorySnapshot))
        {
            driftedArtifacts.Add("repository");
        }
        else if (primarySnapshot is not null && repositorySnapshot is not null
            && !HaveSameProperties(primarySnapshot.Properties ?? new Dictionary<string, string>(), repositorySnapshot.Properties ?? new Dictionary<string, string>()))
        {
            driftedArtifacts.Add("repository");
        }

        return driftedArtifacts;
    }

    /// <summary>
    /// EN: Classifies the local artifact set required by the generation flow before metadata comparison.
    /// PT: Classifica o conjunto de artefatos locais exigido pelo fluxo de geracao antes da comparacao de metadados.
    /// </summary>
    /// <param name="databaseObject">EN: Database object reference being checked. PT: Referencia do objeto de banco sendo verificada.</param>
    /// <param name="localFilePath">EN: Primary generated class file path. PT: Caminho do arquivo principal da classe gerada.</param>
    /// <param name="hasPrimaryClass">EN: Indicates whether the main generated class exists. PT: Indica se a classe gerada principal existe.</param>
    /// <param name="hasModel">EN: Indicates whether the companion model file exists. PT: Indica se o arquivo complementar de modelo existe.</param>
    /// <param name="hasRepository">EN: Indicates whether the companion repository file exists. PT: Indica se o arquivo complementar de repositorio existe.</param>
    /// <param name="missingMessage">EN: Diagnostic message used when the artifact set is incomplete or missing. PT: Mensagem diagnostica usada quando o conjunto de artefatos esta incompleto ou ausente.</param>
    public ObjectHealthResult? EvaluateLocalArtifacts(
        DatabaseObjectReference databaseObject,
        string localFilePath,
        bool hasPrimaryClass,
        bool hasModel,
        bool hasRepository,
        string? missingMessage = null)
    {
        var missingArtifacts = GetMissingArtifactKinds(hasPrimaryClass, hasModel, hasRepository);
        var existingArtifacts = 3 - missingArtifacts.Count;

        return existingArtifacts switch
        {
            3 => null,
            0 => new ObjectHealthResult(databaseObject, localFilePath, ObjectHealthStatus.MissingLocalArtifacts, missingMessage),
            _ => new ObjectHealthResult(databaseObject, localFilePath, ObjectHealthStatus.IncompleteLocalArtifacts, missingMessage),
        };
    }

    /// <summary>
    /// EN: Classifies generated artifacts as drifted when their embedded snapshot no longer matches the selected database object.
    /// PT: Classifica artefatos gerados como divergentes quando o snapshot embutido nao corresponde mais ao objeto de banco selecionado.
    /// </summary>
    /// <param name="databaseObject">EN: Database object reference being checked. PT: Referencia do objeto de banco sendo verificada.</param>
    /// <param name="localFilePath">EN: Primary generated class file path. PT: Caminho do arquivo principal da classe gerada.</param>
    /// <param name="driftedArtifactKinds">EN: Artifact kinds whose snapshot metadata diverged from the selected object. PT: Tipos de artefato cujo metadata de snapshot divergiu do objeto selecionado.</param>
    /// <param name="driftMessage">EN: Diagnostic message used when drift is detected. PT: Mensagem diagnostica usada quando um drift e detectado.</param>
    public ObjectHealthResult? EvaluateArtifactDrift(
        DatabaseObjectReference databaseObject,
        string localFilePath,
        IReadOnlyCollection<string> driftedArtifactKinds,
        string? driftMessage = null)
    {
        return driftedArtifactKinds.Count == 0
            ? null
            : new ObjectHealthResult(databaseObject, localFilePath, ObjectHealthStatus.DifferentFromDatabase, driftMessage);
    }

    /// <summary>
    /// Executes this API operation.
    /// Executa esta operação da API.
    /// </summary>
    public async Task<ObjectHealthResult> CheckAsync(
        ConnectionDefinition connection,
        LocalObjectSnapshot snapshot,
        IDatabaseMetadataProvider provider,
        CancellationToken cancellationToken = default)
    {
        var databaseObject = await provider.GetObjectAsync(connection, snapshot.Reference, cancellationToken);

        if (databaseObject is null)
        {
            return new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.MissingInDatabase,
                "Objeto não existe mais na base.");
        }

        var localProperties = snapshot.Properties ?? new Dictionary<string, string>();
        var dbProperties = databaseObject.Properties ?? new Dictionary<string, string>();
        var isSame = HaveSameProperties(localProperties, dbProperties);

        return isSame
            ? new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.Synchronized)
            : new ObjectHealthResult(snapshot.Reference, snapshot.FilePath, ObjectHealthStatus.DifferentFromDatabase,
                "Arquivo local diferente das propriedades atuais no banco.");
    }

    private static bool HaveSameProperties(IReadOnlyDictionary<string, string> left, IReadOnlyDictionary<string, string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        foreach (var pair in left)
        {
            if (!right.TryGetValue(pair.Key, out var value))
            {
                return false;
            }

            if (!string.Equals(pair.Value, value, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsSnapshotAligned(DatabaseObjectReference databaseObject, LocalObjectSnapshot snapshot)
        => string.Equals(snapshot.Reference.Schema, databaseObject.Schema, StringComparison.OrdinalIgnoreCase)
           && string.Equals(snapshot.Reference.Name, databaseObject.Name, StringComparison.OrdinalIgnoreCase)
           && snapshot.Reference.Type == databaseObject.Type;
}
