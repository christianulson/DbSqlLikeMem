namespace DbSqlLikeMem.VisualStudioExtension.Core.Generation;

/// <summary>
/// EN: Represents the versioned review metadata associated with the shared template baseline.
/// PT: Representa os metadados versionados de revisao associados a baseline compartilhada de templates.
/// </summary>
public sealed record TemplateReviewMetadata
{
    /// <summary>
    /// EN: Creates a versioned template review metadata snapshot.
    /// PT: Cria um snapshot versionado dos metadados de revisao de templates.
    /// </summary>
    /// <param name="currentBaseline">EN: Current promoted baseline identifier. PT: Identificador da baseline promovida atual.</param>
    /// <param name="promotionStagingPath">EN: Repository-relative staging path used for the next promotion. PT: Caminho relativo no repositorio usado como staging da proxima promocao.</param>
    /// <param name="reviewCadence">EN: Expected review cadence label. PT: Rotulo da cadencia esperada de revisao.</param>
    /// <param name="lastReviewedOn">EN: Last completed review date in ISO format. PT: Data da ultima revisao concluida em formato ISO.</param>
    /// <param name="nextPlannedReviewOn">EN: Next planned review date in ISO format. PT: Proxima data planejada de revisao em formato ISO.</param>
    /// <param name="profileFocusById">EN: Recommended focus indexed by baseline profile id. PT: Foco recomendado indexado pelo id do perfil de baseline.</param>
    /// <param name="evidenceFiles">EN: Repository-relative evidence files that support the review. PT: Arquivos de evidencia relativos ao repositorio que sustentam a revisao.</param>
    public TemplateReviewMetadata(
        string currentBaseline,
        string promotionStagingPath,
        string reviewCadence,
        string lastReviewedOn,
        string nextPlannedReviewOn,
        IReadOnlyDictionary<string, string> profileFocusById,
        IReadOnlyCollection<string> evidenceFiles)
    {
        CurrentBaseline = currentBaseline;
        PromotionStagingPath = promotionStagingPath;
        ReviewCadence = reviewCadence;
        LastReviewedOn = lastReviewedOn;
        NextPlannedReviewOn = nextPlannedReviewOn;
        ProfileFocusById = profileFocusById;
        EvidenceFiles = evidenceFiles;
    }

    /// <summary>
    /// EN: Gets the current promoted baseline identifier.
    /// PT: Obtem o identificador da baseline promovida atual.
    /// </summary>
    public string CurrentBaseline { get; }

    /// <summary>
    /// EN: Gets the repository-relative staging path for the next promotion.
    /// PT: Obtem o caminho relativo no repositorio para staging da proxima promocao.
    /// </summary>
    public string PromotionStagingPath { get; }

    /// <summary>
    /// EN: Gets the expected review cadence label.
    /// PT: Obtem o rotulo da cadencia esperada de revisao.
    /// </summary>
    public string ReviewCadence { get; }

    /// <summary>
    /// EN: Gets the last completed review date in ISO format.
    /// PT: Obtem a data da ultima revisao concluida em formato ISO.
    /// </summary>
    public string LastReviewedOn { get; }

    /// <summary>
    /// EN: Gets the next planned review date in ISO format.
    /// PT: Obtem a proxima data planejada de revisao em formato ISO.
    /// </summary>
    public string NextPlannedReviewOn { get; }

    /// <summary>
    /// EN: Gets the recommended focus text indexed by profile id.
    /// PT: Obtem o texto de foco recomendado indexado por id do perfil.
    /// </summary>
    public IReadOnlyDictionary<string, string> ProfileFocusById { get; }

    /// <summary>
    /// EN: Gets the repository-relative evidence files associated with the review.
    /// PT: Obtem os arquivos de evidencia relativos ao repositorio associados a revisao.
    /// </summary>
    public IReadOnlyCollection<string> EvidenceFiles { get; }
}
