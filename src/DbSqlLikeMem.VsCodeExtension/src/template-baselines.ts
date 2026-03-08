export interface TemplateSettingsLike {
  modelTemplatePath: string;
  repositoryTemplatePath: string;
  modelTargetFolder: string;
  repositoryTargetFolder: string;
  modelFileNamePattern: string;
  repositoryFileNamePattern: string;
}

export type MappingBaselineObjectType = 'Table' | 'View' | 'Procedure' | 'Sequence';

export interface MappingBaselineEntry {
  targetFolder: string;
  fileSuffix: string;
}

export type TemplateBaselineProfileId = 'api' | 'worker';

export interface TemplateReviewMetadata {
  currentBaseline: string;
  promotionStagingPath: string;
  reviewCadence: string;
  lastReviewedOn: string;
  nextPlannedReviewOn: string;
  profileFocusById: Partial<Record<TemplateBaselineProfileId, string>>;
  evidenceFiles: string[];
}

export interface TemplateBaselineProfile extends TemplateSettingsLike {
  id: TemplateBaselineProfileId;
  version: 'vCurrent';
  label: string;
  description: string;
  recommendedTestFocus: string;
  reviewCadence: 'quarterly';
  nextPlannedReviewOn: string;
  mappingDefaults: Record<MappingBaselineObjectType, MappingBaselineEntry>;
}

const TEMPLATE_BASELINE_PROFILES: readonly TemplateBaselineProfile[] = [
  {
    id: 'api',
    version: 'vCurrent',
    label: 'API',
    description: 'Read-oriented baseline for models and repositories.',
    recommendedTestFocus: 'Light integration tests for tables, views, and repositories.',
    reviewCadence: 'quarterly',
    nextPlannedReviewOn: '2026-06-30',
    modelTemplatePath: 'templates/dbsqllikemem/vCurrent/api/model.template.txt',
    repositoryTemplatePath: 'templates/dbsqllikemem/vCurrent/api/repository.template.txt',
    modelTargetFolder: 'src/Models',
    repositoryTargetFolder: 'src/Repositories',
    modelFileNamePattern: '{NamePascal}Model.cs',
    repositoryFileNamePattern: '{NamePascal}Repository.cs',
    mappingDefaults: {
      Table: { targetFolder: 'tests/Integration/Tables', fileSuffix: 'TableIntegrationTests' },
      View: { targetFolder: 'tests/Integration/Views', fileSuffix: 'ViewIntegrationTests' },
      Procedure: { targetFolder: 'tests/Integration/Procedures', fileSuffix: 'ProcedureIntegrationTests' },
      Sequence: { targetFolder: 'tests/Integration/Sequences', fileSuffix: 'SequenceIntegrationTests' }
    }
  },
  {
    id: 'worker',
    version: 'vCurrent',
    label: 'Worker/Batch',
    description: 'Execution-oriented baseline for batch and worker solutions.',
    recommendedTestFocus: 'Consistency-oriented tests for batch flows and DML validation.',
    reviewCadence: 'quarterly',
    nextPlannedReviewOn: '2026-06-30',
    modelTemplatePath: 'templates/dbsqllikemem/vCurrent/worker/model.template.txt',
    repositoryTemplatePath: 'templates/dbsqllikemem/vCurrent/worker/repository.template.txt',
    modelTargetFolder: 'src/Batch/Models',
    repositoryTargetFolder: 'src/Batch/Repositories',
    modelFileNamePattern: '{NamePascal}Model.cs',
    repositoryFileNamePattern: '{NamePascal}Repository.cs',
    mappingDefaults: {
      Table: { targetFolder: 'tests/Consistency/Tables', fileSuffix: 'TableConsistencyTests' },
      View: { targetFolder: 'tests/Consistency/Views', fileSuffix: 'ViewConsistencyTests' },
      Procedure: { targetFolder: 'tests/Consistency/Procedures', fileSuffix: 'ProcedureConsistencyTests' },
      Sequence: { targetFolder: 'tests/Consistency/Sequences', fileSuffix: 'SequenceConsistencyTests' }
    }
  }
];

export function getTemplateBaselineProfiles(): readonly TemplateBaselineProfile[] {
  return TEMPLATE_BASELINE_PROFILES;
}

export function getTemplateBaselineProfile(id: string): TemplateBaselineProfile | undefined {
  return TEMPLATE_BASELINE_PROFILES.find((profile) => profile.id.toLowerCase() === id.trim().toLowerCase());
}

export function getMappingBaselineDefaults(profileId: string): Record<MappingBaselineObjectType, MappingBaselineEntry> {
  const baseline = getTemplateBaselineProfile(profileId) ?? getTemplateBaselineProfile('api');
  return baseline?.mappingDefaults ?? {
    Table: { targetFolder: 'tests/Integration/Tables', fileSuffix: 'TableIntegrationTests' },
    View: { targetFolder: 'tests/Integration/Views', fileSuffix: 'ViewIntegrationTests' },
    Procedure: { targetFolder: 'tests/Integration/Procedures', fileSuffix: 'ProcedureIntegrationTests' },
    Sequence: { targetFolder: 'tests/Integration/Sequences', fileSuffix: 'SequenceIntegrationTests' }
  };
}

export function parseTemplateReviewMetadata(content: string): TemplateReviewMetadata | undefined {
  if (!content.trim()) {
    return undefined;
  }

  try {
    const parsed = JSON.parse(content) as {
      currentBaseline?: unknown;
      promotionStagingPath?: unknown;
      reviewCadence?: unknown;
      lastReviewedOn?: unknown;
      nextPlannedReviewOn?: unknown;
      profiles?: Record<string, { focus?: unknown }>;
      evidenceFiles?: unknown;
    };

    const profileFocusById: Partial<Record<TemplateBaselineProfileId, string>> = {};
    for (const profileId of ['api', 'worker'] as const) {
      const focus = parsed.profiles?.[profileId]?.focus;
      if (typeof focus === 'string' && focus.trim()) {
        profileFocusById[profileId] = focus;
      }
    }

    return {
      currentBaseline: typeof parsed.currentBaseline === 'string' ? parsed.currentBaseline : '',
      promotionStagingPath: typeof parsed.promotionStagingPath === 'string' ? parsed.promotionStagingPath : '',
      reviewCadence: typeof parsed.reviewCadence === 'string' ? parsed.reviewCadence : '',
      lastReviewedOn: typeof parsed.lastReviewedOn === 'string' ? parsed.lastReviewedOn : '',
      nextPlannedReviewOn: typeof parsed.nextPlannedReviewOn === 'string' ? parsed.nextPlannedReviewOn : '',
      profileFocusById,
      evidenceFiles: Array.isArray(parsed.evidenceFiles)
        ? parsed.evidenceFiles.filter((entry): entry is string => typeof entry === 'string' && entry.trim().length > 0)
        : []
    };
  } catch {
    return undefined;
  }
}

export function validateTemplateBaselineProfileAlignment(
  profile: TemplateBaselineProfile,
  reviewMetadata?: TemplateReviewMetadata,
  todayIsoDate?: string): readonly string[] {
  if (!reviewMetadata) {
    return [];
  }

  const warnings: string[] = [];
  if (reviewMetadata.currentBaseline
    && reviewMetadata.currentBaseline.localeCompare(profile.version, undefined, { sensitivity: 'accent' }) !== 0) {
    warnings.push(`Current baseline '${reviewMetadata.currentBaseline}' differs from catalog version '${profile.version}'.`);
  }

  if (reviewMetadata.reviewCadence
    && reviewMetadata.reviewCadence.localeCompare(profile.reviewCadence, undefined, { sensitivity: 'accent' }) !== 0) {
    warnings.push(`Review cadence '${reviewMetadata.reviewCadence}' differs from catalog cadence '${profile.reviewCadence}'.`);
  }

  if (reviewMetadata.nextPlannedReviewOn
    && reviewMetadata.nextPlannedReviewOn !== profile.nextPlannedReviewOn) {
    warnings.push(`Next planned review '${reviewMetadata.nextPlannedReviewOn}' differs from catalog date '${profile.nextPlannedReviewOn}'.`);
  }

  const focusFromMetadata = reviewMetadata.profileFocusById[profile.id];
  if (focusFromMetadata && focusFromMetadata !== profile.recommendedTestFocus) {
    warnings.push('Recommended focus differs between review metadata and catalog.');
  }

  const effectiveToday = normalizeIsoDate(todayIsoDate) ?? normalizeIsoDate(new Date().toISOString().slice(0, 10));
  const nextReviewDate = normalizeIsoDate(reviewMetadata.nextPlannedReviewOn);
  if (effectiveToday && nextReviewDate && nextReviewDate < effectiveToday) {
    warnings.push(`Template baseline review is overdue since '${reviewMetadata.nextPlannedReviewOn}'.`);
  }

  return warnings;
}

export function buildTemplateBaselineProfileSummary(
  profile: TemplateBaselineProfile,
  reviewMetadata?: TemplateReviewMetadata,
  todayIsoDate?: string): string {
  const effectiveFocus = reviewMetadata?.profileFocusById[profile.id] || profile.recommendedTestFocus;
  const effectiveCadence = reviewMetadata?.reviewCadence || profile.reviewCadence;
  const effectiveNextReviewOn = reviewMetadata?.nextPlannedReviewOn || profile.nextPlannedReviewOn;
  const lastReviewedOn = reviewMetadata?.lastReviewedOn || 'n/a';
  const evidenceSuffix = reviewMetadata && reviewMetadata.evidenceFiles.length > 0
    ? ` Evidence files: ${reviewMetadata.evidenceFiles.length}.`
    : '';
  const governanceWarnings = validateTemplateBaselineProfileAlignment(profile, reviewMetadata, todayIsoDate);
  const governanceSuffix = governanceWarnings.length > 0
    ? ` Governance drift: ${governanceWarnings.join(' | ')}`
    : '';
  const outputSuffix = ` Outputs: ${profile.modelTargetFolder} | ${profile.repositoryTargetFolder}.`;

  return `${profile.label} (${profile.version}) - ${profile.description} Focus: ${effectiveFocus} Review: ${effectiveCadence} (last ${lastReviewedOn}, next ${effectiveNextReviewOn}).${outputSuffix}${evidenceSuffix}${governanceSuffix}`;
}

export function resolveTemplateSettingsDefaults(current: TemplateSettingsLike): TemplateSettingsLike {
  const baseline = getTemplateBaselineProfile('api');
  if (!baseline) {
    return current;
  }

  return {
    modelTemplatePath: current.modelTemplatePath || baseline.modelTemplatePath,
    repositoryTemplatePath: current.repositoryTemplatePath || baseline.repositoryTemplatePath,
    modelTargetFolder: current.modelTargetFolder || baseline.modelTargetFolder,
    repositoryTargetFolder: current.repositoryTargetFolder || baseline.repositoryTargetFolder,
    modelFileNamePattern: current.modelFileNamePattern || baseline.modelFileNamePattern,
    repositoryFileNamePattern: current.repositoryFileNamePattern || baseline.repositoryFileNamePattern
  };
}

function normalizeIsoDate(value: string | undefined): string | undefined {
  if (!value || !/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return undefined;
  }

  return value;
}
