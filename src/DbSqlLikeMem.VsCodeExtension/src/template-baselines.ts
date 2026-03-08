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
