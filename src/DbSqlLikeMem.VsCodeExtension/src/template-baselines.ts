export interface TemplateSettingsLike {
  modelTemplatePath: string;
  repositoryTemplatePath: string;
  modelTargetFolder: string;
  repositoryTargetFolder: string;
}

export type TemplateBaselineProfileId = 'api' | 'worker';

export interface TemplateBaselineProfile extends TemplateSettingsLike {
  id: TemplateBaselineProfileId;
  version: 'vCurrent';
  label: string;
  description: string;
}

const TEMPLATE_BASELINE_PROFILES: readonly TemplateBaselineProfile[] = [
  {
    id: 'api',
    version: 'vCurrent',
    label: 'API',
    description: 'Read-oriented baseline for models and repositories.',
    modelTemplatePath: 'templates/dbsqllikemem/vCurrent/api/model.template.txt',
    repositoryTemplatePath: 'templates/dbsqllikemem/vCurrent/api/repository.template.txt',
    modelTargetFolder: 'src/Models',
    repositoryTargetFolder: 'src/Repositories'
  },
  {
    id: 'worker',
    version: 'vCurrent',
    label: 'Worker/Batch',
    description: 'Execution-oriented baseline for batch and worker solutions.',
    modelTemplatePath: 'templates/dbsqllikemem/vCurrent/worker/model.template.txt',
    repositoryTemplatePath: 'templates/dbsqllikemem/vCurrent/worker/repository.template.txt',
    modelTargetFolder: 'src/Batch/Models',
    repositoryTargetFolder: 'src/Batch/Repositories'
  }
];

export function getTemplateBaselineProfiles(): readonly TemplateBaselineProfile[] {
  return TEMPLATE_BASELINE_PROFILES;
}

export function getTemplateBaselineProfile(id: string): TemplateBaselineProfile | undefined {
  return TEMPLATE_BASELINE_PROFILES.find((profile) => profile.id.toLowerCase() === id.trim().toLowerCase());
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
    repositoryTargetFolder: current.repositoryTargetFolder || baseline.repositoryTargetFolder
  };
}
